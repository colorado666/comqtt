// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co, wind

// package mqtt provides a high performance, fully compliant MQTT v5 broker server with v3.1.1 backward compatibility.
package mqtt

import (
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/hooks/storage"
	"github.com/wind-c/comqtt/v2/mqtt/listeners"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
	"github.com/wind-c/comqtt/v2/mqtt/system"

	"log/slog"
)

const (
	Version                       = "2.4.0" // the current server version.
	defaultSysTopicInterval int64 = 1       // the interval between $SYS topic publishes
	LocalListener                 = "local"
	InlineClientId                = "inline"
)

var (
	// DefaultServerCapabilities defines the default features and capabilities provided by the server.
	DefaultServerCapabilities = &Capabilities{
		MaximumSessionExpiryInterval: math.MaxUint32, // maximum number of seconds to keep disconnected sessions
		MaximumMessageExpiryInterval: 60 * 60 * 24,   // maximum message expiry if message expiry is 0 or over
		ReceiveMaximum:               1024,           // maximum number of concurrent qos messages per client
		MaximumQos:                   2,              // maximum qos value available to clients
		RetainAvailable:              1,              // retain messages is available
		MaximumPacketSize:            0,              // no maximum packet size
		TopicAliasMaximum:            math.MaxUint16, // maximum topic alias value
		WildcardSubAvailable:         1,              // wildcard subscriptions are available
		SubIDAvailable:               1,              // subscription identifiers are available
		SharedSubAvailable:           1,              // shared subscriptions are available
		MinimumProtocolVersion:       3,              // minimum supported mqtt version (3.0.0)
		MaximumClientWritesPending:   1024 * 8,       // maximum number of pending message writes for a client
	}

	ErrListenerIDExists       = errors.New("listener id already exists")                               // a listener with the same id already exists
	ErrConnectionClosed       = errors.New("connection not open")                                      // connection is closed
	ErrInlineClientNotEnabled = errors.New("please set Options.InlineClient=true to use this feature") // inline client is not enabled by default
)

// Capabilities indicates the capabilities and features provided by the server.
type Capabilities struct {
	MaximumMessageExpiryInterval int64  `yaml:"maximum-message-expiry-interval"`
	MaximumClientWritesPending   int32  `yaml:"maximum-client-writes-pending"`
	MaximumSessionExpiryInterval uint32 `yaml:"maximum-session-expiry-interval"`
	MaximumPacketSize            uint32 `yaml:"maximum-packet-size"`
	maximumPacketID              uint32 // unexported, used for testing only
	ReceiveMaximum               uint16 `yaml:"receive-maximum"`
	TopicAliasMaximum            uint16 `yaml:"topic-alias-maximum"`
	SharedSubAvailable           byte   `yaml:"shared-sub-available"`
	MinimumProtocolVersion       byte   `yaml:"minimum-protocol-version"`
	Compatibilities              Compatibilities
	MaximumQos                   byte `yaml:"maximum-qos"`
	RetainAvailable              byte `yaml:"retain-available"`
	WildcardSubAvailable         byte `yaml:"wildcard-sub-available"`
	SubIDAvailable               byte `yaml:"sub-id-available"`
}

// Compatibilities provides flags for using compatibility modes.
type Compatibilities struct {
	ObscureNotAuthorized       bool `yaml:"obscure-not-authorized"`    // return unspecified errors instead of not authorized
	PassiveClientDisconnect    bool `yaml:"passive-client-disconnect"` // don't disconnect the client forcefully after sending disconnect packet (paho)
	AlwaysReturnResponseInfo   bool `yaml:"always-return-response"`    // always return response info (useful for testing)
	RestoreSysInfoOnRestart    bool `yaml:"restore-sys-info-restart"`  // restore system info from store as if server never stopped
	NoInheritedPropertiesOnAck bool // don't allow inherited user properties on ack (paho - spec violation)
}

// Options contains configurable options for the server.
type Options struct {
	// Capabilities defines the server features and behaviour. If you only wish to modify
	// several of these values, set them explicitly - e.g.
	// 	server.Options.Capabilities.MaximumClientWritesPending = 16 * 1024
	Capabilities *Capabilities

	// ClientNetWriteBufferSize specifies the size of the client *bufio.Writer write buffer.
	ClientNetWriteBufferSize int `yaml:"client-write-buffer-size"`

	// ClientNetReadBufferSize specifies the size of the client *bufio.Reader read buffer.
	ClientNetReadBufferSize int `yaml:"client-read-buffer-size"`

	// Logger specifies a custom configured implementation of zerolog to override
	// the servers default logger configuration. If you wish to change the log level,
	// of the default logger, you can do so by setting
	// 	server := mqtt.New(nil)
	// level := new(slog.LevelVar)
	// server.Slog = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	// 	Level: level,
	// }))
	// level.Set(slog.LevelDebug)
	Logger *slog.Logger

	// SysTopicResendInterval specifies the interval between $SYS topic updates in seconds.
	SysTopicResendInterval int64 `yaml:"sys-topic-resend-interval"`

	// Enable Inline client to allow direct subscribing and publishing from the parent codebase,
	// with negligible performance difference (disabled by default to prevent confusion in statistics).
	InlineClient bool `yaml:"inline-client"`
}

// Server is an MQTT broker server. It should be created with server.New()
// in order to ensure all the internal fields are correctly populated.
type Server struct {
	Options      *Options             // configurable server options
	Listeners    *listeners.Listeners // listeners are network interfaces which listen for new connections
	Clients      *Clients             // clients known to the broker
	Topics       *TopicsIndex         // an index of topic filter subscriptions and retained messages
	Info         *system.Info         // values about the server commonly known as $SYS topics
	loop         *loop                // loop contains tickers for the system event loop
	done         chan bool            // indicate that the server is ending
	Log          *slog.Logger         // minimal no-alloc logger
	hooks        *Hooks               // hooks contains hooks for extra functionality such as auth and persistent storage
	inlineClient *Client              // inlineClient is a special client used for inline subscriptions and inline Publish
	Blacklist    []string             // blacklist of client id
}

// loop contains interval tickers for the system events loop.
type loop struct {
	sysTopics      *time.Ticker     // interval ticker for sending updating $SYS topics
	clientExpiry   *time.Ticker     // interval ticker for cleaning expired clients
	inflightExpiry *time.Ticker     // interval ticker for cleaning up expired inflight messages
	retainedExpiry *time.Ticker     // interval ticker for cleaning retained messages
	willDelaySend  *time.Ticker     // interval ticker for sending Will Messages with a delay
	willDelayed    *packets.Packets // activate LWT packets which will be sent after a delay
}

// ops contains server values which can be propagated to other structs.
type ops struct {
	options *Options     // a pointer to the server options and capabilities, for referencing in clients
	info    *system.Info // pointers to server system info
	hooks   *Hooks       // pointer to the server hooks
	log     *slog.Logger // a structured logger for the client
}

// New returns a new instance of comqtt broker. Optional parameters
// can be specified to override some default settings (see Options).
func New(opts *Options) *Server {
	if opts == nil {
		opts = new(Options)
	}

	opts.ensureDefaults()

	s := &Server{
		done:      make(chan bool),
		Clients:   NewClients(),
		Topics:    NewTopicsIndex(),
		Listeners: listeners.New(),
		loop: &loop{
			sysTopics:      time.NewTicker(time.Second * time.Duration(opts.SysTopicResendInterval)),
			clientExpiry:   time.NewTicker(time.Second * 10),
			inflightExpiry: time.NewTicker(time.Second * 10),
			retainedExpiry: time.NewTicker(time.Second * 30),
			willDelaySend:  time.NewTicker(time.Second * 5),
			willDelayed:    packets.NewPackets(),
		},
		Options: opts,
		Info: &system.Info{
			Version: Version,
			Started: time.Now().Unix(),
		},
		Log: opts.Logger,
		hooks: &Hooks{
			Log: opts.Logger,
		},
	}

	if s.Options.InlineClient {
		s.inlineClient = s.NewClient(nil, LocalListener, InlineClientId, true)
		s.Clients.Add(s.inlineClient)
	}

	return s
}

// ensureDefaults ensures that the server starts with sane default values, if none are provided.
func (o *Options) ensureDefaults() {
	if o.Capabilities == nil {
		o.Capabilities = DefaultServerCapabilities
	}

	o.Capabilities.maximumPacketID = math.MaxUint16 // spec maximum is 65535

	if o.SysTopicResendInterval == 0 {
		o.SysTopicResendInterval = defaultSysTopicInterval
	}

	if o.ClientNetWriteBufferSize == 0 {
		o.ClientNetWriteBufferSize = 1024 * 2
	}

	if o.ClientNetReadBufferSize == 0 {
		o.ClientNetReadBufferSize = 1024 * 2
	}

	if o.Logger == nil {
		log := slog.New(slog.NewTextHandler(os.Stdout, nil))
		o.Logger = log
	}
}

// NewClient returns a new Client instance, populated with all the required values and
// references to be used with the server. If you are using this client to directly publish
// messages from the embedding application, set the inline flag to true to bypass ACL and
// topic validation checks.
func (s *Server) NewClient(c net.Conn, listener string, id string, inline bool) *Client {
	cl := newClient(c, &ops{ // [MQTT-3.1.2-6] implicit
		options: s.Options,
		info:    s.Info,
		hooks:   s.hooks,
		log:     s.Log,
	})

	cl.ID = id
	cl.Net.Listener = listener

	if inline { // inline clients bypass acl and some validity checks.
		cl.Net.Inline = true
		// By default, we don't want to restrict developer publishes,
		// but if you do, reset this after creating inline client.
		cl.State.Inflight.ResetReceiveQuota(math.MaxInt32)
	} else {
		go cl.WriteLoop() // can only write to real clients
	}

	return cl
}

// AddHook attaches a new Hook to the server. Ideally, this should be called
// before the server is started with s.Serve().
func (s *Server) AddHook(hook Hook, config any) error {
	nl := s.Log.With("hook", hook.ID())
	hook.SetOpts(nl, &HookOptions{
		Capabilities: s.Options.Capabilities,
	})

	s.Log.Info("added hook", "hook", hook.ID())
	return s.hooks.Add(hook, config)
}

// AddListener adds a new network listener to the server, for receiving incoming client connections.
func (s *Server) AddListener(l listeners.Listener) error {
	if _, ok := s.Listeners.Get(l.ID()); ok {
		return ErrListenerIDExists
	}

	nl := s.Log.With(slog.String("listener", l.ID()))
	err := l.Init(nl)
	if err != nil {
		return err
	}

	s.Listeners.Add(l)
	s.Log.Info("attached listener", "id", l.ID(), "protocol", l.Protocol(), "address", l.Address())
	return nil
}

// Serve starts the event loops responsible for establishing client connections
// on all attached listeners, publishing the system topics, and starting all hooks.
func (s *Server) Serve() error {
	//s.Log.Info("version", Version).Msg("comqtt starting")
	defer s.Log.Info("comqtt server started")

	if s.hooks.Provides(
		StoredClients,
		StoredInflightMessages,
		StoredRetainedMessages,
		StoredSubscriptions,
		StoredSysInfo,
	) {
		err := s.readStore()
		if err != nil {
			return err
		}
	}

	go s.eventLoop()                            // spin up event loop for issuing $SYS values and closing server.
	s.Listeners.ServeAll(s.EstablishConnection) // start listening on all listeners.
	s.publishSysTopics()                        // begin publishing $SYS system values.
	s.hooks.OnStarted()

	return nil
}

// eventLoop loops forever, running various server housekeeping methods at different intervals.
func (s *Server) eventLoop() {
	s.Log.Debug("system event loop started")
	defer s.Log.Debug("system event loop halted")

	for {
		select {
		case <-s.done:
			s.loop.sysTopics.Stop()
			return
		case <-s.loop.sysTopics.C:
			s.publishSysTopics()
		case <-s.loop.clientExpiry.C:
			s.clearExpiredClients(time.Now().Unix())
		case <-s.loop.retainedExpiry.C:
			s.clearExpiredRetainedMessages(time.Now().Unix())
		case <-s.loop.willDelaySend.C:
			s.sendDelayedLWT(time.Now().Unix())
		case <-s.loop.inflightExpiry.C:
			s.clearExpiredInflights(time.Now().Unix())
		}
	}
}

// EstablishConnection establishes a new client when a listener accepts a new connection.
func (s *Server) EstablishConnection(listener string, c net.Conn) error {
	cl := s.NewClient(c, listener, "", false)
	return s.attachClient(cl, listener)
}

// attachClient validates an incoming client connection and if viable, attaches the client
// to the server, performs session housekeeping, and reads incoming packets.
func (s *Server) attachClient(cl *Client, listener string) error {
	defer cl.Stop(nil)
	pk, err := s.readConnectionPacket(cl)
	if err != nil {
		return fmt.Errorf("read connection: %w", err)
	}

	cl.ParseConnect(listener, pk)
	if slices.Contains(s.Blacklist, cl.ID) {
		return fmt.Errorf("blacklisted client: %s", cl.ID)
	}

	code := s.validateConnect(cl, pk) // [MQTT-3.1.4-1] [MQTT-3.1.4-2]
	if code != packets.CodeSuccess {
		if err := s.SendConnack(cl, code, false, nil); err != nil {
			return fmt.Errorf("invalid connection send ack: %w", err)
		}
		return code // [MQTT-3.2.2-7] [MQTT-3.1.4-6]
	}

	err = s.hooks.OnConnect(cl, pk)
	if err != nil {
		return err
	}

	cl.refreshDeadline(cl.State.Keepalive)
	if !s.hooks.OnConnectAuthenticate(cl, pk) { // [MQTT-3.1.4-2]
		err := s.SendConnack(cl, packets.ErrBadUsernameOrPassword, false, nil)
		if err != nil {
			return fmt.Errorf("invalid connection send ack: %w", err)
		}

		return packets.ErrBadUsernameOrPassword
	}

	atomic.AddInt64(&s.Info.ClientsConnected, 1)
	defer atomic.AddInt64(&s.Info.ClientsConnected, -1)

	s.hooks.OnSessionEstablish(cl, pk)

	sessionPresent := s.inheritClientSession(pk, cl)
	s.Clients.Add(cl) // [MQTT-4.1.0-1]

	err = s.SendConnack(cl, code, sessionPresent, nil) // [MQTT-3.1.4-5] [MQTT-3.2.0-1] [MQTT-3.2.0-2] &[MQTT-3.14.0-1]
	if err != nil {
		return fmt.Errorf("ack connection packet: %w", err)
	}

	s.loop.willDelayed.Delete(cl.ID) // [MQTT-3.1.3-9]

	if sessionPresent {
		err = cl.ResendInflightMessages(true)
		if err != nil {
			return fmt.Errorf("resend inflight: %w", err)
		}
	}

	s.hooks.OnSessionEstablished(cl, pk)

	err = cl.Read(s.receivePacket)
	if err != nil {
		s.sendLWT(cl)
		cl.Stop(err)
	} else {
		cl.Properties.Will = Will{} // [MQTT-3.14.4-3] [MQTT-3.1.2-10]
	}
	s.Log.Debug("client disconnected", "error", err, "client", cl.ID, "remote", cl.Net.Remote, "listener", listener)

	expire := (cl.Properties.ProtocolVersion == 5 && cl.Properties.Props.SessionExpiryInterval == 0) || (cl.Properties.ProtocolVersion < 5 && cl.Properties.Clean)
	s.hooks.OnDisconnect(cl, err, expire)

	if expire && atomic.LoadUint32(&cl.State.isTakenOver) == 0 {
		cl.ClearInflights(math.MaxInt64, 0)
		s.UnsubscribeClient(cl)
		s.Clients.Delete(cl.ID) // [MQTT-4.1.0-2] ![MQTT-3.1.2-23]
	}

	return err
}

// readConnectionPacket reads the first incoming header for a connection, and if
// acceptable, returns the valid connection packet.
func (s *Server) readConnectionPacket(cl *Client) (pk packets.Packet, err error) {
	fh := new(packets.FixedHeader)
	err = cl.ReadFixedHeader(fh)
	if err != nil {
		return
	}

	if fh.Type != packets.Connect {
		return pk, packets.ErrProtocolViolationRequireFirstConnect // [MQTT-3.1.0-1]
	}

	pk, err = cl.ReadPacket(fh)
	if err != nil {
		return
	}

	return
}

// receivePacket processes an incoming packet for a client, and issues a disconnect to the client
// if an error has occurred (if mqtt v5).
func (s *Server) receivePacket(cl *Client, pk packets.Packet) error {
	err := s.processPacket(cl, pk)
	if err != nil {
		if code, ok := err.(packets.Code); ok &&
			cl.Properties.ProtocolVersion == 5 &&
			code.Code >= packets.ErrUnspecifiedError.Code {
			_ = s.DisconnectClient(cl, code)
		}

		s.Log.Warn("error processing packet", "error", err, "client", cl.ID, "listener", cl.Net.Listener, "pk", pk)

		return err
	}

	return nil
}

// validateConnect validates that a connect packet is compliant.
func (s *Server) validateConnect(cl *Client, pk packets.Packet) packets.Code {
	code := pk.ConnectValidate() // [MQTT-3.1.4-1] [MQTT-3.1.4-2]
	if code != packets.CodeSuccess {
		return code
	}

	if cl.Properties.ProtocolVersion < 5 && !pk.Connect.Clean && pk.Connect.ClientIdentifier == "" {
		return packets.ErrUnspecifiedError
	}

	if cl.Properties.ProtocolVersion < s.Options.Capabilities.MinimumProtocolVersion {
		return packets.ErrUnsupportedProtocolVersion // [MQTT-3.1.2-2]
	} else if cl.Properties.Will.Qos > s.Options.Capabilities.MaximumQos {
		return packets.ErrQosNotSupported // [MQTT-3.2.2-12]
	} else if cl.Properties.Will.Retain && s.Options.Capabilities.RetainAvailable == 0x00 {
		return packets.ErrRetainNotSupported // [MQTT-3.2.2-13]
	}

	return code
}

// inheritClientSession inherits the state of an existing client sharing the same
// connection ID. If clean is true, the state of any previously existing client
// session is abandoned.
func (s *Server) inheritClientSession(pk packets.Packet, cl *Client) bool {
	if existing, ok := s.Clients.Get(pk.Connect.ClientIdentifier); ok {
		_ = s.DisconnectClient(existing, packets.ErrSessionTakenOver)                                   // [MQTT-3.1.4-3]
		if pk.Connect.Clean || (existing.Properties.Clean && existing.Properties.ProtocolVersion < 5) { // [MQTT-3.1.2-4] [MQTT-3.1.4-4]
			s.UnsubscribeClient(existing)
			existing.ClearInflights(math.MaxInt64, 0)
			atomic.StoreUint32(&existing.State.isTakenOver, 1) // only set isTakenOver after unsubscribe has occurred
			return false                                       // [MQTT-3.2.2-3]
		}

		atomic.StoreUint32(&existing.State.isTakenOver, 1)
		if existing.State.Inflight.Len() > 0 {
			cl.State.Inflight = existing.State.Inflight.Clone() // [MQTT-3.1.2-5]
			if cl.State.Inflight.maximumReceiveQuota == 0 && cl.ops.options.Capabilities.ReceiveMaximum != 0 {
				cl.State.Inflight.ResetReceiveQuota(int32(cl.ops.options.Capabilities.ReceiveMaximum)) // server receive max per client
				cl.State.Inflight.ResetSendQuota(int32(cl.Properties.Props.ReceiveMaximum))            // client receive max
			}
		}

		for _, sub := range existing.State.Subscriptions.GetAll() {
			isNew, count := s.Topics.Subscribe(cl.ID, sub) // [MQTT-3.8.4-3]
			if isNew {
				atomic.AddInt64(&s.Info.Subscriptions, 1)
				s.hooks.OnSubscribed(existing, packets.Packet{Filters: []packets.Subscription{sub}}, []byte{sub.Qos}, []int{count})
			}
			cl.State.Subscriptions.Add(sub.Filter, sub)
			s.publishRetainedToClient(cl, sub, !isNew)
		}

		// Clean the state of the existing client to prevent sequential take-overs
		// from increasing memory usage by inflights + subs * client-id.
		s.UnsubscribeClient(existing)
		existing.ClearInflights(math.MaxInt64, 0)

		s.Log.Debug("session taken over", "client", cl.ID, "old_remote", existing.Net.Remote, "new_remote", cl.Net.Remote)

		cl.InheritWay = InheritWayLocal
		return true // [MQTT-3.2.2-3]
	}

	if atomic.LoadInt64(&s.Info.ClientsConnected) > atomic.LoadInt64(&s.Info.ClientsMaximum) {
		atomic.AddInt64(&s.Info.ClientsMaximum, 1)
	}

	if pk.Connect.Clean {
		return false
	}

	if s.loadClientHistory(cl.ID) {
		cl.InheritWay = InheritWayRemote
		return true
	}

	return false
}

// SendConnack returns a Connack packet to a client.
func (s *Server) SendConnack(cl *Client, reason packets.Code, present bool, properties *packets.Properties) error {
	if properties == nil {
		properties = &packets.Properties{
			ReceiveMaximum: s.Options.Capabilities.ReceiveMaximum,
		}
	}

	properties.ReceiveMaximum = s.Options.Capabilities.ReceiveMaximum // 3.2.2.3.3 Receive Maximum
	if cl.State.ServerKeepalive {                                     // You can set this dynamically using the OnConnect hook.
		properties.ServerKeepAlive = cl.State.Keepalive // [MQTT-3.1.2-21]
		properties.ServerKeepAliveFlag = true
	}

	if reason.Code >= packets.ErrUnspecifiedError.Code {
		if cl.Properties.ProtocolVersion < 5 {
			if v3reason, ok := packets.V5CodesToV3[reason]; ok { // NB v3 3.2.2.3 Connack return codes
				reason = v3reason
			}
		}

		properties.ReasonString = reason.Reason
		ack := packets.Packet{
			FixedHeader: packets.FixedHeader{
				Type: packets.Connack,
			},
			SessionPresent: false,       // [MQTT-3.2.2-6]
			ReasonCode:     reason.Code, // [MQTT-3.2.2-8]
			Properties:     *properties,
		}
		return cl.WritePacket(ack)
	}

	if s.Options.Capabilities.MaximumQos < 2 {
		properties.MaximumQos = s.Options.Capabilities.MaximumQos // [MQTT-3.2.2-9]
		properties.MaximumQosFlag = true
	}

	if cl.Properties.Props.AssignedClientID != "" {
		properties.AssignedClientID = cl.Properties.Props.AssignedClientID // [MQTT-3.1.3-7] [MQTT-3.2.2-16]
	}

	if cl.Properties.Props.SessionExpiryInterval > s.Options.Capabilities.MaximumSessionExpiryInterval {
		properties.SessionExpiryInterval = s.Options.Capabilities.MaximumSessionExpiryInterval
		properties.SessionExpiryIntervalFlag = true
		cl.Properties.Props.SessionExpiryInterval = properties.SessionExpiryInterval
		cl.Properties.Props.SessionExpiryIntervalFlag = true
	}

	ack := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Connack,
		},
		SessionPresent: present,
		ReasonCode:     reason.Code, // [MQTT-3.2.2-8]
		Properties:     *properties,
	}
	return cl.WritePacket(ack)
}

// loadClientHistory loads history info of client
func (s *Server) loadClientHistory(cid string) bool {
	ss, err := s.hooks.StoredSubscriptionsByCid(cid)
	if err != nil {
		return false
	}
	s.loadSubscriptions(ss)

	fs, err := s.hooks.StoredInflightMessagesByCid(cid)
	if err != nil {
		return false
	}
	s.loadInflight(fs)

	if len(ss) > 0 || len(fs) > 0 {
		return true
	}

	return false
}

// processPacket processes an inbound packet for a client. Since the method is
// typically called as a goroutine, errors are primarily for test checking purposes.
func (s *Server) processPacket(cl *Client, pk packets.Packet) error {
	var err error

	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = s.processConnect(cl, pk)
	case packets.Disconnect:
		err = s.processDisconnect(cl, pk)
	case packets.Pingreq:
		err = s.processPingreq(cl, pk)
	case packets.Publish:
		code := pk.PublishValidate(s.Options.Capabilities.TopicAliasMaximum)
		if code != packets.CodeSuccess {
			return code
		}
		err = s.processPublish(cl, pk)
	case packets.Puback:
		err = s.processPuback(cl, pk)
	case packets.Pubrec:
		err = s.processPubrec(cl, pk)
	case packets.Pubrel:
		err = s.processPubrel(cl, pk)
	case packets.Pubcomp:
		err = s.processPubcomp(cl, pk)
	case packets.Subscribe:
		code := pk.SubscribeValidate()
		if code != packets.CodeSuccess {
			return code
		}
		err = s.processSubscribe(cl, pk)
	case packets.Unsubscribe:
		code := pk.UnsubscribeValidate()
		if code != packets.CodeSuccess {
			return code
		}
		err = s.processUnsubscribe(cl, pk)
	case packets.Auth:
		code := pk.AuthValidate()
		if code != packets.CodeSuccess {
			return code
		}
		err = s.processAuth(cl, pk)
	default:
		return fmt.Errorf("no valid packet available; %v", pk.FixedHeader.Type)
	}

	s.hooks.OnPacketProcessed(cl, pk, err)
	if err != nil {
		return err
	}

	if cl.State.Inflight.Len() > 0 && atomic.LoadInt32(&cl.State.Inflight.sendQuota) > 0 {
		next, ok := cl.State.Inflight.NextImmediate()
		if ok {
			_ = cl.WritePacket(next)
			if ok := cl.State.Inflight.Delete(next.PacketID); ok {
				atomic.AddInt64(&s.Info.Inflight, -1)
			}
			cl.State.Inflight.DecreaseSendQuota()
		}
	}

	return nil
}

// processConnect processes a Connect packet. The packet cannot be used to establish
// a new connection on an existing connection. See EstablishConnection instead.
func (s *Server) processConnect(cl *Client, _ packets.Packet) error {
	s.sendLWT(cl)
	return packets.ErrProtocolViolationSecondConnect // [MQTT-3.1.0-2]
}

// processPingreq processes a Pingreq packet.
func (s *Server) processPingreq(cl *Client, _ packets.Packet) error {
	return cl.WritePacket(packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Pingresp, // [MQTT-3.12.4-1]
		},
	})
}

// Publish publishes a publish packet into the broker as if it were sent from the specified client.
// This is a convenience function which wraps InjectPacket. As such, this method can publish packets
// to any topic (including $SYS) and bypass ACL checks. The qos byte is used for limiting the
// outbound qos (mqtt v5) rather than issuing to the broker (we assume qos 2 complete).
func (s *Server) Publish(topic string, payload []byte, retain bool, qos byte) error {
	if !s.Options.InlineClient {
		return ErrInlineClientNotEnabled
	}

	return s.InjectPacket(s.inlineClient, packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type:   packets.Publish,
			Qos:    qos,
			Retain: retain,
		},
		TopicName: topic,
		Payload:   payload,
		PacketID:  uint16(qos), // we never process the inbound qos, but we need a packet id for validity checks.
	})
}

// Subscribe adds an inline subscription for the specified topic filter and subscription identifier
// with the provided handler function.
func (s *Server) Subscribe(filter string, subscriptionId int, handler InlineSubFn) error {
	if !s.Options.InlineClient {
		return ErrInlineClientNotEnabled
	}

	if handler == nil {
		return packets.ErrInlineSubscriptionHandlerInvalid
	}

	if !IsValidFilter(filter, false) {
		return packets.ErrTopicFilterInvalid
	}

	subscription := packets.Subscription{
		Identifier: subscriptionId,
		Filter:     filter,
	}

	pk := s.hooks.OnSubscribe(s.inlineClient, packets.Packet{ // subscribe like a normal client.
		Origin:      s.inlineClient.ID,
		FixedHeader: packets.FixedHeader{Type: packets.Subscribe},
		Filters:     packets.Subscriptions{subscription},
	})

	inlineSubscription := InlineSubscription{
		Subscription: subscription,
		Handler:      handler,
	}

	_, count := s.Topics.InlineSubscribe(inlineSubscription)
	s.hooks.OnSubscribed(s.inlineClient, pk, []byte{packets.CodeSuccess.Code}, []int{count})

	// Handling retained messages.
	for _, pkv := range s.Topics.Messages(filter) { // [MQTT-3.8.4-4]
		handler(s.inlineClient, inlineSubscription.Subscription, pkv)
	}
	return nil
}

// Unsubscribe removes an inline subscription for the specified subscription and topic filter.
// It allows you to unsubscribe a specific subscription from the internal subscription
// associated with the given topic filter.
func (s *Server) Unsubscribe(filter string, subscriptionId int) error {
	if !s.Options.InlineClient {
		return ErrInlineClientNotEnabled
	}

	if !IsValidFilter(filter, false) {
		return packets.ErrTopicFilterInvalid
	}

	pk := s.hooks.OnUnsubscribe(s.inlineClient, packets.Packet{
		Origin:      s.inlineClient.ID,
		FixedHeader: packets.FixedHeader{Type: packets.Unsubscribe},
		Filters: packets.Subscriptions{
			{
				Identifier: subscriptionId,
				Filter:     filter,
			},
		},
	})

	_, count := s.Topics.InlineUnsubscribe(subscriptionId, filter)
	s.hooks.OnUnsubscribed(s.inlineClient, pk, []byte{packets.CodeSuccess.Code}, []int{count})
	return nil
}

// InjectPacket injects a packet into the broker as if it were sent from the specified client.
// InlineClients using this method can publish packets to any topic (including $SYS) and bypass ACL checks.
func (s *Server) InjectPacket(cl *Client, pk packets.Packet) error {
	pk.ProtocolVersion = cl.Properties.ProtocolVersion

	err := s.processPacket(cl, pk)
	if err != nil {
		return err
	}

	atomic.AddInt64(&cl.ops.info.PacketsReceived, 1)
	if pk.FixedHeader.Type == packets.Publish {
		atomic.AddInt64(&cl.ops.info.MessagesReceived, 1)
	}

	return nil
}

// processPublish processes a Publish packet.
func (s *Server) processPublish(cl *Client, pk packets.Packet) error {
	if !cl.Net.Inline && !IsValidFilter(pk.TopicName, true) {
		return nil
	}

	if atomic.LoadInt32(&cl.State.Inflight.receiveQuota) == 0 {
		return s.DisconnectClient(cl, packets.ErrReceiveMaximum) // ~[MQTT-3.3.4-7] ~[MQTT-3.3.4-8]
	}

	if !cl.Net.Inline && !s.hooks.OnACLCheck(cl, pk.TopicName, true) {
		if pk.FixedHeader.Qos == 0 {
			return nil
		}

		if cl.Properties.ProtocolVersion != 5 {
			return s.DisconnectClient(cl, packets.ErrNotAuthorized)
		}

		ackType := packets.Puback
		if pk.FixedHeader.Qos == 2 {
			ackType = packets.Pubrec
		}

		ack := s.buildAck(pk.PacketID, ackType, 0, pk.Properties, packets.ErrNotAuthorized)
		return cl.WritePacket(ack)
	}

	pk.Origin = cl.ID
	pk.Created = time.Now().Unix()

	if !cl.Net.Inline {
		if pki, ok := cl.State.Inflight.Get(pk.PacketID); ok {
			if pki.FixedHeader.Type == packets.Pubrec { // [MQTT-4.3.3-10]
				ack := s.buildAck(pk.PacketID, packets.Pubrec, 0, pk.Properties, packets.ErrPacketIdentifierInUse)
				return cl.WritePacket(ack)
			}
			if ok := cl.State.Inflight.Delete(pk.PacketID); ok { // [MQTT-4.3.2-5]
				atomic.AddInt64(&s.Info.Inflight, -1)
			}
		}
	}

	if pk.Properties.TopicAliasFlag && pk.Properties.TopicAlias > 0 { // [MQTT-3.3.2-11]
		pk.TopicName = cl.State.TopicAliases.Inbound.Set(pk.Properties.TopicAlias, pk.TopicName)
	}

	if pk.FixedHeader.Qos > s.Options.Capabilities.MaximumQos {
		pk.FixedHeader.Qos = s.Options.Capabilities.MaximumQos // [MQTT-3.2.2-9] Reduce qos based on server max qos capability
	}

	pkx, err := s.hooks.OnPublish(cl, pk)
	if err == nil {
		pk = pkx
	} else if errors.Is(err, packets.ErrRejectPacket) {
		return nil
	} else if errors.Is(err, packets.CodeSuccessIgnore) {
		pk.Ignore = true
	} else if cl.Properties.ProtocolVersion == 5 && pk.FixedHeader.Qos > 0 && errors.As(err, new(packets.Code)) {
		err = cl.WritePacket(s.buildAck(pk.PacketID, packets.Puback, 0, pk.Properties, err.(packets.Code)))
		if err != nil {
			return err
		}
		return nil
	}

	if pk.FixedHeader.Retain { // [MQTT-3.3.1-5] ![MQTT-3.3.1-8]
		s.retainMessage(cl, pk)
	}

	// If it's inlineClient, it can't handle PUBREC and PUBREL.
	// When it publishes a package with a qos > 0, the server treats
	// the package as qos=0, and the client receives it as qos=1 or 2.
	if pk.FixedHeader.Qos == 0 || cl.Net.Inline {
		s.publishToSubscribers(pk)
		s.hooks.OnPublished(cl, pk)
		return nil
	}

	cl.State.Inflight.DecreaseReceiveQuota()
	ack := s.buildAck(pk.PacketID, packets.Puback, 0, pk.Properties, packets.QosCodes[pk.FixedHeader.Qos]) // [MQTT-4.3.2-4]
	if pk.FixedHeader.Qos == 2 {
		ack = s.buildAck(pk.PacketID, packets.Pubrec, 0, pk.Properties, packets.CodeSuccess) // [MQTT-3.3.4-1] [MQTT-4.3.3-8]
	}

	if ok := cl.State.Inflight.Set(ack); ok {
		atomic.AddInt64(&s.Info.Inflight, 1)
		s.hooks.OnQosPublish(cl, ack, ack.Created, 0)
	}

	err = cl.WritePacket(ack)
	if err != nil {
		return err
	}

	if pk.FixedHeader.Qos == 1 {
		if ok := cl.State.Inflight.Delete(ack.PacketID); ok {
			atomic.AddInt64(&s.Info.Inflight, -1)
		}
		cl.State.Inflight.IncreaseReceiveQuota()
		s.hooks.OnQosComplete(cl, ack)
	}

	s.publishToSubscribers(pk)
	s.hooks.OnPublished(cl, pk)

	return nil
}

// retainMessage adds a message to a topic, and if a persistent store is provided,
// adds the message to the store to be reloaded if necessary.
func (s *Server) retainMessage(cl *Client, pk packets.Packet) {
	if s.Options.Capabilities.RetainAvailable == 0 || pk.Ignore {
		return
	}

	out := pk.Copy(false)
	r := s.Topics.RetainMessage(out)
	s.hooks.OnRetainMessage(cl, pk, r)
	atomic.StoreInt64(&s.Info.Retained, int64(s.Topics.Retained.Len()))
}

// PublishToSubscribers publishes a publish packet to all subscribers with matching topic filters.
func (s *Server) publishToSubscribers(pk packets.Packet) {
	s.PublishToSubscribers(pk, true)
}

// PublishToSubscribers publishes a publish packet to all subscribers with matching topic filters.
// local: true indicates the current process call,false indicates external forwarding
func (s *Server) PublishToSubscribers(pk packets.Packet, local bool) {
	if pk.Ignore {
		return
	}

	if pk.Created == 0 {
		pk.Created = time.Now().Unix()
	}

	pk.Expiry = pk.Created + s.Options.Capabilities.MaximumMessageExpiryInterval
	if pk.Properties.MessageExpiryInterval > 0 {
		pk.Expiry = pk.Created + int64(pk.Properties.MessageExpiryInterval)
	}

	sharedFilters := make(map[string]bool)
	subscribers := s.Topics.Subscribers(pk.TopicName)
	if len(subscribers.Shared) > 0 {
		subscribers = s.hooks.OnSelectSubscribers(subscribers, pk)
		if len(subscribers.SharedSelected) == 0 {
			subscribers.SelectShared()
		}

		// records shared subscriptions for different groups
		for _, sub := range subscribers.SharedSelected {
			sharedFilters[sub.Filter] = false
		}

		subscribers.MergeSharedSelected()
	} else {
		// no shared subscription, publish directly to the cluster
		if !strings.HasPrefix(pk.TopicName, SysPrefix) && local {
			s.hooks.OnPublishedWithSharedFilters(pk, sharedFilters)
		}
	}

	for _, inlineSubscription := range subscribers.InlineSubscriptions {
		inlineSubscription.Handler(s.inlineClient, inlineSubscription.Subscription, pk)
	}

	for id, subs := range subscribers.Subscriptions {
		if cl, ok := s.Clients.Get(id); ok {
			if _, err := s.publishToClient(cl, subs, pk); err != nil {
				if strings.HasPrefix(subs.Filter, "$share") {
					sharedFilters[subs.Filter] = false
				}
				s.Log.Debug("failed publishing packet", "error", err, "client", cl.ID, "packet", pk)
			} else {
				if strings.HasPrefix(subs.Filter, "$share") {
					sharedFilters[subs.Filter] = true
				}
			}
		}
	}

	// publish results with local shared subscriptions
	if len(sharedFilters) > 0 && local {
		s.hooks.OnPublishedWithSharedFilters(pk, sharedFilters)
	}
}

func (s *Server) publishToClient(cl *Client, sub packets.Subscription, pk packets.Packet) (packets.Packet, error) {
	if sub.NoLocal && pk.Origin == cl.ID {
		return pk, nil // [MQTT-3.8.3-3]
	}

	out := pk.Copy(false)
	if !s.hooks.OnACLCheck(cl, pk.TopicName, false) {
		return out, packets.ErrNotAuthorized
	}
	if !sub.FwdRetainedFlag && ((cl.Properties.ProtocolVersion == 5 && !sub.RetainAsPublished) || cl.Properties.ProtocolVersion < 5) { // ![MQTT-3.3.1-13] [v3 MQTT-3.3.1-9]
		out.FixedHeader.Retain = false // [MQTT-3.3.1-12]
	}

	if len(sub.Identifiers) > 0 { // [MQTT-3.3.4-3]
		out.Properties.SubscriptionIdentifier = []int{}
		for _, id := range sub.Identifiers {
			out.Properties.SubscriptionIdentifier = append(out.Properties.SubscriptionIdentifier, id) // [MQTT-3.3.4-4] ![MQTT-3.3.4-5]
		}
		sort.Ints(out.Properties.SubscriptionIdentifier)
	}

	if out.FixedHeader.Qos > sub.Qos {
		out.FixedHeader.Qos = sub.Qos
	}

	if out.FixedHeader.Qos > s.Options.Capabilities.MaximumQos {
		out.FixedHeader.Qos = s.Options.Capabilities.MaximumQos // [MQTT-3.2.2-9]
	}

	if cl.Properties.Props.TopicAliasMaximum > 0 {
		var aliasExists bool
		out.Properties.TopicAlias, aliasExists = cl.State.TopicAliases.Outbound.Set(pk.TopicName)
		if out.Properties.TopicAlias > 0 {
			out.Properties.TopicAliasFlag = true
			if aliasExists {
				out.TopicName = ""
			}
		}
	}

	if out.FixedHeader.Qos > 0 {
		i, err := cl.NextPacketID() // [MQTT-4.3.2-1] [MQTT-4.3.3-1]
		if err != nil {
			s.hooks.OnPacketIDExhausted(cl, pk)
			s.Log.Warn("packet ids exhausted", "error", err, "client", cl.ID, "listener", cl.Net.Listener)
			return out, packets.ErrQuotaExceeded
		}

		out.PacketID = uint16(i) // [MQTT-2.2.1-4]
		sentQuota := atomic.LoadInt32(&cl.State.Inflight.sendQuota)

		if ok := cl.State.Inflight.Set(out); ok { // [MQTT-4.3.2-3] [MQTT-4.3.3-3]
			atomic.AddInt64(&s.Info.Inflight, 1)
			s.hooks.OnQosPublish(cl, out, out.Created, 0)
			cl.State.Inflight.DecreaseSendQuota()
		}

		if sentQuota == 0 && atomic.LoadInt32(&cl.State.Inflight.maximumSendQuota) > 0 {
			out.Expiry = -1
			cl.State.Inflight.Set(out)
			return out, nil
		}
	}

	if cl.Net.Conn == nil || cl.Closed() {
		return out, packets.CodeDisconnect
	}

	select {
	case cl.State.outbound <- &out:
		atomic.AddInt32(&cl.State.outboundQty, 1)
	default:
		atomic.AddInt64(&s.Info.MessagesDropped, 1)
		cl.ops.hooks.OnPublishDropped(cl, pk)
		cl.State.Inflight.Delete(out.PacketID) // packet was dropped due to irregular circumstances, so rollback inflight.
		cl.State.Inflight.IncreaseSendQuota()
		return out, packets.ErrPendingClientWritesExceeded
	}

	return out, nil
}

func (s *Server) publishRetainedToClient(cl *Client, sub packets.Subscription, existed bool) {
	if IsSharedFilter(sub.Filter) {
		return // 4.8.2 Non-normative - Shared Subscriptions - No Retained Messages are sent to the Session when it first subscribes.
	}

	if sub.RetainHandling == 1 && existed || sub.RetainHandling == 2 { // [MQTT-3.3.1-10] [MQTT-3.3.1-11]
		return
	}

	sub.FwdRetainedFlag = true
	for _, pkv := range s.Topics.Messages(sub.Filter) { // [MQTT-3.8.4-4]
		_, err := s.publishToClient(cl, sub, pkv)
		if err != nil {
			s.Log.Debug("failed to publish retained message", "error", err, "client", cl.ID, "listener", cl.Net.Listener, "packet", pkv)
			continue
		}
		s.hooks.OnRetainPublished(cl, pkv)
	}
}

// buildAck builds a standardised ack message for Puback, Pubrec, Pubrel, Pubcomp packets.
func (s *Server) buildAck(packetID uint16, pkt, qos byte, properties packets.Properties, reason packets.Code) packets.Packet {
	if s.Options.Capabilities.Compatibilities.NoInheritedPropertiesOnAck {
		properties = packets.Properties{}
	}
	if reason.Code >= packets.ErrUnspecifiedError.Code {
		properties.ReasonString = reason.Reason
	}

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: pkt,
			Qos:  qos,
		},
		PacketID:   packetID,    // [MQTT-2.2.1-5]
		ReasonCode: reason.Code, // [MQTT-3.4.2-1]
		Properties: properties,
		Created:    time.Now().Unix(),
		Expiry:     time.Now().Unix() + s.Options.Capabilities.MaximumMessageExpiryInterval,
	}

	return pk
}

// processPuback processes a Puback packet, denoting completion of a QOS 1 packet sent from the server.
func (s *Server) processPuback(cl *Client, pk packets.Packet) error {
	if _, ok := cl.State.Inflight.Get(pk.PacketID); !ok {
		return nil // omit, but would be packets.ErrPacketIdentifierNotFound
	}

	if ok := cl.State.Inflight.Delete(pk.PacketID); ok { // [MQTT-4.3.2-5]
		cl.State.Inflight.IncreaseSendQuota()
		atomic.AddInt64(&s.Info.Inflight, -1)
		s.hooks.OnQosComplete(cl, pk)
	}

	return nil
}

// processPubrec processes a Pubrec packet, denoting receipt of a QOS 2 packet sent from the server.
func (s *Server) processPubrec(cl *Client, pk packets.Packet) error {
	if _, ok := cl.State.Inflight.Get(pk.PacketID); !ok { // [MQTT-4.3.3-7] [MQTT-4.3.3-13]
		return cl.WritePacket(s.buildAck(pk.PacketID, packets.Pubrel, 1, pk.Properties, packets.ErrPacketIdentifierNotFound))
	}

	if pk.ReasonCode >= packets.ErrUnspecifiedError.Code || !pk.ReasonCodeValid() { // [MQTT-4.3.3-4]
		if ok := cl.State.Inflight.Delete(pk.PacketID); ok {
			atomic.AddInt64(&s.Info.Inflight, -1)
		}
		cl.ops.hooks.OnQosDropped(cl, pk)
		return nil // as per MQTT5 Section 4.13.2 paragraph 2
	}

	ack := s.buildAck(pk.PacketID, packets.Pubrel, 1, pk.Properties, packets.CodeSuccess) // [MQTT-4.3.3-4] ![MQTT-4.3.3-6]
	cl.State.Inflight.DecreaseReceiveQuota()                                              // -1 RECV QUOTA
	cl.State.Inflight.Set(ack)                                                            // [MQTT-4.3.3-5]
	return cl.WritePacket(ack)
}

// processPubrel processes a Pubrel packet, denoting completion of a QOS 2 packet sent from the client.
func (s *Server) processPubrel(cl *Client, pk packets.Packet) error {
	if _, ok := cl.State.Inflight.Get(pk.PacketID); !ok { // [MQTT-4.3.3-7] [MQTT-4.3.3-13]
		return cl.WritePacket(s.buildAck(pk.PacketID, packets.Pubcomp, 0, pk.Properties, packets.ErrPacketIdentifierNotFound))
	}

	if pk.ReasonCode >= packets.ErrUnspecifiedError.Code || !pk.ReasonCodeValid() { // [MQTT-4.3.3-9]
		if ok := cl.State.Inflight.Delete(pk.PacketID); ok {
			atomic.AddInt64(&s.Info.Inflight, -1)
		}
		cl.ops.hooks.OnQosDropped(cl, pk)
		return nil
	}

	ack := s.buildAck(pk.PacketID, packets.Pubcomp, 0, pk.Properties, packets.CodeSuccess) // [MQTT-4.3.3-11]
	cl.State.Inflight.Set(ack)

	err := cl.WritePacket(ack)
	if err != nil {
		return err
	}

	cl.State.Inflight.IncreaseReceiveQuota()             // +1 RECV QUOTA
	cl.State.Inflight.IncreaseSendQuota()                // +1 SENT QUOTA
	if ok := cl.State.Inflight.Delete(pk.PacketID); ok { // [MQTT-4.3.3-12]
		atomic.AddInt64(&s.Info.Inflight, -1)
		s.hooks.OnQosComplete(cl, pk)
	}

	return nil
}

// processPubcomp processes a Pubcomp packet, denoting completion of a QOS 2 packet sent from the server.
func (s *Server) processPubcomp(cl *Client, pk packets.Packet) error {
	// regardless of whether the pubcomp is a success or failure, we end the qos flow, delete inflight, and restore the quotas.
	cl.State.Inflight.IncreaseReceiveQuota() // +1 RECV QUOTA
	cl.State.Inflight.IncreaseSendQuota()    // +1 SENT QUOTA
	if ok := cl.State.Inflight.Delete(pk.PacketID); ok {
		atomic.AddInt64(&s.Info.Inflight, -1)
		s.hooks.OnQosComplete(cl, pk)
	}

	return nil
}

// processSubscribe processes a Subscribe packet.
func (s *Server) processSubscribe(cl *Client, pk packets.Packet) error {
	pk = s.hooks.OnSubscribe(cl, pk)
	code := packets.CodeSuccess
	if _, ok := cl.State.Inflight.Get(pk.PacketID); ok {
		code = packets.ErrPacketIdentifierInUse
	}

	filterExisted := make([]bool, len(pk.Filters))
	reasonCodes := make([]byte, len(pk.Filters))
	counts := make([]int, len(pk.Filters)) // An array of the number of subscribers for the same filter
	for i, sub := range pk.Filters {
		if code != packets.CodeSuccess {
			reasonCodes[i] = code.Code // NB 3.9.3 Non-normative 0x91
			continue
		} else if !IsValidFilter(sub.Filter, false) {
			reasonCodes[i] = packets.ErrTopicFilterInvalid.Code
		} else if sub.NoLocal && IsSharedFilter(sub.Filter) {
			reasonCodes[i] = packets.ErrProtocolViolationInvalidSharedNoLocal.Code // [MQTT-3.8.3-4]
		} else if !s.hooks.OnACLCheck(cl, sub.Filter, false) {
			reasonCodes[i] = packets.ErrNotAuthorized.Code
			if s.Options.Capabilities.Compatibilities.ObscureNotAuthorized {
				reasonCodes[i] = packets.ErrUnspecifiedError.Code
			}
		} else {
			isNew, count := s.Topics.Subscribe(cl.ID, sub) // [MQTT-3.8.4-3]
			if isNew {
				atomic.AddInt64(&s.Info.Subscriptions, 1)
			}
			cl.State.Subscriptions.Add(sub.Filter, sub) // [MQTT-3.2.2-10]

			if sub.Qos > s.Options.Capabilities.MaximumQos {
				sub.Qos = s.Options.Capabilities.MaximumQos // [MQTT-3.2.2-9]
			}

			filterExisted[i] = !isNew
			reasonCodes[i] = sub.Qos // [MQTT-3.9.3-1] [MQTT-3.8.4-7]
			counts[i] = count
		}

		if reasonCodes[i] > packets.CodeGrantedQos2.Code && cl.Properties.ProtocolVersion < 5 { // MQTT3
			reasonCodes[i] = packets.ErrUnspecifiedError.Code
		}
	}

	ack := packets.Packet{ // [MQTT-3.8.4-1] [MQTT-3.8.4-5]
		FixedHeader: packets.FixedHeader{
			Type: packets.Suback,
		},
		PacketID:    pk.PacketID, // [MQTT-2.2.1-6] [MQTT-3.8.4-2]
		ReasonCodes: reasonCodes, // [MQTT-3.8.4-6]
		Properties: packets.Properties{
			User: pk.Properties.User,
		},
	}

	if code.Code >= packets.ErrUnspecifiedError.Code {
		ack.Properties.ReasonString = code.Reason
	}

	s.hooks.OnSubscribed(cl, pk, reasonCodes, counts)
	err := cl.WritePacket(ack)
	if err != nil {
		return err
	}

	for i, sub := range pk.Filters { // [MQTT-3.3.1-9]
		if reasonCodes[i] >= packets.ErrUnspecifiedError.Code {
			continue
		}

		s.publishRetainedToClient(cl, sub, filterExisted[i])
	}

	return nil
}

// processUnsubscribe processes an unsubscribe packet.
func (s *Server) processUnsubscribe(cl *Client, pk packets.Packet) error {
	code := packets.CodeSuccess
	if _, ok := cl.State.Inflight.Get(pk.PacketID); ok {
		code = packets.ErrPacketIdentifierInUse
	}

	pk = s.hooks.OnUnsubscribe(cl, pk)
	reasonCodes := make([]byte, len(pk.Filters))
	counts := make([]int, len(pk.Filters)) // An array of the number of subscribers for the same filter
	for i, sub := range pk.Filters {       // [MQTT-3.10.4-6] [MQTT-3.11.3-1]
		if code != packets.CodeSuccess {
			reasonCodes[i] = code.Code // NB 3.11.3 Non-normative 0x91
			continue
		}

		q, count := s.Topics.Unsubscribe(sub.Filter, cl.ID)
		if q {
			atomic.AddInt64(&s.Info.Subscriptions, -1)
			reasonCodes[i] = packets.CodeSuccess.Code
		} else {
			reasonCodes[i] = packets.CodeNoSubscriptionExisted.Code
		}
		counts[i] = count

		cl.State.Subscriptions.Delete(sub.Filter) // [MQTT-3.10.4-2] [MQTT-3.10.4-2] ~[MQTT-3.10.4-3]
	}

	ack := packets.Packet{ // [MQTT-3.10.4-4]
		FixedHeader: packets.FixedHeader{
			Type: packets.Unsuback,
		},
		PacketID:    pk.PacketID, // [MQTT-2.2.1-6]  [MQTT-3.10.4-5]
		ReasonCodes: reasonCodes, // [MQTT-3.11.3-2]
		Properties: packets.Properties{
			User: pk.Properties.User,
		},
	}

	if code.Code >= packets.ErrUnspecifiedError.Code {
		ack.Properties.ReasonString = code.Reason
	}

	s.hooks.OnUnsubscribed(cl, pk, reasonCodes, counts)
	return cl.WritePacket(ack)
}

// UnsubscribeClient unsubscribes a client from all of their subscriptions.
func (s *Server) UnsubscribeClient(cl *Client) {
	i := 0
	filterMap := cl.State.Subscriptions.GetAll()

	for k := range filterMap {
		cl.State.Subscriptions.Delete(k)
	}

	if atomic.LoadUint32(&cl.State.isTakenOver) == 1 {
		return
	}

	length := len(filterMap)
	filters := make([]packets.Subscription, length)
	reasonCodes := make([]byte, length)
	counts := make([]int, length) // An array of the number of subscribers for the same filter
	for k, v := range filterMap {
		q, count := s.Topics.Unsubscribe(k, cl.ID)
		if q {
			atomic.AddInt64(&s.Info.Subscriptions, -1)
			reasonCodes[i] = packets.CodeSuccess.Code
		} else {
			reasonCodes[i] = packets.CodeNoSubscriptionExisted.Code
		}
		counts[i] = count
		filters[i] = v
		i++
	}

	s.hooks.OnUnsubscribed(cl, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Unsubscribe}, Filters: filters}, reasonCodes, counts)
}

// processAuth processes an Auth packet.
func (s *Server) processAuth(cl *Client, pk packets.Packet) error {
	_, err := s.hooks.OnAuthPacket(cl, pk)
	if err != nil {
		return err
	}

	return nil
}

// processDisconnect processes a Disconnect packet.
func (s *Server) processDisconnect(cl *Client, pk packets.Packet) error {
	if pk.Properties.SessionExpiryIntervalFlag {
		if pk.Properties.SessionExpiryInterval > 0 && cl.Properties.Props.SessionExpiryInterval == 0 {
			return packets.ErrProtocolViolationZeroNonZeroExpiry
		}

		cl.Properties.Props.SessionExpiryInterval = pk.Properties.SessionExpiryInterval
		cl.Properties.Props.SessionExpiryIntervalFlag = true
	}

	s.loop.willDelayed.Delete(cl.ID) // [MQTT-3.1.3-9] [MQTT-3.1.2-8]
	cl.Stop(packets.CodeDisconnect)  // [MQTT-3.14.4-2]

	return nil
}

// DisconnectClient sends a Disconnect packet to a client and then closes the client connection.
func (s *Server) DisconnectClient(cl *Client, code packets.Code) error {
	out := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Disconnect,
		},
		ReasonCode: code.Code,
		Properties: packets.Properties{},
	}

	if code.Code >= packets.ErrUnspecifiedError.Code {
		out.Properties.ReasonString = code.Reason //  // [MQTT-3.14.2-1]
	}

	// We already have a code we are using to disconnect the client, so we are not
	// interested if the write packet fails due to a closed connection (as we are closing it).
	err := cl.WritePacket(out)
	if !s.Options.Capabilities.Compatibilities.PassiveClientDisconnect {
		cl.Stop(code)
		if code.Code >= packets.ErrUnspecifiedError.Code {
			return code
		}
	}

	return err
}

// publishSysTopics publishes the current values to the server $SYS topics.
// Due to the int to string conversions this method is not as cheap as
// some of the others so the publishing interval should be set appropriately.
func (s *Server) publishSysTopics() {
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type:   packets.Publish,
			Retain: true,
		},
		Created: time.Now().Unix(),
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	atomic.StoreInt64(&s.Info.MemoryAlloc, int64(m.HeapInuse))
	atomic.StoreInt64(&s.Info.Threads, int64(runtime.NumGoroutine()))
	atomic.StoreInt64(&s.Info.Time, time.Now().Unix())
	atomic.StoreInt64(&s.Info.Uptime, time.Now().Unix()-atomic.LoadInt64(&s.Info.Started))
	atomic.StoreInt64(&s.Info.ClientsTotal, int64(s.Clients.Len()))
	atomic.StoreInt64(&s.Info.ClientsDisconnected, atomic.LoadInt64(&s.Info.ClientsTotal)-atomic.LoadInt64(&s.Info.ClientsConnected))

	topics := map[string]string{
		SysPrefix + "/broker/version":              s.Info.Version,
		SysPrefix + "/broker/time":                 AtomicItoa(&s.Info.Time),
		SysPrefix + "/broker/uptime":               AtomicItoa(&s.Info.Uptime),
		SysPrefix + "/broker/started":              AtomicItoa(&s.Info.Started),
		SysPrefix + "/broker/load/bytes/received":  AtomicItoa(&s.Info.BytesReceived),
		SysPrefix + "/broker/load/bytes/sent":      AtomicItoa(&s.Info.BytesSent),
		SysPrefix + "/broker/clients/connected":    AtomicItoa(&s.Info.ClientsConnected),
		SysPrefix + "/broker/clients/disconnected": AtomicItoa(&s.Info.ClientsDisconnected),
		SysPrefix + "/broker/clients/maximum":      AtomicItoa(&s.Info.ClientsMaximum),
		SysPrefix + "/broker/clients/total":        AtomicItoa(&s.Info.ClientsTotal),
		SysPrefix + "/broker/packets/received":     AtomicItoa(&s.Info.PacketsReceived),
		SysPrefix + "/broker/packets/sent":         AtomicItoa(&s.Info.PacketsSent),
		SysPrefix + "/broker/messages/received":    AtomicItoa(&s.Info.MessagesReceived),
		SysPrefix + "/broker/messages/sent":        AtomicItoa(&s.Info.MessagesSent),
		SysPrefix + "/broker/messages/dropped":     AtomicItoa(&s.Info.MessagesDropped),
		SysPrefix + "/broker/messages/inflight":    AtomicItoa(&s.Info.Inflight),
		SysPrefix + "/broker/retained":             AtomicItoa(&s.Info.Retained),
		SysPrefix + "/broker/subscriptions":        AtomicItoa(&s.Info.Subscriptions),
		SysPrefix + "/broker/system/memory":        AtomicItoa(&s.Info.MemoryAlloc),
		SysPrefix + "/broker/system/threads":       AtomicItoa(&s.Info.Threads),
	}

	for topic, payload := range topics {
		pk.TopicName = topic
		pk.Payload = []byte(payload)
		s.Topics.RetainMessage(pk.Copy(false))
		s.publishToSubscribers(pk)
	}

	s.hooks.OnSysInfoTick(s.Info)
}

// Close attempts to gracefully shut down the server, all listeners, clients, and stores.
func (s *Server) Close() error {
	close(s.done)

	// Notify hooks that server is stopping, but keep them active
	s.hooks.OnStopped()

	// Close all listeners and clients (this will trigger OnDisconnect callbacks)
	// Hooks are still active at this point, so database operations will work
	s.Listeners.CloseAll(s.closeListenerClients)

	// Now that all clients are disconnected and their data is persisted,
	// we can safely stop the hooks (including closing the database)
	s.hooks.Stop()

	s.Log.Info("comqtt server stopped")
	return nil
}

// closeListenerClients closes all clients on the specified listener.
func (s *Server) closeListenerClients(listener string) {
	clients := s.Clients.GetByListener(listener)
	for _, cl := range clients {
		_ = s.DisconnectClient(cl, packets.ErrServerShuttingDown)
	}
}

// sendLWT issues an LWT message to a topic when a client disconnects.
func (s *Server) sendLWT(cl *Client) {
	if atomic.LoadUint32(&cl.Properties.Will.Flag) == 0 {
		return
	}

	modifiedLWT := s.hooks.OnWill(cl, cl.Properties.Will)

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type:   packets.Publish,
			Retain: modifiedLWT.Retain, // [MQTT-3.1.2-14] [MQTT-3.1.2-15]
			Qos:    modifiedLWT.Qos,
		},
		TopicName: modifiedLWT.TopicName,
		Payload:   modifiedLWT.Payload,
		Properties: packets.Properties{
			User: modifiedLWT.User,
		},
		Origin:  cl.ID,
		Created: time.Now().Unix(),
	}

	if cl.Properties.Will.WillDelayInterval > 0 {
		pk.Connect.WillProperties.WillDelayInterval = cl.Properties.Will.WillDelayInterval
		pk.Expiry = time.Now().Unix() + int64(pk.Connect.WillProperties.WillDelayInterval)
		s.loop.willDelayed.Add(cl.ID, pk)
		return
	}

	if pk.FixedHeader.Retain {
		s.retainMessage(cl, pk)
	}

	s.publishToSubscribers(pk)                      // [MQTT-3.1.2-8]
	atomic.StoreUint32(&cl.Properties.Will.Flag, 0) // [MQTT-3.1.2-10]
	s.hooks.OnWillSent(cl, pk)
}

// readStore reads in any data from the persistent datastore (if applicable).
func (s *Server) readStore() error {
	if s.hooks.Provides(StoredClients) {
		clients, err := s.hooks.StoredClients()
		if err != nil {
			return fmt.Errorf("failed to load clients; %w", err)
		}
		s.loadClients(clients)
		s.Log.Debug("loaded clients from store", "len", len(clients))
	}

	if s.hooks.Provides(StoredSubscriptions) {
		subs, err := s.hooks.StoredSubscriptions()
		if err != nil {
			return fmt.Errorf("load subscriptions; %w", err)
		}
		s.loadSubscriptions(subs)
		s.Log.Debug("loaded subscriptions from store", "len", len(subs))
	}

	if s.hooks.Provides(StoredInflightMessages) {
		inflight, err := s.hooks.StoredInflightMessages()
		if err != nil {
			return fmt.Errorf("load inflight; %w", err)
		}
		s.loadInflight(inflight)
		s.Log.Debug("loaded inflights from store", "len", len(inflight))
	}

	if s.hooks.Provides(StoredRetainedMessages) {
		retained, err := s.hooks.StoredRetainedMessages()
		if err != nil {
			return fmt.Errorf("load retained; %w", err)
		}
		s.loadRetained(retained)
		s.Log.Debug("loaded retained messages from store", "len", len(retained))
	}

	if s.hooks.Provides(StoredSysInfo) {
		sysInfo, err := s.hooks.StoredSysInfo()
		if err != nil {
			return fmt.Errorf("load server info; %w", err)
		}
		s.loadServerInfo(sysInfo.Info)
		s.Log.Debug("loaded $SYS info from store")
	}

	return nil
}

// loadServerInfo restores server info from the datastore.
func (s *Server) loadServerInfo(v system.Info) {
	if s.Options.Capabilities.Compatibilities.RestoreSysInfoOnRestart {
		atomic.StoreInt64(&s.Info.BytesReceived, v.BytesReceived)
		atomic.StoreInt64(&s.Info.BytesSent, v.BytesSent)
		atomic.StoreInt64(&s.Info.ClientsMaximum, v.ClientsMaximum)
		atomic.StoreInt64(&s.Info.ClientsTotal, v.ClientsTotal)
		atomic.StoreInt64(&s.Info.ClientsDisconnected, v.ClientsDisconnected)
		atomic.StoreInt64(&s.Info.MessagesReceived, v.MessagesReceived)
		atomic.StoreInt64(&s.Info.MessagesSent, v.MessagesSent)
		atomic.StoreInt64(&s.Info.MessagesDropped, v.MessagesDropped)
		atomic.StoreInt64(&s.Info.PacketsReceived, v.PacketsReceived)
		atomic.StoreInt64(&s.Info.PacketsSent, v.PacketsSent)
		atomic.StoreInt64(&s.Info.InflightDropped, v.InflightDropped)
	}
	atomic.StoreInt64(&s.Info.Retained, v.Retained)
	atomic.StoreInt64(&s.Info.Inflight, v.Inflight)
	atomic.StoreInt64(&s.Info.Subscriptions, v.Subscriptions)
}

// loadSubscriptions restores subscriptions from the datastore.
func (s *Server) loadSubscriptions(v []storage.Subscription) {
	for _, sub := range v {
		sb := packets.Subscription{
			Filter:            sub.Filter,
			RetainHandling:    sub.RetainHandling,
			Qos:               sub.Qos,
			RetainAsPublished: sub.RetainAsPublished,
			NoLocal:           sub.NoLocal,
			Identifier:        sub.Identifier,
		}
		// count represents the number of subscribers for the current filter
		// isNew represents whether to subscribe for the first time
		if isNew, count := s.Topics.Subscribe(sub.Client, sb); isNew {
			if cl, ok := s.Clients.Get(sub.Client); ok {
				cl.State.Subscriptions.Add(sub.Filter, sb)
				s.hooks.OnSubscribed(cl, packets.Packet{Filters: []packets.Subscription{sb}}, []byte{sub.Qos}, []int{count})
			}
		}
	}
}

// loadClients restores clients from the datastore.
func (s *Server) loadClients(v []storage.Client) {
	for _, c := range v {
		cl := s.NewClient(nil, c.Listener, c.ID, false)
		cl.Properties.Username = c.Username
		cl.Properties.Clean = c.Clean
		cl.Properties.ProtocolVersion = c.ProtocolVersion
		cl.Properties.Props = packets.Properties{
			SessionExpiryInterval:     c.Properties.SessionExpiryInterval,
			SessionExpiryIntervalFlag: c.Properties.SessionExpiryIntervalFlag,
			AuthenticationMethod:      c.Properties.AuthenticationMethod,
			AuthenticationData:        c.Properties.AuthenticationData,
			RequestProblemInfoFlag:    c.Properties.RequestProblemInfoFlag,
			RequestProblemInfo:        c.Properties.RequestProblemInfo,
			RequestResponseInfo:       c.Properties.RequestResponseInfo,
			ReceiveMaximum:            c.Properties.ReceiveMaximum,
			TopicAliasMaximum:         c.Properties.TopicAliasMaximum,
			User:                      c.Properties.User,
			MaximumPacketSize:         c.Properties.MaximumPacketSize,
		}
		cl.Properties.Will = Will(c.Will)
		s.Clients.Add(cl)
	}
}

// loadInflight restores inflight messages from the datastore.
func (s *Server) loadInflight(v []storage.Message) {
	for _, msg := range v {
		if client, ok := s.Clients.Get(msg.Origin); ok {
			client.State.Inflight.Set(msg.ToPacket())
		}
	}
}

// loadRetained restores retained messages from the datastore.
func (s *Server) loadRetained(v []storage.Message) {
	for _, msg := range v {
		s.Topics.RetainMessage(msg.ToPacket())
	}
}

// clearExpiredClients deletes all clients which have been disconnected for longer
// than their given expiry intervals.
func (s *Server) clearExpiredClients(dt int64) {
	for id, client := range s.Clients.GetAll() {
		disconnected := atomic.LoadInt64(&client.State.disconnected)
		if disconnected == 0 {
			continue
		}

		expire := s.Options.Capabilities.MaximumSessionExpiryInterval
		if client.Properties.ProtocolVersion == 5 && client.Properties.Props.SessionExpiryIntervalFlag {
			expire = client.Properties.Props.SessionExpiryInterval
		}

		if disconnected+int64(expire) < dt {
			s.hooks.OnClientExpired(client)
			s.Clients.Delete(id) // [MQTT-4.1.0-2]
		}
	}
}

// clearExpiredRetainedMessage deletes retained messages from topics if they have expired.
func (s *Server) clearExpiredRetainedMessages(now int64) {
	for filter, pk := range s.Topics.Retained.GetAll() {
		if (pk.Expiry > 0 && pk.Expiry < now) || pk.Created+s.Options.Capabilities.MaximumMessageExpiryInterval < now {
			s.Topics.Retained.Delete(filter)
			s.hooks.OnRetainedExpired(filter)
		}
	}
}

// clearExpiredInflights deletes any inflight messages which have expired.
func (s *Server) clearExpiredInflights(now int64) {
	for _, client := range s.Clients.GetAll() {
		if deleted := client.ClearInflights(now, s.Options.Capabilities.MaximumMessageExpiryInterval); len(deleted) > 0 {
			for _, id := range deleted {
				s.hooks.OnQosDropped(client, packets.Packet{PacketID: id})
			}
		}
	}
}

// sendDelayedLWT sends any LWT messages which have reached their issue time.
func (s *Server) sendDelayedLWT(dt int64) {
	for id, pk := range s.loop.willDelayed.GetAll() {
		if dt > pk.Expiry {
			s.publishToSubscribers(pk) // [MQTT-3.1.2-8]
			if cl, ok := s.Clients.Get(id); ok {
				if pk.FixedHeader.Retain {
					s.retainMessage(cl, pk)
				}
				cl.Properties.Will = Will{} // [MQTT-3.1.2-10]
				s.hooks.OnWillSent(cl, pk)
			}
			s.loop.willDelayed.Delete(id)
		}
	}
}

// AtomicItoa converts an int64 point to a string.
func AtomicItoa(ptr *int64) string {
	return strconv.FormatInt(atomic.LoadInt64(ptr), 10)
}
