// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2024 comqtt
// SPDX-FileContributor: comqtt

package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// Options contains configuration for the device events hook.
type Options struct {
	NodeName string `yaml:"node-name" json:"node-name"` // Node name for topic construction
}

// DeviceEventsHook publishes device connect/disconnect events to $SYS topics.
type DeviceEventsHook struct {
	mqtt.HookBase
	config   *Options
	nodeName string
	server   *mqtt.Server
}

// DeviceEvent represents the structure of device event messages.
type DeviceEvent struct {
	ClientID        string `json:"client_id"`
	RemoteAddr      string `json:"remote_addr"`
	Username        string `json:"username,omitempty"`
	Timestamp       int64  `json:"timestamp"`
	Event           string `json:"event"` // "connected" or "disconnected"
	ProtocolVersion byte   `json:"protocol_version,omitempty"`
	CleanSession    bool   `json:"clean_session,omitempty"`
	Keepalive       uint16 `json:"keepalive,omitempty"`
}

// ID returns the ID of the hook.
func (h *DeviceEventsHook) ID() string {
	return "device-events"
}

// Provides indicates which hook methods this hook provides.
func (h *DeviceEventsHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnSessionEstablished,
		mqtt.OnDisconnect,
	}, []byte{b})
}

// Init initializes the hook with the provided configuration.
func (h *DeviceEventsHook) Init(config any) error {
	if config == nil {
		h.config = &Options{
			NodeName: "single", // Default to "single" mode
		}
	} else {
		cfg, ok := config.(*Options)
		if !ok {
			return fmt.Errorf("invalid config type for device events hook")
		}
		h.config = cfg
	}

	// Set node name
	h.nodeName = h.config.NodeName
	h.Log.Info("device events hook initialized", "node-name", h.nodeName)
	return nil
}

// Stop gracefully stops the hook.
func (h *DeviceEventsHook) Stop() error {
	h.Log.Info("device events hook stopped")
	return nil
}

// SetServer sets the MQTT server reference for publishing events.
func (h *DeviceEventsHook) SetServer(server *mqtt.Server) {
	h.server = server
}

// OnSessionEstablished is called when a client session is established (device comes online).
func (h *DeviceEventsHook) OnSessionEstablished(cl *mqtt.Client, pk packets.Packet) {
	event := DeviceEvent{
		ClientID:        cl.ID,
		RemoteAddr:      cl.Net.Remote,
		Username:        string(cl.Properties.Username),
		Timestamp:       time.Now().Unix(),
		Event:           "connected",
		ProtocolVersion: cl.Properties.ProtocolVersion,
		CleanSession:    pk.Connect.Clean,
		Keepalive:       pk.Connect.Keepalive,
	}

	h.publishEvent(event, "connected")
}

// OnDisconnect is called when a client disconnects (device goes offline).
func (h *DeviceEventsHook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	event := DeviceEvent{
		ClientID:   cl.ID,
		RemoteAddr: cl.Net.Remote,
		Username:   string(cl.Properties.Username),
		Timestamp:  time.Now().Unix(),
		Event:      "disconnected",
	}

	h.publishEvent(event, "disconnected")
}

// publishEvent publishes the device event to the appropriate $SYS topic.
func (h *DeviceEventsHook) publishEvent(event DeviceEvent, eventType string) {
	// Construct topic: $SYS/brokers/${node}/clients/${clientid}/connected or disconnected
	topic := fmt.Sprintf("$SYS/brokers/%s/clients/%s/%s", h.nodeName, event.ClientID, eventType)

	// Marshal event to JSON
	payload, err := json.Marshal(event)
	if err != nil {
		h.Log.Error("failed to marshal device event", "error", err, "client", event.ClientID, "event", eventType)
		return
	}

	// Create MQTT packet
	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type:   packets.Publish,
			Retain: false, // Don't retain these events
		},
		TopicName: topic,
		Payload:   payload,
		Created:   time.Now().Unix(),
	}

	// Publish the event using the server's internal publishing mechanism
	if h.server != nil {
		// Use the server's inline client if available, otherwise publish directly
		if h.server.Options.InlineClient {
			err := h.server.Publish(topic, payload, false, 0)
			if err != nil {
				h.Log.Error("failed to publish device event via inline client", "error", err, "topic", topic)
			}
		} else {
			// Publish directly to subscribers
			h.server.PublishToSubscribers(pk, true)
		}

		h.Log.Debug("published device event", "topic", topic, "client", event.ClientID, "event", eventType)
	} else {
		h.Log.Error("server reference not set, cannot publish device event", "topic", topic, "client", event.ClientID)
	}
}
