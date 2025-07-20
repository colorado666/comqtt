package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/wind-c/comqtt/v2/mqtt"
	"github.com/wind-c/comqtt/v2/mqtt/hooks/auth"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
	pa "github.com/wind-c/comqtt/v2/plugin/auth"
)

const (
	TypeJson = "application/json"
	TypeForm = "application/x-www-form-urlencoded"
)

type Options struct {
	pa.Blacklist
	AuthMode    byte   `json:"auth-mode" yaml:"auth-mode"`
	AclMode     byte   `json:"acl-mode" yaml:"acl-mode"`
	TlsEnable   bool   `json:"tls-enable" yaml:"tls-enable"`
	TlsCert     string `json:"tls-cert" yaml:"tls-cert"`
	TlsKey      string `json:"tls-key" yaml:"tls-key"`
	Method      string `json:"method" yaml:"method"`
	ContentType string `json:"content-type" yaml:"content-type"`
	AuthUrl     string `json:"auth-url" yaml:"auth-url"`
	AclUrl      string `json:"acl-url" yaml:"acl-url"`
}

// Auth is an auth controller which allows access to all connections and topics.
type Auth struct {
	mqtt.HookBase
	config *Options
}

type respAuth struct {
	Result string `json:"result"`
}

// ID returns the ID of the hook.
func (a *Auth) ID() string {
	return "auth-http"
}

// Provides indicates which hook methods this hook provides.
func (a *Auth) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnConnectAuthenticate,
		mqtt.OnACLCheck,
	}, []byte{b})
}

func (a *Auth) Init(config any) error {
	if _, ok := config.(*Options); config == nil || (!ok && config != nil) {
		return mqtt.ErrInvalidConfigType
	}

	a.config = config.(*Options)
	a.Log.Info("", "auth-url", a.config.AuthUrl, "acl-url", a.config.AclUrl)

	return nil
}

// OnConnectAuthenticate returns true if the connecting client has rules which provide access
// in the auth ledger.
func (a *Auth) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	if a.config.AuthMode == byte(auth.AuthAnonymous) {
		return true
	}

	// check blacklist
	if n, ok := a.config.CheckBLAuth(cl, pk); n >= 0 { // It's on the blacklist
		return ok
	}

	clientID := cl.ID
	username := string(cl.Properties.Username)
	password := string(pk.Connect.Password)
	address := cl.Net.Remote

	var err error
	var resp *http.Response
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	// 默认 post 操作，json 格式
	payload := make(map[string]string)
	payload["username"] = username
	payload["password"] = password
	payload["client_id"] = clientID
	payload["peer_host"] = address
	bytesData, _ := json.Marshal(payload)
	resp, err = http.Post(a.config.AuthUrl, TypeJson, bytes.NewBuffer(bytesData))
	if err != nil {
		return false
	}
	if resp.StatusCode != 200 {
		return false
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}

	var ra respAuth
	err = json.Unmarshal(body, &ra)
	if err != nil {
		return false
	}
	if ra.Result == "allow" {
		fmt.Println("auth success")
		return true
	} else {
		fmt.Println("auth failed")
		return false
	}

	// if string(body) == "1" {
	// 	fmt.Println("auth success")
	// 	return true
	// } else {
	// 	fmt.Println("auth failed")
	// 	return false
	// }
}

// OnACLCheck returns true if the connecting client has matching read or write access to subscribe
// or publish to a given topic.
func (a *Auth) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	if a.config.AclMode == byte(auth.AuthAnonymous) {
		return true
	}

	// check blacklist
	if n, ok := a.config.CheckBLAcl(cl, topic, write); n >= 0 { // It's on the blacklist
		return ok
	}

	var err error
	var resp *http.Response
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	clientID := cl.ID
	username := string(cl.Properties.Username)
	address := cl.Net.Remote

	var action string
	if write {
		action = "publish"
	} else {
		action = "subscribe"
	}

	// 默认 post 操作，json 格式
	payload := make(map[string]string)
	payload["username"] = username
	payload["client_id"] = clientID
	payload["peer_host"] = address
	payload["topic"] = topic
	payload["action"] = action
	bytesData, _ := json.Marshal(payload)
	resp, err = http.Post(a.config.AclUrl, TypeJson, bytes.NewBuffer(bytesData))
	if err != nil {
		return false
	}

	if resp.StatusCode != 200 {
		return false
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}

	var ra respAuth
	err = json.Unmarshal(body, &ra)
	if err != nil {
		return false
	}
	if ra.Result == "allow" {
		fmt.Println("auth success")
		return true
	} else {
		fmt.Println("auth failed")
		return false
	}

	// if string(body) == "1" {
	// 	fmt.Println("auth success")
	// 	return true
	// } else {
	// 	fmt.Println("auth failed")
	// 	return false
	// }
}
