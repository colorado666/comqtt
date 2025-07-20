# Device Events Hook

This hook publishes device connect/disconnect events to `$SYS` topics, providing real-time notifications when devices come online or go offline.

## Features

- Publishes device connection events to structured `$SYS` topics
- Supports both single and cluster modes
- Configurable message retention
- JSON-formatted event payloads with comprehensive device information
- Auto-detection of node name in different deployment modes

## Topic Structure

Events are published to the following topics:

```
$SYS/brokers/${node}/clients/${clientid}/connected    # Device online event
$SYS/brokers/${node}/clients/${clientid}/disconnected # Device offline event
```

Where:
- `${node}` is "single" in single mode, or the cluster node name in cluster mode
- `${clientid}` is the MQTT client identifier

## Event Message Format

```json
{
  "client_id": "device001",
  "remote_addr": "192.168.1.100:54321",
  "username": "sensor_user",
  "timestamp": 1703123456,
  "event": "connected",
  "protocol_version": 4,
  "clean_session": true,
  "keepalive": 60
}
```

### Fields Description

- `client_id`: MQTT client identifier
- `remote_addr`: Client's remote IP address and port
- `username`: MQTT username (if provided)
- `timestamp`: Unix timestamp when the event occurred
- `event`: Event type ("connected" or "disconnected")
- `protocol_version`: MQTT protocol version (3, 4, or 5)
- `clean_session`: Whether the client uses clean session (connect events only)
- `keepalive`: Client keepalive interval in seconds (connect events only)

## Configuration

### Options

```go
type Options struct {
    NodeName     string `yaml:"node-name" json:"node-name"`         // Node name for topic construction
}
```

### YAML Configuration

```yaml
# device-events.yml
node-name: "my-broker-node"    # Custom node name (optional if auto-detect is true)
auto-detect: true              # Auto-detect node name (default: true)
retain-events: false           # Don't retain event messages (default: false)
```

## Usage Examples

### Single Mode

```go
package main

import (
    "github.com/wind-c/comqtt/v2/mqtt"
    "github.com/wind-c/comqtt/v2/mqtt/hooks/events"
)

func main() {
    server := mqtt.New(nil)
    
    // Add device events hook with default settings (single mode)
    err := server.AddHook(new(events.DeviceEventsHook), nil)
    if err != nil {
        panic(err)
    }
    
    // Start server...
}
```

### Cluster Mode

```go
package main

import (
    "github.com/wind-c/comqtt/v2/mqtt"
    "github.com/wind-c/comqtt/v2/mqtt/hooks/events"
)

func main() {
    server := mqtt.New(nil)
    
    // Add device events hook with cluster node name
    config := &events.Options{
        NodeName:     "cluster-node-01",
    }
    
    err := server.AddHook(new(events.DeviceEventsHook), config)
    if err != nil {
        panic(err)
    }
    
    // Start server...
}
```

### With Configuration File

```go
package main

import (
    "github.com/wind-c/comqtt/v2/mqtt"
    "github.com/wind-c/comqtt/v2/mqtt/hooks/events"
    "github.com/wind-c/comqtt/v2/plugin"
)

func main() {
    server := mqtt.New(nil)
    
    // Load configuration from YAML file
    var config events.Options
    err := plugin.LoadYaml("device-events.yml", &config)
    if err != nil {
        panic(err)
    }
    
    err = server.AddHook(new(events.DeviceEventsHook), &config)
    if err != nil {
        panic(err)
    }
    
    // Start server...
}
```

## Subscribing to Events

Clients can subscribe to device events using MQTT topic filters:

```bash
# Subscribe to all device events for a specific node
mosquitto_sub -t '$SYS/brokers/single/clients/+/+'

# Subscribe to connection events only
mosquitto_sub -t '$SYS/brokers/+/clients/+/connected'

# Subscribe to disconnection events only  
mosquitto_sub -t '$SYS/brokers/+/clients/+/disconnected'

# Subscribe to events for a specific device
mosquitto_sub -t '$SYS/brokers/+/clients/device001/+'
```

## Integration Notes

- The hook automatically detects whether inline client is enabled and uses the appropriate publishing method
- Events are published with QoS 0 for performance
- Message retention can be configured based on your monitoring requirements
- The hook is designed to be lightweight and not impact broker performance

## Testing

Run the tests with:

```bash
go test ./mqtt/hooks/events
```
