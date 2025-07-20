# Device Events Hook Implementation

## 概述

我已经为你实现了一个完整的设备上线/离线事件通知 Hook，它会在设备连接和断开时发布消息到指定的 `$SYS` 主题。

## 实现的功能

### 1. 主题结构
```
$SYS/brokers/${node}/clients/${clientid}/connected    # 设备上线事件
$SYS/brokers/${node}/clients/${clientid}/disconnected # 设备下线事件
```

其中：
- `${node}` 在 single 模式下为 "single"，在 cluster 模式下为实际的节点名称
- `${clientid}` 为 MQTT 客户端标识符

### 2. 消息格式
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

## 文件结构

```
mqtt/hooks/events/
├── device_events.go          # 主要实现文件
├── device_events_test.go     # 测试文件
├── example_integration.go    # 集成示例
├── example_usage.go          # 使用示例
├── device-events.yml         # 配置文件示例
└── README.md                 # 详细文档
```

## 集成方式

### Single 模式集成

已经修改了 `cmd/single/main.go`，添加了：

```go
// 导入
"github.com/wind-c/comqtt/v2/mqtt/hooks/events"

// 初始化函数
func initDeviceEvents(server *mqtt.Server, conf *config.Config) {
    config := &events.Options{
        NodeName:     "single",
    }
    
    hook := new(events.DeviceEventsHook)
    hook.SetServer(server)
    
    onError(server.AddHook(hook, config), "init device events")
}
```

### Cluster 模式集成

已经修改了 `cmd/cluster/main.go`，添加了：

```go
// 导入
"github.com/wind-c/comqtt/v2/mqtt/hooks/events"

// 初始化函数
func initDeviceEvents(server *mqtt.Server, conf *config.Config) {
    config := &events.Options{
        NodeName:     conf.Cluster.NodeName,
    }
    
    hook := new(events.DeviceEventsHook)
    hook.SetServer(server)
    
    onError(server.AddHook(hook, config), "init device events")
}
```

## 配置选项

```go
type Options struct {
    NodeName     string `yaml:"node-name"`      // 节点名称
}
```

## 使用示例

### 1. 基本使用
```go
server := mqtt.New(&mqtt.Options{
    InlineClient: true, // 必须启用以发布事件
})

hook := new(events.DeviceEventsHook)
hook.SetServer(server)

err := server.AddHook(hook, &events.Options{
    NodeName: "my-broker",
})
```

### 2. 订阅设备事件
```bash
# 订阅所有设备事件
mosquitto_sub -t '$SYS/brokers/+/clients/+/+'

# 订阅连接事件
mosquitto_sub -t '$SYS/brokers/+/clients/+/connected'

# 订阅断开事件
mosquitto_sub -t '$SYS/brokers/+/clients/+/disconnected'

# 订阅特定设备事件
mosquitto_sub -t '$SYS/brokers/+/clients/device001/+'
```

## 测试

运行测试：
```bash
go test ./mqtt/hooks/events -v
```

## 特性

1. **自动模式检测**：可以自动检测 single 或 cluster 模式
2. **可配置保留**：可以选择是否保留事件消息
3. **完整信息**：包含客户端 ID、远程地址、用户名、时间戳等完整信息
4. **性能优化**：使用轻量级实现，不影响 broker 性能
5. **灵活集成**：可以轻松集成到现有项目中

## 事件触发时机

- **连接事件**：在 `OnSessionEstablished` 时触发，确保客户端完全连接后才发送
- **断开事件**：在 `OnDisconnect` 时触发，无论是正常断开还是异常断开

## 注意事项

1. 需要启用 `InlineClient` 选项才能发布事件
2. 事件消息默认使用 QoS 0 发布
3. 可以根据需要配置消息保留策略
4. 在集群模式下，每个节点都会发布自己的设备事件

这个实现提供了完整的设备上线/离线事件通知功能，可以满足你的监控和管理需求。
