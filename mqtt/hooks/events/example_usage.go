// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2024 comqtt
// SPDX-FileContributor: comqtt

package events

import (
	"fmt"
	"log"

	"github.com/wind-c/comqtt/v2/mqtt"
	"github.com/wind-c/comqtt/v2/mqtt/hooks/auth"
	"github.com/wind-c/comqtt/v2/mqtt/listeners"
)

// ExampleDeviceEventsUsage demonstrates how to use the DeviceEventsHook
func ExampleDeviceEventsUsage() {
	// Create a new MQTT server with inline client enabled
	server := mqtt.New(&mqtt.Options{
		InlineClient: true, // Required for publishing events
	})

	// Add allow-all auth hook for testing
	err := server.AddHook(new(auth.AllowHook), nil)
	if err != nil {
		log.Fatal("Failed to add auth hook:", err)
	}

	// Add device events hook for single mode
	deviceEventsConfig := &Options{
		NodeName: "single",
	}

	deviceEventsHook := new(DeviceEventsHook)
	deviceEventsHook.SetServer(server)

	err = server.AddHook(deviceEventsHook, deviceEventsConfig)
	if err != nil {
		log.Fatal("Failed to add device events hook:", err)
	}

	// Add a TCP listener
	tcp := listeners.NewTCP("tcp", ":1883", nil)
	err = server.AddListener(tcp)
	if err != nil {
		log.Fatal("Failed to add TCP listener:", err)
	}

	// Start the server
	fmt.Println("Starting MQTT server with device events...")
	fmt.Println("Device events will be published to:")
	fmt.Println("  - $SYS/brokers/single/clients/+/connected")
	fmt.Println("  - $SYS/brokers/single/clients/+/disconnected")
	fmt.Println("")
	fmt.Println("Connect a client to see events in action!")

	err = server.Serve()
	if err != nil {
		log.Fatal("Server error:", err)
	}
}

// ExampleClusterMode demonstrates device events in cluster mode
func ExampleClusterMode() {
	server := mqtt.New(&mqtt.Options{
		InlineClient: true,
	})

	// Add auth hook
	err := server.AddHook(new(auth.AllowHook), nil)
	if err != nil {
		log.Fatal("Failed to add auth hook:", err)
	}

	// Add device events hook for cluster mode
	deviceEventsConfig := &Options{
		NodeName: "cluster-node-01",
	}

	deviceEventsHook := new(DeviceEventsHook)
	deviceEventsHook.SetServer(server)

	err = server.AddHook(deviceEventsHook, deviceEventsConfig)
	if err != nil {
		log.Fatal("Failed to add device events hook:", err)
	}

	fmt.Println("Cluster mode device events configured.")
	fmt.Println("Events will be published to:")
	fmt.Println("  - $SYS/brokers/cluster-node-01/clients/+/connected")
	fmt.Println("  - $SYS/brokers/cluster-node-01/clients/+/disconnected")
}
