package main

import (
	"fmt"
	"log"
	"testing"

	"github.com/gorilla/websocket"
)

type TestConfig struct {
	ClientCount int
}

func DialerServer() {
	dialer := websocket.DefaultDialer

	conn, _, err := dialer.Dial(fmt.Sprintf("ws://localhost:%s", WSPort), nil)
	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println("Connected to the server", conn.LocalAddr().String())
}

func TestConnection(t *testing.T) {
	tc := TestConfig{
		ClientCount: 3,
	}

	for range tc.ClientCount {
		go DialerServer()
	}
}
