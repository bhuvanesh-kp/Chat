package main

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type TestConfig struct {
	ClientCount int
	wg          *sync.WaitGroup
}

func DialerServer(wg *sync.WaitGroup) {

	dialer := websocket.DefaultDialer

	conn, _, err := dialer.Dial(fmt.Sprintf("ws://localhost%s", WSPort), nil)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer func() {
		conn.Close()
		wg.Done()
	}()

	fmt.Println("Connected to the server ", conn.LocalAddr().String())
	time.Sleep(1 * time.Second)
}

func TestConnection(t *testing.T) {
	go CreateServer()
	time.Sleep(1 * time.Second)

	tc := TestConfig{
		ClientCount: 50,
		wg:          new(sync.WaitGroup),
	}

	tc.wg.Add(tc.ClientCount)

	for range tc.ClientCount {
		go DialerServer(tc.wg)
	}

	tc.wg.Wait()
	fmt.Println("exiting test case")
}
