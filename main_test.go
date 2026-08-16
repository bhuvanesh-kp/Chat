package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type TestConfig struct {
	ClientCount    int
	wg             *sync.WaitGroup
	broadCstMsgCnt *atomic.Int64
	targetMsgCnt   int
}

func DialerServer(tc *TestConfig) *websocket.Conn {
	exitChan := make(chan struct{})
	dialer := websocket.DefaultDialer

	conn, _, err := dialer.Dial(fmt.Sprintf("ws://localhost%s", WSPort), nil)
	if err != nil {
		log.Fatal(err.Error())
	}

	go func() {
		for {
			time.Sleep(2 * time.Second)
			if tc.targetMsgCnt == int(tc.broadCstMsgCnt.Load()) {
				close(exitChan)
				return
			}
		}
	}()

	go func() {
		<-exitChan
		conn.Close()
		tc.wg.Done()
	}()

	// time.Sleep(2 * time.Second)

	go func() {
		for {
			_, b, err := conn.ReadMessage()
			if err != nil {
				return
			}

			if len(b) > 0 {
				tc.broadCstMsgCnt.Add(1)
			}
		}
	}()

	return conn
}

func TestConnection(t *testing.T) {
	go CreateServer()
	time.Sleep(1 * time.Second)
	clientCount := 5
	brCnt := 3

	tc := TestConfig{
		ClientCount:    clientCount,
		wg:             new(sync.WaitGroup),
		broadCstMsgCnt: new(atomic.Int64),
		targetMsgCnt:   clientCount * brCnt,
	}

	tc.wg.Add(tc.ClientCount + 1)

	brClient := DialerServer(&tc)

	for range tc.ClientCount {
		go DialerServer(&tc)
	}

	for range brCnt {
		msg := ReqType{
			MsgType: MsgType_BroadCast,
			Data:    "hello from test",
		}

		time.Sleep(100 * time.Millisecond)
		err := brClient.WriteJSON(&msg)
		if err != nil {
			fmt.Printf("Error Sending msg %v\n", err.Error())
			return
		}

	}

	tc.wg.Wait()
	time.Sleep(1 * time.Second)
	fmt.Println("exiting test case")
}
