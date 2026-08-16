package main

import (
	"context"
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

type TestClient struct {
	conn    *websocket.Conn
	msgChan chan *ReqType
	done    chan struct{}
	ctx     context.Context
}

func NewTestClient(conn *websocket.Conn, ctx context.Context) *TestClient {
	return &TestClient{
		conn:    conn,
		msgChan: make(chan *ReqType, 64),
		done:    make(chan struct{}),
		ctx:     ctx,
	}
}

func (c *TestClient) writeMsgLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-c.msgChan:
			err := c.conn.WriteJSON(&msg)
			if err != nil {
				fmt.Printf("Error sending msg %v\n", err.Error())
				return
			}
		}
	}
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
	ctx, cancel := context.WithCancel(context.Background())
	time.Sleep(1 * time.Second)
	clientCount := 50
	brCnt := 10

	tc := TestConfig{
		ClientCount:    clientCount,
		wg:             new(sync.WaitGroup),
		broadCstMsgCnt: new(atomic.Int64),
		targetMsgCnt:   clientCount * brCnt,
	}

	tc.wg.Add(tc.ClientCount + 1)

	brConn := DialerServer(&tc)
	brClient := NewTestClient(brConn, ctx)
	go brClient.writeMsgLoop()

	for range tc.ClientCount {
		go DialerServer(&tc)
	}

	for range brCnt {
		msg := ReqType{
			MsgType: MsgType_BroadCast,
			Data:    "hello from test",
		}

		brClient.msgChan <- &msg
	}

	tc.wg.Wait()
	cancel()
	time.Sleep(1 * time.Second)
	fmt.Println("exiting test case")
}
