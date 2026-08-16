package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var (
	WSPort = ":3245"
)

type MsgType string

const (
	MsgType_BroadCast MsgType = "broadcast"
)

type ReqType struct {
	MsgType MsgType
	Client  *Client
	Data    string
}

type RespType struct {
	MsgType  MsgType
	SenderId string
	Data     string
}

func NewRespType(msg *ReqType) *RespType {
	return &RespType{
		MsgType:  msg.MsgType,
		SenderId: msg.Client.Id,
		Data:     msg.Data,
	}
}

type Server struct {
	clients         map[string]*Client
	mu              *sync.RWMutex
	joinServerChan  chan *Client
	leaveServerChan chan *Client
	broadcastChan   chan *ReqType
}

type Client struct {
	Id      string
	mu      *sync.RWMutex
	conn    *websocket.Conn
	msgChan chan *RespType
	done    chan struct{}
}

func NewClient(conn *websocket.Conn) *Client {
	Id := rand.Text()[:9]
	return &Client{
		Id:      Id,
		conn:    conn,
		mu:      new(sync.RWMutex),
		msgChan: make(chan *RespType, 64),
		done:    make(chan struct{}),
	}
}

func NewServer() *Server {
	return &Server{
		clients:         map[string]*Client{},
		mu:              new(sync.RWMutex),
		joinServerChan:  make(chan *Client, 64),
		leaveServerChan: make(chan *Client, 64),
		broadcastChan:   make(chan *ReqType, 64),
	}
}

func (srv *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  512,
		WriteBufferSize: 512,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("Error on HTTP conn upgrade %v\n", err.Error())
		return
	}

	client := NewClient(conn)

	// using srv.mu.Lock and UnLock is good for small number of clients to avoid race condition
	// if when for a large number of clients are there it will cause perfomace degradation
	// Thus by go's philosify "Dont communicate by sharing memory, share memory by communication" we are using channels to achive the same
	// srv.clients[client.Id] = client
	srv.joinServerChan <- client

	go client.writeMsgLoop()
	go client.readMsgLoop(srv)
}

func (cl *Client) writeMsgLoop() {
	defer cl.conn.Close()
	for {
		select {
		case <-cl.done:
			return
		case msg := <-cl.msgChan:
			err := cl.conn.WriteJSON(msg)
			if err != nil {
				fmt.Println("Error in broadcasting msg to clientID: ", cl.Id)
				continue
			}
		}
	}
}

func (cl *Client) readMsgLoop(srv *Server) {
	defer func() {
		close(cl.done)
		srv.leaveServerChan <- cl
	}()

	for {
		_, b, err := cl.conn.ReadMessage()
		if err != nil {
			return
		}

		msg := new(ReqType)
		err = json.Unmarshal(b, msg)
		if err != nil {
			fmt.Println("Error in unmarshalling data : ", err.Error())
			continue
		}
		msg.Client = cl
		srv.broadcastChan <- msg
	}
}

func (srv *Server) AcceptLoop() {
	for {
		select {
		case c := <-srv.joinServerChan:
			srv.joinServer(c)
		case c := <-srv.leaveServerChan:
			srv.leaveServer(c)
		case msg := <-srv.broadcastChan:
			cl := map[string]*Client{}
			for id, c := range srv.clients {
				if c.Id != msg.Client.Id {
					cl[id] = c
				}
			}
			go srv.broadcastMsg(msg, cl)
		}
	}
}

func (srv *Server) joinServer(client *Client) {
	srv.clients[client.Id] = client
	fmt.Printf("Client joining the server, CId: %s\n", client.Id)
}

func (srv *Server) leaveServer(client *Client) {
	delete(srv.clients, client.Id)
	fmt.Printf("Client left the server, CId: %s\n", client.Id)
}

func (srv *Server) broadcastMsg(msg *ReqType, cl map[string]*Client) {
	resp := NewRespType(msg)
	for _, c := range cl {
		c.msgChan <- resp
	}

	fmt.Println("Broadcast completed")
}

func CreateServer() {
	srv := NewServer()
	go srv.AcceptLoop()

	http.HandleFunc("/", srv.handleWS)

	fmt.Printf("Running http server in port: %v\n", WSPort)
	err := http.ListenAndServe(WSPort, nil)
	if err != nil {
		log.Fatalf("Error occured in http server: %v\n", err.Error())
	}
}

// TODO's
// [x] Create a HTTP server
// [x] Upgrade it to Web Socket server
// [x] Add newly connected Web Socker to the server
// [x] Add Web Socket Client
// [x] Remove the connection when the client disconnects the server
// [x] send broadcast message -> with no race condition happeing
func main() {
	CreateServer()
}
