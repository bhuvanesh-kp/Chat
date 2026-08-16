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
	Id   string
	mu   *sync.RWMutex
	conn *websocket.Conn
}

func NewClient(conn *websocket.Conn) *Client {
	Id := rand.Text()[:9]
	return &Client{
		Id:   Id,
		conn: conn,
		mu:   new(sync.RWMutex),
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

	go client.readMsgLoop(srv)
}

func (cl *Client) readMsgLoop(srv *Server) {
	defer func() {
		cl.conn.Close()
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
			go srv.broadcastMsg(msg)
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

func (srv *Server) broadcastMsg(msg *ReqType) {
	cl := []*Client{}

	srv.mu.RLock()
	for _, c := range srv.clients {
		if c.Id != msg.Client.Id {
			cl = append(cl, c)
		}
	}
	srv.mu.RUnlock()

	resp := NewRespType(msg)
	for _, c := range cl {
		err := c.conn.WriteJSON(resp)
		if err != nil {
			fmt.Println("Error in broadcasting msg to clientID: ", c.Id)
			continue
		}
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
