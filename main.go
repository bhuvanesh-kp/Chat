package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var (
	WSPort = ":3245"
)

type Server struct {
	clients []*Client
	mu      *sync.RWMutex
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
		clients: []*Client{},
		mu:      new(sync.RWMutex),
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
	srv.clients = append(srv.clients, client)
}

// TODO's
// [x] Create a HTTP server
// [x] Upgrade it to Web Socket server
// [x] Add newly connected Web Socker to the server
// [] Add Web Socket Client
// [] Remove the connection when the client disconnects the server
// [] send broadcast message -> with no race condition happeing
func main() {
	srv := NewServer()

	http.HandleFunc("/", srv.handleWS)

	fmt.Printf("Running http server in port: %v", WSPort)
	err := http.ListenAndServe(WSPort, nil)
	if err != nil {
		log.Fatalf("Error occured in http server: %v", err.Error())
	}
}
