package main

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/consul/api"
	bank "github.com/nikolasnorth/lamport-clocks/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"io"
	"log"
	"net"
	"os"
	"time"
)

const accountsFilename = "accounts.json"

// Storage for all Account funds
var accounts []Account

type Account struct {
	Name      *string  `json:"Name"`
	AccountID *int64   `json:"AccountID"`
	Balance   *float32 `json:"Balance"`
}

type Node struct {
	Name    string
	Addr    string
	SDAddr  string
	SDKV    api.KV
	Clients map[string]bank.BankClient
}

type BankServer struct {
	bank.UnimplementedBankServer
}

// Start gRPC server
func (n *Node) listen() {
	listener, err := net.Listen("tcp", n.Addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	bank.RegisterBankServer(server, &BankServer{})
	reflection.Register(server)

	err = server.Serve(listener)
	if err != nil {
		log.Fatalf("failed to start server: %v", err)
	}

	fmt.Println("Listening on", n.Addr, "...")
}

// Register this node with the service discovery module.
func (n *Node) registerService() {
	config := api.DefaultConfig()
	config.Address = n.SDAddr

	consul, err := api.NewClient(config)
	if err != nil {
		log.Fatalf("failed to contact service discovery: %v", err)
	}

	kv := consul.KV()
	pair := &api.KVPair{Key: n.Name, Value: []byte(n.Addr)}

	_, err = kv.Put(pair, nil)
	if err != nil {
		log.Fatalf("failed to register with service discovery: %v", err)
	}

	n.SDKV = *kv
	log.Println("Successfully registered with service discovery.")
}

func (n *Node) setupClient(name, addr string) {
	connection, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer connection.Close()

	n.Clients[name] = bank.NewBankClient(connection)

	// TODO: Perform RPC call
}

func (n *Node) greetAll() {
	kvPairs, _, err := n.SDKV.List("nnorth2", nil)
	if err != nil {
		log.Fatalf("failed to retrieve list of all nodes: %v", err)
	}

	for _, pair := range kvPairs {
		if pair.Key == n.Name {
			continue
		}
		if n.Clients[pair.Key] == nil {
			fmt.Println("New member: ", pair.Key)
			n.setupClient(pair.Key, string(pair.Value))
		}
	}
}

func (n *Node) start() {
	go n.listen()

	n.registerService()

	for {
		time.Sleep(20 * time.Second)
		n.greetAll()
	}
}

func main() {
	fmt.Println("Hi")
	args := os.Args[1:]
	if len(args) != 3 {
		log.Fatalln("Arguments required: [port number] [filename] [node name]")
	}
	name := args[0]
	port := args[1]
	addr := "localhost" + port
	sdAddr := args[2]

	accountsFile, err := os.Open(accountsFilename)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}

	bytes, err := io.ReadAll(accountsFile)
	if err != nil {
		log.Fatalf("failed to read from file: %v", err)
	}

	err = json.Unmarshal(bytes, &accounts)
	if err != nil {
		log.Fatalf("failed to unmarshal data into slice of accounts: %v", err)
	}

	n := Node{
		Name:    name,
		Addr:    addr,
		SDAddr:  sdAddr,
		SDKV:    api.KV{},
		Clients: make(map[string]bank.BankClient),
	}
	n.start()
}
