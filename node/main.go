// Uses Lamport's logical clocks to implement a totally-ordered multicast of bank account transactions under the
// following assumptions:
// 1. No messages are lost
// 2. Messages from the same sender are received in the same order as they were sent.

package main

import (
	"context"
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

func (b *BankServer) Deposit(ctx context.Context, req *bank.Request) (*bank.Response, error) {
	return &bank.Response{}, nil
}

func (b *BankServer) Withdraw(ctx context.Context, req *bank.Request) (*bank.Response, error) {
	return &bank.Response{}, nil
}

func (b *BankServer) AddInterest(ctx context.Context, req *bank.Request) (*bank.Response, error) {
	return &bank.Response{}, nil
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
}

func (n *Node) greetAll() {
	kvPairs, _, err := n.SDKV.List("nnorth2", nil)
	if err != nil {
		log.Fatalf("failed to retrieve list of all nodes: %v", err)
	}

	for _, pair := range kvPairs {
		fmt.Println("Here")
		if pair.Key == n.Name {
			continue
		}
		if n.Clients[pair.Key] == nil {
			fmt.Println("new member: ", pair.Key)
			n.setupClient(pair.Key, string(pair.Value))
		}
	}
}

func (n *Node) start() {
	go n.listen()
	fmt.Println("listening on", n.Addr, "...")

	n.registerService()

	for {
		time.Sleep(20 * time.Second)
		fmt.Println("Calling greetAll()...")
		n.greetAll()
	}
}

func main() {
	args := os.Args[1:]
	if len(args) != 3 {
		log.Fatalln("Arguments required: [node name] [:port number] localhost:8500 [operations filename]")
	}

	name := args[0]
	port := args[1]
	addr := "localhost" + port
	sdAddr := args[2]

	accountsFile, err := os.Open(accountsFilename)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer accountsFile.Close()

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
