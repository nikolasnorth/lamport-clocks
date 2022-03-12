// Uses Lamport's logical clocks to implement a totally-ordered multicast of bank account transactions under the
// following assumptions:
// 1. No messages are lost
// 2. Messages from the same sender are received in the same order as they were sent.

package main

import (
	"bufio"
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
	"strconv"
	"strings"
	"time"
)

const accountsFilename = "accounts.json"
const operationsFilename = "operations.txt"

const deposit = "deposit"
const withdraw = "withdraw"
const interest = "interest"
const done = "done"

var queue MessageQueue
var clock LamportClock

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
	op := Operation{Name: deposit, Amount: req.Amount, From: req.}
	t := clock.Tick(req.Timestamp)
	queue.Push(op, t)
	return &bank.Response{}, nil
}

func (b *BankServer) Withdraw(ctx context.Context, req *bank.Request) (*bank.Response, error) {
	op := Operation{Name: withdraw, Amount: req.Amount}
	t := clock.Tick(req.Timestamp)
	queue.Push(op, t)
	return &bank.Response{}, nil
}

func (b *BankServer) AddInterest(ctx context.Context, req *bank.Request) (*bank.Response, error) {
	op := Operation{Name: interest, Amount: req.Amount}
	t := clock.Tick(req.Timestamp)
	queue.Push(op, t)
	return &bank.Response{}, nil
}

func (b *BankServer) Done(ctx context.Context, req *bank.DoneRequest) (*bank.Response, error) {
	// Process initial account balances
	accountsFile, err := os.Open(accountsFilename)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer accountsFile.Close()

	bytes, err := io.ReadAll(accountsFile)
	if err != nil {
		log.Fatalf("failed to read from file: %v", err)
	}

	accounts := make([]Account, 0)
	err = json.Unmarshal(bytes, &accounts)
	if err != nil {
		log.Fatalf("failed to unmarshal data into slice of accounts: %v", err)
	}

	op, isEmpty := queue.Pop()
	for !isEmpty {
		switch op.Name {
		case deposit:
			for _, account := range accounts {
				if *account.AccountID == op.AccountNumber {
					*account.Balance += op.Amount
				}
			}
		case withdraw:
			for _, account := range accounts {
				if *account.AccountID == op.AccountNumber {
					*account.Balance -= op.Amount
				}
			}
		case interest:
			for _, account := range accounts {
				if *account.AccountID == op.AccountNumber {
					*account.Balance += *account.Balance * (op.Amount / 100)
				}
			}
		default:
			log.Fatalf("invalid operation: %s\n", op.Name)
		}
		op, isEmpty = queue.Pop()
	}

	// Write output file
	return &bank.Response{}, nil
}

// Perform gRPC connections containing operations to be performed to all other nodes.
func (n *Node) sendRequestsToNode(name, addr string) {
	connection, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer connection.Close()

	n.Clients[name] = bank.NewBankClient(connection)

	operationsFile, err := os.Open(operationsFilename)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer operationsFile.Close()

	scanner := bufio.NewScanner(operationsFile)
	for scanner.Scan() {
		//

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		line := scanner.Text()
		splitLine := strings.Split(line, " ")
		if len(splitLine) == 1 && splitLine[0] == done {
			req := &bank.DoneRequest{}
			_, err := n.Clients[name].Done(ctx, req)
			if err != nil {
				log.Fatalf("failed to send done request: %v", err)
			}
			break
		}
		if len(splitLine) != 3 {
			log.Fatalln("invalid input: expected [operation] [num1] [num2]")
		}

		opName, accountNumberStr, amountStr := splitLine[0], splitLine[1], splitLine[2]
		accountNumber, err := strconv.ParseInt(accountNumberStr, 10, 64)
		if err != nil {
			log.Fatalf("failed to convert string to int64: %v", err)
		}
		amount, err := strconv.ParseFloat(amountStr, 32)
		if err != nil {
			log.Fatalf("failed to convert string to float: %v", err)
		}

		req := &bank.Request{AccountNumber: accountNumber, Amount: float32(amount), Timestamp: clock.LatestTime}
		op := Operation{Name: opName, Amount: float32(amount)}
		switch op.Name {
		case deposit:
			_, err := n.Clients[name].Deposit(ctx, req)
			if err != nil {
				log.Fatalf("failed to request deposit: %v", err)
			}
		case withdraw:
			_, err := n.Clients[name].Withdraw(ctx, req)
			if err != nil {
				log.Fatalf("failed to request withdraw: %v", err)
			}
		case interest:
			_, err := n.Clients[name].AddInterest(ctx, req)
			if err != nil {
				log.Fatalf("failed to request interest addition: %v", err)
			}
		default:
			log.Fatalln("invalid operation:", op)
		}

		cancel()  // context
	}
}

// Sends a message to all nodes at most once.
func (n *Node) messageAll() {
	kvPairs, _, err := n.SDKV.List("nnorth", nil)
	if err != nil {
		log.Fatalf("failed to retrieve list of all nodes: %v", err)
	}

	fmt.Println("KV Pairs:", kvPairs)
	for _, pair := range kvPairs {
		if pair.Key == n.Name {
			continue
		}
		if n.Clients[pair.Key] == nil {
			// Found new node. Send requests from operations file
			fmt.Println("new node found: ", pair.Key)
			//n.sendRequestsToNode(pair.Key, string(pair.Value))
		}
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
	log.Println("successfully registered with service discovery.")
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

// Start node
func (n *Node) start() {
	go n.listen()
	fmt.Println("listening on", n.Addr, "...")

	n.registerService()

	for {
		time.Sleep(20 * time.Second)
		n.messageAll()
	}
}

func main() {
	args := os.Args[1:]
	if len(args) != 3 {
		log.Fatalln("Arguments required: [node name] [:port number] localhost:8500 [operations filename]")
	}
	name := args[0]
	addr := "localhost" + args[1]
	sdAddr := args[2]

	queue = NewMessageQueue()
	clock = NewLamportClock()

	n := Node{
		Name:    name,
		Addr:    addr,
		SDAddr:  sdAddr,
		SDKV:    api.KV{},
		Clients: make(map[string]bank.BankClient),
	}
	n.start()
}
