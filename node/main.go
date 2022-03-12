// Uses Lamport's logical clocks to implement a totally-ordered multicast of bank account transactions under the
// following assumptions:
// 1. No messages are lost
// 2. Messages from the same sender are received in the same order as they were sent.

package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/hashicorp/consul/api"
	bank "github.com/nikolasnorth/lamport-clocks/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
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

// Operation represents a bank operation to be performed.
type Operation struct {
	Name          string
	Amount        float32
	AccountNumber int64
}

type Timestamp struct {
	node  int64
	event int64
}

// =====================================================================================================================
// Lamport Clock
// =====================================================================================================================

type LamportClock struct {
	LatestTime int64
}

// NewLamportClock returns a new instance of LamportClock with time set to 0.
func NewLamportClock() LamportClock {
	return LamportClock{LatestTime: 0}
}

// Tick increments the latest time
func (lc *LamportClock) Tick(requestTime int64) int64 {
	lc.LatestTime = maxTime(lc.LatestTime, requestTime) + 1
	return lc.LatestTime
}

// maxTime returns the max of the two given times.
func maxTime(t1, t2 int64) int64 {
	if t1 < t2 {
		return t2
	}
	return t1
}

// =====================================================================================================================
// Message Priority Queue
// =====================================================================================================================

type Message struct {
	t  Timestamp
	op Operation
}

type MessageQueue struct {
	storage []Message
	mutex   sync.Mutex
}

// NewMessageQueue constructs a new empty queue.
func NewMessageQueue() MessageQueue {
	return MessageQueue{
		storage: make([]Message, 0),
	}
}

// Push inserts the given message in the correct order.
func (q *MessageQueue) Push(op Operation, t Timestamp) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

}

// Pop returns message, isEmpty. Removes and returns the message with the highest priority, with isEmpty set to false.
// Otherwise, if queue is empty, isEmpty is true.
func (q *MessageQueue) Pop() (Operation, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if len(q.storage) == 0 {
		return Operation{}, true
	}

	op := q.storage[0].op
	q.storage = q.storage[1:]
	return op, false
}

// =====================================================================================================================
// gRPC Bank Server
// =====================================================================================================================

type BankServer struct {
	bank.UnimplementedBankServer
}

func (b *BankServer) Deposit(ctx context.Context, req *bank.Request) (*bank.Response, error) {
	op := Operation{Name: deposit, Amount: req.GetAmount(), AccountNumber: req.GetAccountNumber()}
	t := clock.Tick(req.GetTimestamp().GetEvent())
	//queue.Push(op, t)
	fmt.Println("received deposit request for", op.Amount, "to account", op.AccountNumber, ", t =", t)
	return &bank.Response{}, nil
}

func (b *BankServer) Withdraw(ctx context.Context, req *bank.Request) (*bank.Response, error) {
	op := Operation{Name: withdraw, Amount: req.GetAmount(), AccountNumber: req.GetAccountNumber()}
	t := clock.Tick(req.GetTimestamp().GetEvent())
	//queue.Push(op, t)
	fmt.Println("received withdraw request for", op.Amount, "to account", op.AccountNumber, ", t =", t)
	return &bank.Response{}, nil
}

func (b *BankServer) AddInterest(ctx context.Context, req *bank.Request) (*bank.Response, error) {
	op := Operation{Name: interest, Amount: req.GetAmount(), AccountNumber: req.GetAccountNumber()}
	t := clock.Tick(req.GetTimestamp().GetEvent())
	//queue.Push(op, t)
	fmt.Println("received withdraw request for", op.Amount, "to account", op.AccountNumber, ", t =", t)
	return &bank.Response{}, nil
}

func (b *BankServer) Done(ctx context.Context, req *bank.DoneRequest) (*bank.Response, error) {
	//op := Operation{Name: done, Amount: -1, AccountNumber: -1}
	t := clock.Tick(req.GetTimestamp().GetEvent())

	fmt.Println("received done request, t =", t)
	//// Process initial account balances
	//accountsFile, err := os.Open(accountsFilename)
	//if err != nil {
	//	log.Fatalf("failed to open file: %v", err)
	//}
	//defer accountsFile.Close()
	//
	//bytes, err := io.ReadAll(accountsFile)
	//if err != nil {
	//	log.Fatalf("failed to read from file: %v", err)
	//}
	//
	//accounts := make([]Account, 0)
	//err = json.Unmarshal(bytes, &accounts)
	//if err != nil {
	//	log.Fatalf("failed to unmarshal data into slice of accounts: %v", err)
	//}
	//
	//op, isEmpty := queue.Pop()
	//for !isEmpty {
	//	switch op.Name {
	//	case deposit:
	//		for _, account := range accounts {
	//			if *account.AccountID == op.AccountNumber {
	//				*account.Balance += op.Amount
	//			}
	//		}
	//	case withdraw:
	//		for _, account := range accounts {
	//			if *account.AccountID == op.AccountNumber {
	//				*account.Balance -= op.Amount
	//			}
	//		}
	//	case interest:
	//		for _, account := range accounts {
	//			if *account.AccountID == op.AccountNumber {
	//				*account.Balance += *account.Balance * (op.Amount / 100)
	//			}
	//		}
	//	default:
	//		log.Fatalf("invalid operation: %s\n", op.Name)
	//	}
	//	op, isEmpty = queue.Pop()
	//}

	return &bank.Response{}, nil
}

// =====================================================================================================================
// Peer-to-peer Node
// =====================================================================================================================

type Node struct {
	ID      int64
	Name    string
	Addr    string
	SDAddr  string
	SDKV    api.KV
	Clients map[string]bank.BankClient
}

func (n *Node) sendRequestToNode(name, operation string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Split current `line` into operation parameters
	splitOperation := strings.Split(operation, " ")

	if len(splitOperation) == 1 && splitOperation[0] == done {
		ts := &bank.Timestamp{Node: n.ID, Event: clock.Tick(-1)}
		req := &bank.DoneRequest{Timestamp: ts}

		_, err := n.Clients[name].Done(ctx, req)
		if err != nil {
			log.Fatalf("failed to send request: %v", err)
		}
		return
	}
	if len(splitOperation) != 3 {
		log.Fatalln("invalid input: expected [operation] [num1] [num2]")
	}

	// Parse operation parameters for current `line`
	opName, accountNumberStr, amountStr := splitOperation[0], splitOperation[1], splitOperation[2]
	accountNumber, err := strconv.ParseInt(accountNumberStr, 10, 64)
	if err != nil {
		log.Fatalf("failed to convert string to int64: %v", err)
	}
	amount, err := strconv.ParseFloat(amountStr, 32)
	if err != nil {
		log.Fatalf("failed to convert string to float: %v", err)
	}

	// Send gRPC request to Node with given `name`
	op := Operation{Name: opName, Amount: float32(amount), AccountNumber: accountNumber}
	ts := &bank.Timestamp{Node: n.ID, Event: clock.Tick(-1)}
	req := &bank.Request{AccountNumber: accountNumber, Amount: float32(amount), Timestamp: ts}
	switch op.Name {
	case deposit:
		_, err := n.Clients[name].Deposit(ctx, req)
		if err != nil {
			log.Fatalf("failed to request deposit: %v", err)
		}
		//queue.Push(op, t)
	case withdraw:
		_, err := n.Clients[name].Withdraw(ctx, req)
		if err != nil {
			log.Fatalf("failed to request withdraw: %v", err)
		}
		//queue.Push(op, t)
	case interest:
		_, err := n.Clients[name].AddInterest(ctx, req)
		if err != nil {
			log.Fatalf("failed to request interest addition: %v", err)
		}
		//queue.Push(op, t)
	default:
		log.Fatalln("invalid operation:", op)
	}
}

func (n *Node) sendRequestsToNode(name string) {
	operationsFile, err := os.Open(operationsFilename)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer operationsFile.Close()

	// Make a separate RPC request for each operation in the input file
	scanner := bufio.NewScanner(operationsFile)
	for scanner.Scan() {
		operation := scanner.Text()
		fmt.Println("sending request to node:", operation)
		n.sendRequestToNode(name, operation)
	}
}

func (n *Node) connectToNode(name, addr string) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	n.Clients[name] = bank.NewBankClient(conn)
	n.sendRequestsToNode(name)
}

// Sends a message to all nodes at most once.
func (n *Node) checkForNewNodes() {
	kvPairs, _, err := n.SDKV.List("nnorth", nil)
	if err != nil {
		log.Fatalf("failed to retrieve list of all nodes: %v", err)
	}

	for _, pair := range kvPairs {
		if pair.Key == n.Name {
			// Itself
			continue
		}
		if n.Clients[pair.Key] == nil {
			// New node was found, establish connection and send RPC requests for all operations
			fmt.Println("found node: ", pair.Key)
			n.connectToNode(pair.Key, string(pair.Value))
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
		n.checkForNewNodes()
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

	splitName := strings.Split(name, " ")
	if len(splitName) != 2 {
		log.Fatalln("expected node name: [name] [number]")
	}

	id, err := strconv.ParseInt(splitName[1], 10, 64)
	if err != nil {
		log.Fatalf("failed to convert node name number: %v", err)
	}

	queue = NewMessageQueue()
	clock = NewLamportClock()

	n := Node{
		ID:      id,
		Name:    name,
		Addr:    addr,
		SDAddr:  sdAddr,
		SDKV:    api.KV{},
		Clients: make(map[string]bank.BankClient),
	}
	n.start()
}
