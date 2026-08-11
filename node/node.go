package node

import (
	"net/http"
	"reflect"
	"sync"

	"banyan/config"
	"banyan/identity"
	"banyan/log"
	"banyan/socket"
)

// Node is the primary access point for every replica
// it includes networking, state machine and RESTful API server
type Node interface {
	socket.Socket
	//Database
	ID() identity.NodeID
	Run()
	Register(m interface{}, f interface{})
	IsByz() bool
}

// node implements Node interface
type node struct {
	id identity.NodeID

	socket.Socket
	//Database
	MessageChan chan interface{}
	TxChan      chan interface{}
	handles     map[string]reflect.Value
	server      *http.Server
	isByz       bool

	sync.RWMutex
}

// NewNode creates a new Node object from configuration
func NewNode(id identity.NodeID, isByz bool) Node {
	return &node{
		id:     id,
		isByz:  isByz,
		Socket: socket.NewSocket(id, config.Configuration.Addrs, isByz),
		//Database:    NewDatabase(),
		MessageChan: make(chan interface{}, 1024),
		TxChan:      make(chan interface{}, 1024),
		handles:     make(map[string]reflect.Value),
	}
}

func (n *node) ID() identity.NodeID {
	return n.id
}

func (n *node) IsByz() bool {
	return n.isByz
}

// Register a handle function for each message type
func (n *node) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	fn := reflect.ValueOf(f)

	if fn.Kind() != reflect.Func {
		panic("handle function is not func")
	}

	if fn.Type().In(0) != t {
		panic("func type is not t")
	}

	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 1 || fn.Type().In(0) != t {
		panic("register handle function error")
	}
	n.handles[t.String()] = fn
}

// Run start and run the node
func (n *node) Run() {
	log.Infof("node %v start running", n.id)
	if len(n.handles) > 0 {
		go n.handle()
		go n.recv()
		go n.txn()
		// Pipelined client transport; the HTTP server below still serves
		// /query and the one-request-per-connection /request endpoint.
		go n.clientListener()
	}
	n.http()
}

func (n *node) txn() {
	for {
		tx := <-n.TxChan
		v := reflect.ValueOf(tx)
		name := v.Type().String()
		f, exists := n.handles[name]
		if !exists {
			// Log and drop rather than exit: this channel carries messages
			// that arrive from outside (client requests over HTTP), and a
			// protocol that does not register a handler for one of them must
			// not be killable by sending it. Naming what *is* registered
			// turns "the node died" into an obvious wiring diagnosis.
			registered := make([]string, 0, len(n.handles))
			for k := range n.handles {
				registered = append(registered, k)
			}
			log.Errorf("no registered handle function for message type %v (registered: %v)", name, registered)
			continue
		}
		f.Call([]reflect.Value{v})
	}
}

// recv receives messages from socket and pass to message channel
func (n *node) recv() {
	for {
		m := n.Recv()
		n.MessageChan <- m
	}
}

// handle receives messages from message channel and calls handle function using refection
func (n *node) handle() {
	for {
		msg := <-n.MessageChan
		v := reflect.ValueOf(msg)
		name := v.Type().String()
		f, exists := n.handles[name]
		if !exists {
			log.Fatalf("no registered handle function for message type %v", name)
		}
		f.Call([]reflect.Value{v})
	}
}

/*
func (n *node) Forward(id NodeID, m Transaction) {
	key := m.Command.Key
	url := config.HTTPAddrs[id] + "/" + strconv.Itoa(int(key))

	log.Debugf("Node %v forwarding %v to %s", n.NodeID(), m, id)

	method := http.MethodGet
	var body io.Reader
	if !m.Command.IsRead() {
		method = http.MethodPut
		body = bytes.NewBuffer(m.Command.Value)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Error(err)
		return
	}
	req.Header.Set(HTTPClientID, string(n.id))
	req.Header.Set(HTTPCommandID, strconv.Itoa(m.Command.CommandID))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error(err)
		m.TransactionReply(TransactionReply{
			Command: m.Command,
			Err:     err,
		})
		return
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		b, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Error(err)
		}
		m.TransactionReply(TransactionReply{
			Command: m.Command,
			Value:   Value(b),
		})
	} else {
		m.TransactionReply(TransactionReply{
			Command: m.Command,
			Err:     errors.New(res.Status),
		})
	}
}
*/
