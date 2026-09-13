package node

import (
	"encoding/gob"
	"io"
	"net"
	"strconv"

	"banyan/config"
	"banyan/identity"
	"banyan/log"
	"banyan/message"
)

// ClientPortBase is the offset for the per-node client port: node i listens on
// ClientPortBase + i, alongside its peer port (3734+i) and HTTP port (8069+i).
const ClientPortBase = 5000

// ClientPort returns the port node `id` accepts client connections on.
func ClientPort(id identity.NodeID) int {
	return ClientPortBase + id.Node()
}

// clientListener accepts pipelined client connections.
//
// The HTTP endpoint (/request) holds a connection open per outstanding
// request, so a client's connection count grows with rate x latency — 50k
// req/s at 100ms is 5,000 live connections per client, and each one costs a
// server goroutine and its buffers. That is a property of the transport, not
// of the protocol under test, and at high offered load the client saturates
// before banyan does.
//
// Here one connection carries many requests: a reader decodes requests as they
// stream in and a writer sends each reply back as its block commits, matched by
// request id. Connections then scale with the number of nodes rather than with
// in-flight requests. The HTTP path stays for one-off probes and debugging.
func (n *node) clientListener() {
	port := ClientPort(n.id)
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		// Fatal, not a logged return. A node that keeps running without this
		// listener still joins consensus and still commits blocks, so it
		// reports a healthy-looking /query while every client request to it
		// fails -- a run whose real cause is a stale process holding the port
		// is then indistinguishable from the protocol collapsing under load.
		// Dying here makes the harness's own "node exited" path report it.
		log.Fatalf("client listener on :%d failed: %v", port, err)
	}
	log.Info("client listener starting on :", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Error("client accept error: ", err)
			continue
		}
		go n.serveClientConn(conn)
	}
}

// serveClientConn runs one client connection: decode requests forever, and
// write replies as they are produced.
func (n *node) serveClientConn(conn net.Conn) {
	defer conn.Close()

	// Buffered so a commit never blocks on a slow or departed client; when it
	// fills, Request.Reply drops rather than waiting, and the client sees the
	// request time out. Sized to match a generous in-flight window.
	replies := make(chan message.RequestReply, 4096)
	done := make(chan struct{})

	go func() {
		enc := gob.NewEncoder(conn)
		for {
			select {
			case reply := <-replies:
				if err := enc.Encode(&reply); err != nil {
					// Client hung up; the reader will notice too.
					return
				}
			case <-done:
				return
			}
		}
	}()
	defer close(done)

	clientDriven := config.GetConfig().IsClientDriven()
	dec := gob.NewDecoder(conn)
	registered := false
	for {
		var wire message.ClientRequest
		if err := dec.Decode(&wire); err != nil {
			if err != io.EOF {
				log.Debugf("client connection closed: %v", err)
			}
			return
		}
		// An empty id is the client's hello: it names the client on this
		// connection, so this node can reply for requests another node
		// received.
		if wire.ID == "" {
			if !registered {
				n.registerClient(wire.ClientID, replies)
				defer n.unregisterClient(wire.ClientID, replies)
				registered = true
			}
			continue
		}
		if !clientDriven {
			replies <- message.RequestReply{
				ID: wire.ID, Err: "node is not running the client workload",
			}
			continue
		}
		req := message.Request{
			ID:       wire.ID,
			Payload:  wire.Payload,
			ClientID: wire.ClientID,
			Sig:      wire.Sig,
			C:        replies,
		}
		n.TxChan <- req
	}
}

func (n *node) registerClient(id uint32, replies chan message.RequestReply) {
	n.clientsMu.Lock()
	n.clients[id] = replies
	n.clientsMu.Unlock()
}

// unregisterClient forgets a closed connection, unless the client has already
// reconnected and registered a newer one.
func (n *node) unregisterClient(id uint32, replies chan message.RequestReply) {
	n.clientsMu.Lock()
	if n.clients[id] == replies {
		delete(n.clients, id)
	}
	n.clientsMu.Unlock()
}

// ReplyTo queues a reply on a client's connection. It drops the reply when
// the client has not said hello here or its buffer is full, which the client
// sees as a timeout.
func (n *node) ReplyTo(clientID uint32, reply message.RequestReply) bool {
	n.clientsMu.RLock()
	replies, ok := n.clients[clientID]
	n.clientsMu.RUnlock()
	if !ok {
		return false
	}
	select {
	case replies <- reply:
		return true
	default:
		return false
	}
}
