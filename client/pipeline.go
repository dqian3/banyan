package main

import (
	"encoding/gob"
	"net"
	"sync"

	"banyan/crypto"
	"banyan/message"
)

// pipelinedConn is one long-lived connection to a node, carrying many
// requests. Sends are serialized through a single writer goroutine; replies
// arrive out of order on a reader goroutine and are matched by request id in
// a tracker shared by every node's connection, since a request is answered by
// more than the node it was sent to.
//
// This is what keeps connection count proportional to the number of nodes
// rather than to outstanding requests. With one connection per in-flight
// request (the HTTP path), a client at rate R and latency L holds R x L
// connections — 5,000 of them at 50k req/s and 100ms — and starts measuring
// its own transport instead of the protocol.
type pipelinedConn struct {
	target string
	node   int // index into the client's target list
	conn   net.Conn
	enc    *gob.Encoder
	dec    *gob.Decoder

	sendMu sync.Mutex

	replies *replyTracker
	closed  chan struct{}
}

func dialPipelined(target string, node int, clientID uint32, replies *replyTracker) (*pipelinedConn, error) {
	conn, err := net.Dial("tcp", target)
	if err != nil {
		return nil, err
	}
	// Requests are small and latency-sensitive; Nagle would batch them into
	// the next round trip and show up as client-side latency.
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	p := &pipelinedConn{
		target:  target,
		node:    node,
		conn:    conn,
		enc:     gob.NewEncoder(conn),
		dec:     gob.NewDecoder(conn),
		replies: replies,
		closed:  make(chan struct{}),
	}
	// Hello: a request with no id tells the node which client this connection
	// belongs to, so it can reply for requests other nodes received.
	if err := p.enc.Encode(&message.ClientRequest{ClientID: clientID}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	go p.readLoop()
	return p, nil
}

func (p *pipelinedConn) send(id string, payload []byte, clientID uint32, sig crypto.Signature) error {
	p.replies.add(id)

	p.sendMu.Lock()
	err := p.enc.Encode(&message.ClientRequest{
		ID:       id,
		Payload:  payload,
		ClientID: clientID,
		Sig:      sig,
	})
	p.sendMu.Unlock()
	if err != nil {
		p.replies.drop(id)
	}
	return err
}

func (p *pipelinedConn) readLoop() {
	for {
		var reply message.RequestReply
		if err := p.dec.Decode(&reply); err != nil {
			close(p.closed)
			return
		}
		p.replies.reply(reply.ID, p.node, reply.Err)
	}
}

func (p *pipelinedConn) close() { _ = p.conn.Close() }
