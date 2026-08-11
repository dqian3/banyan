package main

import (
	"encoding/gob"
	"net"
	"sync"
	"time"

	"banyan/message"
)

// pipelinedConn is one long-lived connection to a node, carrying many
// requests. Sends are serialized through a single writer goroutine; replies
// arrive out of order on a reader goroutine and are matched back to their send
// time by request id.
//
// This is what keeps connection count proportional to the number of nodes
// rather than to outstanding requests. With one connection per in-flight
// request (the HTTP path), a client at rate R and latency L holds R x L
// connections — 5,000 of them at 50k req/s and 100ms — and starts measuring
// its own transport instead of the protocol.
type pipelinedConn struct {
	target string
	conn   net.Conn
	enc    *gob.Encoder
	dec    *gob.Decoder

	sendMu sync.Mutex

	mu      sync.Mutex
	pending map[string]time.Time

	onReply func(id string, latency time.Duration, ok bool)
	closed  chan struct{}
}

func dialPipelined(target string, onReply func(string, time.Duration, bool)) (*pipelinedConn, error) {
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
		conn:    conn,
		enc:     gob.NewEncoder(conn),
		dec:     gob.NewDecoder(conn),
		pending: make(map[string]time.Time),
		onReply: onReply,
		closed:  make(chan struct{}),
	}
	go p.readLoop()
	return p, nil
}

func (p *pipelinedConn) send(id string, payload []byte) error {
	p.mu.Lock()
	p.pending[id] = time.Now()
	p.mu.Unlock()

	p.sendMu.Lock()
	err := p.enc.Encode(&message.ClientRequest{ID: id, Payload: payload})
	p.sendMu.Unlock()
	if err != nil {
		p.mu.Lock()
		delete(p.pending, id)
		p.mu.Unlock()
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
		p.mu.Lock()
		sent, ok := p.pending[reply.ID]
		if ok {
			delete(p.pending, reply.ID)
		}
		p.mu.Unlock()
		if !ok {
			continue // duplicate or already timed out
		}
		p.onReply(reply.ID, time.Since(sent), reply.Err == "")
	}
}

// expire fails requests older than `timeout`, so a lost reply frees its
// in-flight slot instead of stalling the client forever. Returns how many.
func (p *pipelinedConn) expire(timeout time.Duration) int {
	cutoff := time.Now().Add(-timeout)
	var stale []string
	p.mu.Lock()
	for id, sent := range p.pending {
		if sent.Before(cutoff) {
			stale = append(stale, id)
		}
	}
	for _, id := range stale {
		delete(p.pending, id)
	}
	p.mu.Unlock()
	for _, id := range stale {
		p.onReply(id, timeout, false)
	}
	return len(stale)
}

func (p *pipelinedConn) close() { _ = p.conn.Close() }
