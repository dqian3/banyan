package socket

import (
	"sync"
	"time"

	"banyan/identity"
	"banyan/log"
	"banyan/transport"
	"banyan/utils"
)

// Socket integrates all networking interface and fault injections
type Socket interface {

	// Send put message to outbound queue
	Send(to identity.NodeID, m interface{})

	// Broadcast send to all peers
	Broadcast(m interface{})

	// Recv receives a message
	Recv() interface{}

	Close()
}

type socket struct {
	silence   bool
	id        identity.NodeID
	addresses map[identity.NodeID]string
	nodes     map[identity.NodeID]transport.Transport

	lock sync.RWMutex // locking map nodes
}

// Dial budget for a peer that is not accepting yet: ~47s with Retry's capped
// backoff. A peer still absent after that is logged and left disconnected;
// the next send to it starts a fresh attempt, so connectivity self-heals.
const (
	dialAttempts = 100
	dialInterval = 50 * time.Millisecond
)

// NewSocket return Socket interface instance given self NodeID, node list, transport and codec name
func NewSocket(id identity.NodeID, addrs map[identity.NodeID]string, silence bool) Socket {
	socket := &socket{
		silence:   silence,
		id:        id,
		addresses: addrs,
		nodes:     make(map[identity.NodeID]transport.Transport),
	}

	socket.nodes[id] = transport.NewTransport(addrs[id])
	socket.nodes[id].Listen()

	return socket
}

func (s *socket) Send(to identity.NodeID, m interface{}) {
	//log.Debugf("node %s send message %+v to %v", s.id, m, to)

	s.lock.RLock()
	t, exists := s.nodes[to]
	s.lock.RUnlock()
	if !exists {
		t = s.connect(to)
		if t == nil {
			return
		}
	}

	if !s.silence {
		t.Send(m)
	}
}

func (s *socket) Recv() interface{} {
	s.lock.RLock()
	t := s.nodes[s.id]
	s.lock.RUnlock()
	for {
		m := t.Recv()
		return m
	}
}

func (s *socket) Broadcast(m interface{}) {
	//log.Debugf("node %s broadcasting message %+v", s.id, m)
	for id := range s.addresses {
		if id == s.id {
			continue
		}
		s.Send(id, m)
	}
	//log.Debugf("node %s done  broadcasting message %+v", s.id, m)
}

func (s *socket) Close() {
	for _, t := range s.nodes {
		t.Close()
	}
}

// connect returns the transport for `to`, creating it if needed and dialing
// in the background.
//
// The transport is published before the dial completes, which is the whole
// point: transport.Send only appends to a 10240-deep channel, and Dial's
// writer goroutine drains it once the connection is up. So a message sent to
// a peer that is still coming up is queued, not lost, and the caller is never
// blocked.
//
// It used to dial inline with a retry budget -- and with utils.Retry's
// uncapped backoff that budget was 247 seconds, on whichever goroutine sent
// first. The first send to each peer is a proposal broadcast, made on the
// replica's single event loop, so one peer that was not accepting yet froze
// the entire node: no proposals, no votes, no commits, nothing logged, while
// handleQuery kept answering "Committed blocks: 0" from the node's other
// goroutine. A node that looks alive and does nothing, for longer than the
// harness waits.
func (s *socket) connect(to identity.NodeID) transport.Transport {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Re-check: another sender may have created it while we were unlocked.
	if t, exists := s.nodes[to]; exists {
		return t
	}
	address, ok := s.addresses[to]
	if !ok {
		log.Errorf("socket does not have address of node %s", to)
		return nil
	}

	t := transport.NewTransport(address)
	s.nodes[to] = t
	go func() {
		if err := utils.Retry(t.Dial, dialAttempts, dialInterval); err != nil {
			// Say which peer, and stay up. A node that cannot reach one peer
			// is still useful to the committee, and dying here turns a
			// connectivity problem into a missing replica.
			log.Errorf("socket could not connect to node %s at %s: %v", to, address, err)
			// Forget it, so the next send to this peer starts a fresh
			// attempt rather than queueing into a channel nobody drains.
			s.lock.Lock()
			if cur, ok := s.nodes[to]; ok && cur == t {
				delete(s.nodes, to)
			}
			s.lock.Unlock()
			return
		}
		log.Infof("socket connected to node %s at %s", to, address)
	}()
	return t
}
