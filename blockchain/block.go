package blockchain

import (
	"banyan/crypto"
	"banyan/identity"
	"io"
	"math/rand"
	"time"
)

type Block struct {
	Height    int
	Rank      int
	Proposer  identity.NodeID
	Timestamp time.Time
	Payload   []byte
	PrevID    crypto.Identifier
	Sig       crypto.Signature
	ID        crypto.Identifier
	Ts        time.Duration
}

type rawBlock struct {
	Height      int
	Rank        int
	Proposer    identity.NodeID
	PayloadHash crypto.Identifier
	PrevID      crypto.Identifier
	Sig         crypto.Signature
	ID          crypto.Identifier
}

// MakeBlock creates an unsigned block carrying `blockByteSize` random bytes.
// This is the generated workload: no clients, load set by block size alone.
func MakeBlock(height int, rank int, prevID crypto.Identifier, proposer identity.NodeID, blockByteSize int, r *rand.Rand) *Block {
	return MakeBlockWithPayload(height, rank, prevID, proposer, generateRandomPayload(blockByteSize, r))
}

// MakeBlockWithPayload creates an unsigned block over a caller-supplied
// payload — client requests drained from the mempool, in the client workload.
// The payload stays opaque bytes either way, so block hashing, the gob wire
// format and every protocol's handling are identical in both modes.
func MakeBlockWithPayload(height int, rank int, prevID crypto.Identifier, proposer identity.NodeID, payload []byte) *Block {
	b := new(Block)
	b.Height = height
	b.Rank = rank
	b.Proposer = proposer
	b.Payload = payload
	b.PrevID = prevID
	b.makeID(proposer)
	return b
}

func (b *Block) makeID(nodeID identity.NodeID) {
	raw := &rawBlock{
		Height:   b.Height,
		Rank:     b.Rank,
		Proposer: b.Proposer,
		PrevID:   b.PrevID,
	}
	raw.PayloadHash = crypto.MakeID(b.Payload)
	b.ID = crypto.MakeID(raw)
	// TODO: uncomment the following
	b.Sig, _ = crypto.PrivSign(crypto.IDToByte(b.ID), nodeID, nil)
}

func generateRandomPayload(size int, r *rand.Rand) []byte {
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		panic(err)
	}
	return payload
}
