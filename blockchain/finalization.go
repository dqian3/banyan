package blockchain

import (
	"fmt"

	"banyan/crypto"
	"banyan/identity"
	"banyan/log"
)

type FinalizationShare struct {
	Height  int
	Rank    int
	Voter   identity.NodeID
	BlockID crypto.Identifier
	crypto.Signature
}

type Finalization struct {
	Leader  identity.NodeID
	Height  int
	Rank    int
	BlockID crypto.Identifier
	Signers []identity.NodeID
	crypto.AggSig
	crypto.Signature
}

type FSharesBag struct {
	total int
	votes map[crypto.Identifier]map[identity.NodeID]*FinalizationShare
}

func MakeFShare(height int, rank int, voter identity.NodeID, id crypto.Identifier) *FinalizationShare {
	sig, err := crypto.PrivSign(crypto.IDToByte(id), voter, nil)
	if err != nil {
		log.Fatalf("[%v] has an error when signing a vote", voter)
		return nil
	}
	return &FinalizationShare{
		Height:    height,
		Rank:      rank,
		Voter:     voter,
		BlockID:   id,
		Signature: sig,
	}
}

func NewFSharesBag(total int) *FSharesBag {
	return &FSharesBag{
		total: total,
		votes: make(map[crypto.Identifier]map[identity.NodeID]*FinalizationShare),
	}
}

// Add adds id to quorum ack records
// Forget drops the finalization shares recorded for a block. Same reasoning as
// NSharesBagBanyan.Forget: nothing reads them once the block is settled, and
// upstream never deletes, so the bag holds n signatures for every block the
// node has ever seen.
func (q *FSharesBag) Forget(blockID crypto.Identifier) {
	delete(q.votes, blockID)
}

func (q *FSharesBag) Add(vote *FinalizationShare) bool {
	_, exist := q.votes[vote.BlockID]
	if !exist {
		//	first time of receiving the vote for this block
		q.votes[vote.BlockID] = make(map[identity.NodeID]*FinalizationShare)
	}
	q.votes[vote.BlockID][vote.Voter] = vote
	if q.superMajority(vote.BlockID) {
		//aggSig, signers, err := q.getSigs(vote.BlockID)
		_, _, err := q.getSigs(vote.BlockID)
		if err != nil {
			log.Warningf("cannot generate a valid qc, height: %v, block id: %x: %w", vote.Height, vote.BlockID, err)
		}
		/*
			qc := &Finalization{
				Height:  vote.Height,
				Rank:    vote.Rank,
				BlockID: vote.BlockID,
				AggSig:  aggSig,
				Signers: signers,
			}
		*/
		return true
	}
	return false
}

// Super majority quorum satisfied
func (q *FSharesBag) superMajority(blockID crypto.Identifier) bool {
	return q.size(blockID) > q.total*2/3
}

// Size returns ack size for the block
func (q *FSharesBag) size(blockID crypto.Identifier) int {
	return len(q.votes[blockID])
}

func (q *FSharesBag) getSigs(blockID crypto.Identifier) (crypto.AggSig, []identity.NodeID, error) {
	var sigs crypto.AggSig
	var signers []identity.NodeID
	_, exists := q.votes[blockID]
	if !exists {
		return nil, nil, fmt.Errorf("sigs does not exist, id: %x", blockID)
	}
	for _, vote := range q.votes[blockID] {
		sigs = append(sigs, vote.Signature)
		signers = append(signers, vote.Voter)
	}

	return sigs, signers, nil
}
