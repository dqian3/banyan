package blockchain

import (
	"fmt"

	"banyan/crypto"
	"banyan/identity"
	"banyan/log"
)

type NotarizationShare struct {
	Height  int
	Rank    int
	Voter   identity.NodeID
	BlockID crypto.Identifier
	crypto.Signature
}

type Notarization struct {
	Leader  identity.NodeID
	Height  int
	Rank    int
	BlockID crypto.Identifier
	Signers []identity.NodeID
	crypto.AggSig
	crypto.Signature
}

type NSharesBag struct {
	total int
	votes map[crypto.Identifier]map[identity.NodeID]*NotarizationShare
}

func MakeNShare(height int, rank int, voter identity.NodeID, id crypto.Identifier) *NotarizationShare {
	sig, err := crypto.PrivSign(crypto.IDToByte(id), voter, nil)
	if err != nil {
		log.Fatalf("[%v] has an error when signing a vote", voter)
		return nil
	}
	return &NotarizationShare{
		Height:    height,
		Rank:      rank,
		Voter:     voter,
		BlockID:   id,
		Signature: sig,
	}
}

func NewNSharesBag(total int) *NSharesBag {
	return &NSharesBag{
		total: total,
		votes: make(map[crypto.Identifier]map[identity.NodeID]*NotarizationShare),
	}
}

// Add adds id to quorum ack records
func (q *NSharesBag) Add(vote *NotarizationShare) bool {
	_, exist := q.votes[vote.BlockID]
	if !exist {
		//	first time of receiving the vote for this block
		q.votes[vote.BlockID] = make(map[identity.NodeID]*NotarizationShare)
	}
	q.votes[vote.BlockID][vote.Voter] = vote
	if q.superMajority(vote.BlockID) {
		//aggSig, signers, err := q.getSigs(vote.BlockID)
		_, _, err := q.getSigs(vote.BlockID)
		if err != nil {
			log.Warningf("cannot generate a valid qc, height: %v, block id: %x: %w", vote.Height, vote.BlockID, err)
		}
		/*
			qc := &Notarization{
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
func (q *NSharesBag) superMajority(blockID crypto.Identifier) bool {
	return (len(q.votes[blockID])) > q.total*2/3
}

func (q *NSharesBag) getSigs(blockID crypto.Identifier) (crypto.AggSig, []identity.NodeID, error) {
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

// TODO: add crypto/aggregation of different types for Banyan
// TODO: handle multiple blocks of the same rank
type NSharesBagBanyan struct {
	n                 int
	f                 int
	p                 int
	votes             map[crypto.Identifier]map[identity.NodeID]*NotarizationShare
	fastVotesRankZero map[int]int
}

func NewNSharesBagBanyan(n int, f int, p int) *NSharesBagBanyan {
	return &NSharesBagBanyan{
		n:                 n,
		f:                 f,
		p:                 p,
		votes:             make(map[crypto.Identifier]map[identity.NodeID]*NotarizationShare),
		fastVotesRankZero: make(map[int]int),
	}
}

// Forget drops the votes recorded for a block and the fast-path tally for its
// height. Nothing consults either once the block is committed or forked, and
// without this the bag keeps every notarization share of every block for the
// life of the process — n signatures per block, which at loopback block rates
// is the bulk of a node's growth.
func (q *NSharesBagBanyan) Forget(blockID crypto.Identifier, height int) {
	delete(q.votes, blockID)
	delete(q.fastVotesRankZero, height)
}

// Add adds id to quorum ack records
// return (is notarized, is fast path finalized)
func (q *NSharesBagBanyan) Add(vote *NotarizationShare) (bool, bool) {
	_, exist := q.votes[vote.BlockID]
	if !exist {
		//	first time of receiving the vote for this block
		q.votes[vote.BlockID] = make(map[identity.NodeID]*NotarizationShare)
	}

	bagForThisBlock := q.votes[vote.BlockID]
	bagForThisBlock[vote.Voter] = vote

	if vote.Rank == -1 {
		q.fastVotesRankZero[vote.Height] += 1
	}

	isNotarized := (len(bagForThisBlock))*2 > q.n+q.f
	isFinalized := ((vote.Rank == -1) && (q.fastVotesRankZero[vote.Height] >= q.n-q.p))

	return isNotarized, isFinalized
}
