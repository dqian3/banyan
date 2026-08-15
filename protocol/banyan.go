package protocol

import (
	"banyan/blockchain"
	"banyan/config"
	"banyan/crypto"
	"banyan/election"
	"banyan/local_timeout"
	"banyan/log"
	"banyan/message"
	"banyan/node"
	"fmt"
	"math/rand"
	"time"
)

type Banyan struct {
	node.Node
	election.Election
	bc               *blockchain.BlockChain // all blocks I have
	lt               *local_timeout.LocalTimeout
	NSharesBagBanyan *blockchain.NSharesBagBanyan // notarization shares I've collected
	fSharesBag       *blockchain.FSharesBag       // finalization shares I've collected
	headHeight       int                          // highest notarized block height
	headId           crypto.Identifier            // id of the head
	sentNRank        map[int]int                  // what is the min rank of a notarization I sent on this height
	sentNSharesNo    map[int]int                  // how many notarization shares have I sent for blocks on this height?
	sentNShareId     map[int]crypto.Identifier    // what is the id of the (some) block for which I sent a notarization share on this height?
	sentFShare       map[int]struct{}             // have I sent a finalization share for this height?
	isNotarized      map[crypto.Identifier]struct{}
	isFinalized      map[crypto.Identifier]struct{}
	lastShippedBlock crypto.Identifier
	shipQueue        map[crypto.Identifier]struct{}
	committedBlocks  chan *blockchain.Block
	forkedBlocks     chan *blockchain.Block
	rand             *rand.Rand
	echoedBlock      map[crypto.Identifier]struct{}
	// Highest height whose vote bookkeeping has been dropped; see
	// forgetHeightsBelow.
	forgottenHeight int
}

func NewBanyan(
	node node.Node,
	elec election.Election,
	lt *local_timeout.LocalTimeout,
	committedBlocks chan *blockchain.Block,
	forkedBlocks chan *blockchain.Block,
	f int,
	p int) *Banyan {
	banyan := new(Banyan)
	banyan.Node = node
	banyan.Election = elec
	banyan.bc = blockchain.NewBlockchain(config.GetConfig().N)
	banyan.lt = lt
	banyan.NSharesBagBanyan = blockchain.NewNSharesBagBanyan(config.GetConfig().N, config.GetConfig().F, config.GetConfig().P)
	banyan.fSharesBag = blockchain.NewFSharesBag(config.GetConfig().N)
	banyan.headHeight = 0
	banyan.headId = crypto.MakeID("genesis")
	banyan.sentNRank = make(map[int]int)
	banyan.sentNSharesNo = make(map[int]int)
	banyan.sentNShareId = make(map[int]crypto.Identifier)
	banyan.sentFShare = make(map[int]struct{})
	banyan.isNotarized = make(map[crypto.Identifier]struct{})
	banyan.isFinalized = make(map[crypto.Identifier]struct{})
	banyan.lastShippedBlock = crypto.MakeID("genesis")
	banyan.shipQueue = make(map[crypto.Identifier]struct{})
	banyan.committedBlocks = committedBlocks
	banyan.forkedBlocks = forkedBlocks
	banyan.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	banyan.echoedBlock = make(map[crypto.Identifier]struct{}, 10000)
	banyan.forgottenHeight = 1

	return banyan
}

func (banyan *Banyan) ProcessBlock(block *blockchain.Block) error {
	if banyan.bc.Exists(block.ID) {
		return nil
	}
	log.Debugf("[%v] got new block, height: %v, rank: %v, id: %x", banyan.ID(), block.Height, block.Rank, block.ID)

	// some checks
	if !banyan.Election.IsLeader(block.Proposer, block.Height, block.Rank) {
		return fmt.Errorf("received a proposal (height %v) from an invalid leader (%v)", block.Height, block.Proposer)
	}
	if block.Proposer != banyan.ID() {
		blockIsVerified, _ := crypto.PubVerify(block.Sig, crypto.IDToByte(block.ID), block.Proposer)
		if !blockIsVerified {
			log.Warningf("[%v] received a block with an invalid signature", banyan.ID())
		}
		// Check every client request the block carries, not just the
		// proposer's signature over it. The proposer's signature only says
		// which node assembled these bytes; it says nothing about whether the
		// clients named inside actually asked for any of it, and without this
		// a single Byzantine proposer could put anything it liked into the
		// committed log. This is what a PBFT backup does with the client
		// signatures embedded in a PRE-PREPARE.
		//
		// Skipped for our own block: those requests were verified on arrival,
		// so re-checking them here would double the proposer's cost and make
		// the per-request total n+1 rather than n.
		if n, err := message.VerifyRequestPayload(block.Payload); err != nil {
			log.Warningf("[%v] rejecting block %x from %v: %v (%d request(s))",
				banyan.ID(), block.ID, block.Proposer, err, n)
			return err
		}
	}

	// add a new block!
	_, exists := banyan.echoedBlock[block.ID]
	if !exists && block.Height > banyan.headHeight {
		banyan.echoedBlock[block.ID] = struct{}{}
		banyan.Broadcast(block)
	}
	banyan.bc.AddBlock(block)

	// check if this block was missing
	_, queued := banyan.shipQueue[block.ID]
	if queued {
		banyan.TryToShip(block.ID)
	}

	// should I send a notarization share?
	if (banyan.headHeight < block.Height) && ((banyan.sentNSharesNo[block.Height] == 0) || (banyan.sentNRank[block.Height] > block.Rank)) {
		shareRank := block.Rank
		if (shareRank == 0) && (banyan.sentNSharesNo[block.Height] == 0) {
			shareRank = -1
		}
		notarizationShare := blockchain.MakeNShare(block.Height, shareRank, banyan.ID(), block.ID)
		banyan.sentNSharesNo[block.Height] += 1
		banyan.sentNRank[block.Height] = block.Rank
		banyan.sentNShareId[block.Height] = block.ID
		banyan.Broadcast(notarizationShare)
		banyan.ProcessNotarizationShare(notarizationShare)
	}

	// should I send a finalization share?
	_, isN := banyan.isNotarized[block.ID]
	_, sentF := banyan.sentFShare[block.Height]
	if isN && (!sentF) && (banyan.sentNSharesNo[block.Height] == 1) && (banyan.sentNShareId[block.Height] == block.ID) {
		finalizationShare := blockchain.MakeFShare(block.Height, block.Rank, banyan.ID(), block.ID)
		banyan.sentFShare[block.Height] = struct{}{}
		banyan.Broadcast(finalizationShare)
		banyan.ProcessFinalizationShare(finalizationShare)
	}

	return nil
}

func (banyan *Banyan) TryToShip(id crypto.Identifier) {
	if banyan.bc.Exists(id) {
		block, _ := banyan.bc.GetBlockByID(id)
		if banyan.lastShippedBlock == block.PrevID {
			// commiting the block
			committed, forked, err := banyan.bc.CommitBlock(id, block.Height)
			if err != nil {
				log.Errorf("[%v] cannot commit blocks, %w", banyan.ID(), err)
				return
			}
			for _, cBlock := range committed {
				banyan.forget(cBlock)
				banyan.committedBlocks <- cBlock
			}
			for _, fBlock := range forked {
				banyan.forget(fBlock)
				banyan.forkedBlocks <- fBlock
			}
			banyan.forgetHeightsBelow(block.Height)

			banyan.lastShippedBlock = id
			delete(banyan.shipQueue, id)

			for queued := range banyan.shipQueue {
				banyan.TryToShip(queued)
			}
		}
	} else {
		banyan.shipQueue[id] = struct{}{}
	}
}

func (banyan *Banyan) ProcessNotarizationShare(ns *blockchain.NotarizationShare) {
	_, isF := banyan.isFinalized[ns.BlockID]
	if isF {
		return
	}

	_, isN := banyan.isNotarized[ns.BlockID]
	if isN && (ns.Rank != -1) {
		return
	}

	log.Debugf("[%v] is processing NS from [%v], block id: %x", banyan.ID(), ns.Voter, ns.BlockID)
	if ns.Voter != banyan.ID() {
		voteIsVerified, err := crypto.PubVerify(ns.Signature, crypto.IDToByte(ns.BlockID), ns.Voter)
		if err != nil {
			log.Fatalf("[%v] Error in verifying the signature in vote id: %x", banyan.ID(), ns.BlockID)
			return
		}
		if !voteIsVerified {
			log.Warningf("[%v] received a vote with invalid signature. vote id: %x", banyan.ID(), ns.BlockID)
			return
		}
	}
	new_isN, new_isF := banyan.NSharesBagBanyan.Add(ns)

	if !isN && new_isN {
		// block is notarized!
		banyan.isNotarized[ns.BlockID] = struct{}{}
		if banyan.headHeight < ns.Height {
			banyan.headHeight = ns.Height
			banyan.headId = ns.BlockID
			banyan.lt.HeightIncreased(ns.Height + 1)
		}

		_, sentF := banyan.sentFShare[ns.Height]
		if (!sentF) && (banyan.sentNSharesNo[ns.Height] == 1) && (banyan.sentNShareId[ns.Height] == ns.BlockID) {
			finalizationShare := blockchain.MakeFShare(ns.Height, ns.Rank, banyan.ID(), ns.BlockID)
			banyan.sentFShare[ns.Height] = struct{}{}
			banyan.Broadcast(finalizationShare)
			banyan.ProcessFinalizationShare(finalizationShare)
		}
	}

	if new_isF {
		// block is fast-path finalized!
		banyan.isFinalized[ns.BlockID] = struct{}{}
		banyan.TryToShip(ns.BlockID)
	}
}

func (banyan *Banyan) ProcessFinalizationShare(fs *blockchain.FinalizationShare) {
	_, isF := banyan.isFinalized[fs.BlockID]
	if isF {
		return
	}
	log.Debugf("[%v] is processing FS, block id: %x", banyan.ID(), fs.BlockID)
	if fs.Voter != banyan.ID() {
		voteIsVerified, err := crypto.PubVerify(fs.Signature, crypto.IDToByte(fs.BlockID), fs.Voter)
		if err != nil {
			log.Fatalf("[%v] Error in verifying the signature in vote id: %x", banyan.ID(), fs.BlockID)
			return
		}
		if !voteIsVerified {
			log.Warningf("[%v] received a vote with invalid signature. vote id: %x", banyan.ID(), fs.BlockID)
			return
		}
	}
	isBuilt := banyan.fSharesBag.Add(fs)
	if !isBuilt {
		return
	}

	// block is finalized!
	banyan.isFinalized[fs.BlockID] = struct{}{}
	banyan.TryToShip(fs.BlockID)
}

// forget drops the per-block bookkeeping for a block that has been committed
// or forked. None of it is consulted again: the block is either settled in the
// chain or discarded from the forest.
//
// Upstream never deletes from any of these maps, so a node accumulates an
// entry per block for as long as it runs — and a banyan node runs forever,
// proposing a block per consensus round whether or not there is anything to
// order. On loopback that is thousands of blocks a second; four idle nodes
// grew to 1.6 GB each within four minutes during development.
func (banyan *Banyan) forget(block *blockchain.Block) {
	if block == nil {
		return
	}
	delete(banyan.isNotarized, block.ID)
	delete(banyan.isFinalized, block.ID)
	delete(banyan.echoedBlock, block.ID)
	delete(banyan.shipQueue, block.ID)
	banyan.NSharesBagBanyan.Forget(block.ID, block.Height)
	banyan.fSharesBag.Forget(block.ID)
}

// forgetHeightsBelow drops the height-keyed vote bookkeeping for heights the
// chain has already settled. Heights are dense, so this walks the gap since
// the last sweep rather than scanning the maps, keeping the cost proportional
// to progress instead of to history.
func (banyan *Banyan) forgetHeightsBelow(height int) {
	for h := banyan.forgottenHeight; h < height; h++ {
		delete(banyan.sentNRank, h)
		delete(banyan.sentNSharesNo, h)
		delete(banyan.sentNShareId, h)
		delete(banyan.sentFShare, h)
	}
	if height > banyan.forgottenHeight {
		banyan.forgottenHeight = height
	}
}

func (banyan *Banyan) MakeProposal(height int, rank int, payload []byte) *blockchain.Block {
	prevID := banyan.headId
	block := blockchain.MakeBlockWithPayload(height, rank, prevID, banyan.ID(), payload)
	return block
}
