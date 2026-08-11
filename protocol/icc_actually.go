package protocol

import (
	"banyan/blockchain"
	"banyan/config"
	"banyan/crypto"
	"banyan/election"
	"banyan/local_timeout"
	"banyan/log"
	"banyan/node"
	"fmt"
	"math/rand"
	"time"
)

type Icc struct {
	node.Node
	election.Election
	bc               *blockchain.BlockChain // all blocks I have
	lt               *local_timeout.LocalTimeout
	nSharesBag       *blockchain.NSharesBag    // notarization shares I've collected
	fSharesBag       *blockchain.FSharesBag    // finalization shares I've collected
	headHeight       int                       // highest notarized block height
	headId           crypto.Identifier         // id of the head
	sentNSharesNo    map[int]int               // how many notarization shares have I sent for blocks on this height?
	sentNShareId     map[int]crypto.Identifier // what is the id of the (some) block for which I sent a notarization share on this height?
	sentFShare       map[int]struct{}          // have I sent a finalization share for this height?
	isNotarized      map[crypto.Identifier]struct{}
	isFinalized      map[crypto.Identifier]struct{}
	lastShippedBlock crypto.Identifier
	shipQueue        map[crypto.Identifier]struct{}
	committedBlocks  chan *blockchain.Block
	forkedBlocks     chan *blockchain.Block
	rand             *rand.Rand
	echoedBlock      map[crypto.Identifier]struct{}
}

func NewIcc(
	node node.Node,
	elec election.Election,
	lt *local_timeout.LocalTimeout,
	committedBlocks chan *blockchain.Block,
	forkedBlocks chan *blockchain.Block) *Icc {
	icc := new(Icc)
	icc.Node = node
	icc.Election = elec
	icc.bc = blockchain.NewBlockchain(config.GetConfig().N)
	icc.lt = lt
	icc.nSharesBag = blockchain.NewNSharesBag(config.GetConfig().N)
	icc.fSharesBag = blockchain.NewFSharesBag(config.GetConfig().N)
	icc.headHeight = 0
	icc.headId = crypto.MakeID("genesis")
	icc.sentNSharesNo = make(map[int]int)
	icc.sentNShareId = make(map[int]crypto.Identifier)
	icc.sentFShare = make(map[int]struct{})
	icc.isNotarized = make(map[crypto.Identifier]struct{})
	icc.isFinalized = make(map[crypto.Identifier]struct{})
	icc.lastShippedBlock = crypto.MakeID("genesis")
	icc.shipQueue = make(map[crypto.Identifier]struct{})
	icc.committedBlocks = committedBlocks
	icc.forkedBlocks = forkedBlocks
	icc.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	icc.echoedBlock = make(map[crypto.Identifier]struct{}, 10000)

	return icc
}

func (icc *Icc) ProcessBlock(block *blockchain.Block) error {
	if icc.bc.Exists(block.ID) {
		return nil
	}
	log.Debugf("[%v] got new block, height: %v, rank: %v, id: %x", icc.ID(), block.Height, block.Rank, block.ID)

	// some checks
	if !icc.Election.IsLeader(block.Proposer, block.Height, block.Rank) {
		return fmt.Errorf("received a proposal (height %v) from an invalid leader (%v)", block.Height, block.Proposer)
	}
	if block.Proposer != icc.ID() {
		blockIsVerified, _ := crypto.PubVerify(block.Sig, crypto.IDToByte(block.ID), block.Proposer)
		if !blockIsVerified {
			log.Warningf("[%v] received a block with an invalid signature", icc.ID())
		}
	}

	// add a new block!
	_, exists := icc.echoedBlock[block.ID]
	if !exists && block.Height > icc.headHeight {
		icc.echoedBlock[block.ID] = struct{}{}
		icc.Broadcast(block)
	}
	icc.bc.AddBlock(block)

	// check if this block was missing
	_, queued := icc.shipQueue[block.ID]
	if queued {
		icc.TryToShip(block.ID)
	}

	// should I send a notarization share?
	if icc.headHeight < block.Height {
		notarizationShare := blockchain.MakeNShare(block.Height, block.Rank, icc.ID(), block.ID)
		icc.sentNSharesNo[block.Height] += 1
		icc.sentNShareId[block.Height] = block.ID
		icc.Broadcast(notarizationShare)
		icc.ProcessNotarizationShare(notarizationShare)
	}

	// should I send a finalization share?
	_, isN := icc.isNotarized[block.ID]
	_, sentF := icc.sentFShare[block.Height]
	if isN && (!sentF) && (icc.sentNSharesNo[block.Height] == 1) && (icc.sentNShareId[block.Height] == block.ID) {
		finalizationShare := blockchain.MakeFShare(block.Height, block.Rank, icc.ID(), block.ID)
		icc.sentFShare[block.Height] = struct{}{}
		icc.Broadcast(finalizationShare)
		icc.ProcessFinalizationShare(finalizationShare)
	}

	return nil
}

func (icc *Icc) TryToShip(id crypto.Identifier) {
	if icc.bc.Exists(id) {
		block, _ := icc.bc.GetBlockByID(id)
		if icc.lastShippedBlock == block.PrevID {
			// commiting the block
			committed, forked, err := icc.bc.CommitBlock(id, block.Height)
			if err != nil {
				log.Errorf("[%v] cannot commit blocks, %w", icc.ID(), err)
				return
			}
			for _, cBlock := range committed {
				icc.committedBlocks <- cBlock
			}
			for _, fBlock := range forked {
				icc.forkedBlocks <- fBlock
			}

			icc.lastShippedBlock = id
			delete(icc.shipQueue, id)

			for queued := range icc.shipQueue {
				icc.TryToShip(queued)
			}
		}
	} else {
		icc.shipQueue[id] = struct{}{}
	}
}

func (icc *Icc) ProcessNotarizationShare(ns *blockchain.NotarizationShare) {
	_, isN := icc.isNotarized[ns.BlockID]
	if isN {
		return
	}
	log.Debugf("[%v] is processing NS from [%v], block id: %x", icc.ID(), ns.Voter, ns.BlockID)
	if ns.Voter != icc.ID() {
		voteIsVerified, err := crypto.PubVerify(ns.Signature, crypto.IDToByte(ns.BlockID), ns.Voter)
		if err != nil {
			log.Fatalf("[%v] Error in verifying the signature in vote id: %x", icc.ID(), ns.BlockID)
			return
		}
		if !voteIsVerified {
			log.Warningf("[%v] received a vote with invalid signature. vote id: %x", icc.ID(), ns.BlockID)
			return
		}
	}
	isBuilt := icc.nSharesBag.Add(ns)
	if !isBuilt {
		return
	}

	// block is notarized!
	icc.isNotarized[ns.BlockID] = struct{}{}
	if icc.headHeight < ns.Height {
		icc.headHeight = ns.Height
		icc.headId = ns.BlockID
		icc.lt.HeightIncreased(ns.Height + 1)
	}

	_, sentF := icc.sentFShare[ns.Height]
	if (!sentF) && (icc.sentNSharesNo[ns.Height] == 1) && (icc.sentNShareId[ns.Height] == ns.BlockID) {
		finalizationShare := blockchain.MakeFShare(ns.Height, ns.Rank, icc.ID(), ns.BlockID)
		icc.sentFShare[ns.Height] = struct{}{}
		icc.Broadcast(finalizationShare)
		icc.ProcessFinalizationShare(finalizationShare)
	}
}

func (icc *Icc) ProcessFinalizationShare(fs *blockchain.FinalizationShare) {
	_, isF := icc.isFinalized[fs.BlockID]
	if isF {
		return
	}
	log.Debugf("[%v] is processing FS, block id: %x", icc.ID(), fs.BlockID)
	if fs.Voter != icc.ID() {
		voteIsVerified, err := crypto.PubVerify(fs.Signature, crypto.IDToByte(fs.BlockID), fs.Voter)
		if err != nil {
			log.Fatalf("[%v] Error in verifying the signature in vote id: %x", icc.ID(), fs.BlockID)
			return
		}
		if !voteIsVerified {
			log.Warningf("[%v] received a vote with invalid signature. vote id: %x", icc.ID(), fs.BlockID)
			return
		}
	}
	isBuilt := icc.fSharesBag.Add(fs)
	if !isBuilt {
		return
	}

	// block is finalized!
	icc.isFinalized[fs.BlockID] = struct{}{}
	icc.TryToShip(fs.BlockID)
}

func (icc *Icc) MakeProposal(height int, rank int, payload []byte) *blockchain.Block {
	prevID := icc.headId
	block := blockchain.MakeBlockWithPayload(height, rank, prevID, icc.ID(), payload)
	return block
}
