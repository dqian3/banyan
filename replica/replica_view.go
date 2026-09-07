package replica

import (
	"encoding/gob"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/atomic"

	blockchain "banyan/blockchain_view"
	"banyan/config"
	"banyan/election"
	"banyan/identity"
	"banyan/log"
	"banyan/message"
	"banyan/node"
	"banyan/pacemaker"
	"banyan/protocol"
	"banyan/types"
)

type ReplicaView struct {
	node.Node
	SafetyView
	election.Election
	pm              *pacemaker.Pacemaker
	start           chan bool // signal to start the node
	isStarted       atomic.Bool
	isByz           bool
	strategy        string
	timer           *time.Timer // timeout for each view
	committedBlocks chan *blockchain.Block
	forkedBlocks    chan *blockchain.Block
	eventChan       chan interface{}

	/* for monitoring node statistics */

	experimentStartTime time.Time
	experimentDuration  time.Duration

	allBlockLatency      []time.Duration
	myBlockLatency       []time.Duration
	allBlockTimes        []time.Duration
	lastBlockProposeTime time.Time
	oneBlockPayloadBytes int
	committedBlockNo     int
	lastViewTime         time.Time
	experimentStarted    bool
}

// NewReplica creates a new replica instance
func NewReplicaView(id identity.NodeID, alg string, isByz bool) *ReplicaView {
	r := new(ReplicaView)
	r.Node = node.NewNode(id, isByz)
	if isByz {
		log.Infof("[%v] is Byzantine", r.ID())
	}
	r.Election = election.NewRotation(config.GetConfig().N)

	r.allBlockLatency = make([]time.Duration, 10000)
	r.myBlockLatency = make([]time.Duration, 10000)
	r.allBlockTimes = make([]time.Duration, 10000)
	r.experimentDuration = time.Second * time.Duration(config.GetConfig().ExperimentDuration)
	r.experimentStarted = false

	r.oneBlockPayloadBytes = config.GetConfig().PayloadSize
	r.isByz = isByz
	r.strategy = config.GetConfig().Strategy
	r.pm = pacemaker.NewPacemaker(config.GetConfig().N)
	// Buffered: the one receiver is ListenLocalEvent, which Start()
	// launches and which may not have reached `<-r.start` when the
	// first event arrives. A slot means the signal never blocks.
	r.start = make(chan bool, 1)
	r.eventChan = make(chan interface{}, 100)
	r.committedBlocks = make(chan *blockchain.Block, 100)
	r.forkedBlocks = make(chan *blockchain.Block, 100)
	r.Register(blockchain.Block{}, r.HandleBlock)
	r.Register(blockchain.Vote{}, r.HandleVote)
	r.Register(pacemaker.TMO{}, r.HandleTmo)
	r.Register(message.Query{}, r.handleQuery)
	gob.Register(blockchain.Block{})
	gob.Register(blockchain.Vote{})
	gob.Register(pacemaker.TC{})
	gob.Register(pacemaker.TMO{})

	// Is there a better way to reduce the number of parameters?
	switch alg {
	case "hotstuff":
		r.SafetyView = protocol.NewHotStuff(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	case "streamlet":
		r.SafetyView = protocol.NewStreamlet(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	default:
		r.SafetyView = protocol.NewHotStuff(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	}
	return r
}

/* Message Handlers */

func (r *ReplicaView) HandleBlock(block blockchain.Block) {
	log.Debugf("[%v] received a block from %v, view is %v, id: %x, prevID: %x", r.ID(), block.Proposer, block.View, block.ID, block.PrevID)
	r.eventChan <- block
}

func (r *ReplicaView) HandleVote(vote blockchain.Vote) {
	if vote.View < r.pm.GetCurView() {
		return
	}
	log.Debugf("[%v] received a vote frm %v, blockID is %x", r.ID(), vote.Voter, vote.BlockID)
	r.eventChan <- vote
}

func (r *ReplicaView) HandleTmo(tmo pacemaker.TMO) {
	if tmo.View < r.pm.GetCurView() {
		return
	}
	log.Debugf("[%v] received a timeout from %v for view %v", r.ID(), tmo.NodeID, tmo.View)
	r.eventChan <- tmo
}

// handleQuery replies a query with the statistics of the node
func (r *ReplicaView) handleQuery(m message.Query) {
	if !r.isByz {
		r.startSignal()
	}

	if !(r.experimentStarted && r.experimentStartTime.Add(r.experimentDuration).Before(time.Now())) {
		status := fmt.Sprintf("Committed blocks: %v.\n", r.committedBlockNo)
		m.Reply(message.QueryReply{Info: status})
		return
	}

	response := "blockPayloadSize\n"
	response += strconv.Itoa(r.oneBlockPayloadBytes) + "\n"

	response += "committedBlocks\n"
	response += strconv.Itoa(r.committedBlockNo) + "\n"

	response += "allBlockLatency\n"
	for i := 3; i <= r.committedBlockNo && i < len(r.allBlockLatency); i++ {
		response += strconv.Itoa(int(r.allBlockLatency[i].Milliseconds())) + ","
	}

	response += "\nproposerLatency\n"
	for i := 3; i <= r.committedBlockNo && i < len(r.allBlockLatency); i++ {
		response += strconv.Itoa(int(r.myBlockLatency[i].Milliseconds())) + ","
	}

	response += "\nblockTime\n"
	for i := 3; i <= r.committedBlockNo && i < len(r.allBlockLatency); i++ {
		response += strconv.Itoa(int(r.allBlockTimes[i].Milliseconds())) + ","
	}

	m.Reply(message.QueryReply{Info: response})
}

/* Processors */

func (r *ReplicaView) processCommittedBlock(block *blockchain.Block) {
	blockNum := r.committedBlockNo + 1
	if blockNum == 3 {
		r.experimentStartTime = time.Now()
		r.experimentStarted = true
	}
	if r.experimentStartTime.Add(r.experimentDuration).Before(time.Now()) && (blockNum > 3) {
		return
	}
	r.committedBlockNo++

	proposeTime := block.Timestamp
	if blockNum > 1 {
		r.allBlockTimes = setDuration(r.allBlockTimes, blockNum, proposeTime.Sub(r.lastBlockProposeTime))
	}
	now := time.Now()
	r.allBlockLatency = setDuration(r.allBlockLatency, blockNum, now.Sub(proposeTime))
	if block.Proposer == r.ID() {
		r.myBlockLatency = setDuration(r.myBlockLatency, blockNum, r.allBlockLatency[blockNum])
	}

	r.lastBlockProposeTime = proposeTime

	log.Infof("[%v] the block is committed, view: %v, id: %x", r.ID(), block.View, block.ID)
}

func (r *ReplicaView) processForkedBlock(block *blockchain.Block) {
	log.Infof("[%v] the block is forked, No. of transactions: %v, view: %v, current view: %v, id: %x", r.ID(), len(block.Payload), block.View, r.pm.GetCurView(), block.ID)
}

func (r *ReplicaView) processNewView(newView types.View) {
	log.Debugf("[%v] is processing new view: %v, leader is %v", r.ID(), newView, r.FindLeaderForView(newView))
	if !r.IsLeaderView(r.ID(), newView) {
		return
	}
	r.proposeBlock(newView)
}

func (r *ReplicaView) proposeBlock(view types.View) {
	block := r.SafetyView.MakeProposal(view, r.oneBlockPayloadBytes)
	block.Timestamp = time.Now()
	r.Broadcast(block)
	_ = r.SafetyView.ProcessBlock(block)
}

// ListenLocalEvent listens new view and timeout events
func (r *ReplicaView) ListenLocalEvent() {
	silence := r.isByz
	if silence {
		for {
			<-r.pm.EnteringViewEvent()
		}
	}

	<-r.start
	r.lastViewTime = time.Now()
	r.timer = time.NewTimer(r.pm.GetTimerForView())
	for {
		r.timer.Reset(r.pm.GetTimerForView())
	L:
		for {
			select {
			case view := <-r.pm.EnteringViewEvent():
				// measure round time
				now := time.Now()
				lasts := now.Sub(r.lastViewTime)
				r.lastViewTime = now
				r.eventChan <- view
				log.Debugf("[%v] the last view lasts %v milliseconds, current view: %v", r.ID(), lasts.Milliseconds(), view)
				break L
			case <-r.timer.C:
				r.SafetyView.ProcessLocalTmo(r.pm.GetCurView())
				break L
			}
		}
	}
}

// ListenCommittedBlocks listens committed blocks and forked blocks from the protocols
func (r *ReplicaView) ListenCommittedBlocks() {
	for {
		select {
		case committedBlock := <-r.committedBlocks:
			r.processCommittedBlock(committedBlock)
		case forkedBlock := <-r.forkedBlocks:
			r.processForkedBlock(forkedBlock)
		}
	}
}

func (r *ReplicaView) startSignal() {
	// CompareAndSwap, not Load-then-Store: startSignal is called from two
	// goroutines that both see the first event of a run -- the node's TxChan
	// worker (handleQuery for the harness's kickoff poke, handleRequest for
	// the first client request) and the replica's own event loop. Loading and
	// storing separately lets both observe false, and then both send on
	// `start`, which has exactly one receiver in ListenLocalEvent. The second
	// send blocks forever. When the loser is the event loop the node stays
	// alive and keeps answering /query -- handleQuery reads committedBlockNo
	// without going through the loop -- so it reports "Committed blocks: 0"
	// for as long as the harness cares to poll, while proposing nothing.
	if r.isStarted.CAS(false, true) {
		log.Debugf("[%v] is boosting", r.ID())
		r.start <- true
	}
}

// Starts event loop
func (r *ReplicaView) Start() {
	go r.Run()

	silence := r.isByz

	go r.ListenLocalEvent()
	go r.ListenCommittedBlocks()
	for {
		event := <-r.eventChan
		if silence {
			continue
		}
		r.startSignal()
		switch v := event.(type) {
		case types.View:
			r.processNewView(v)
		case blockchain.Block:
			r.SafetyView.ProcessBlock(&v)
		case blockchain.Vote:
			r.SafetyView.ProcessVote(&v)
		case pacemaker.TMO:
			r.SafetyView.ProcessRemoteTmo(&v)
		}
	}
}
