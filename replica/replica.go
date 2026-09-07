package replica

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"time"

	"go.uber.org/atomic"

	"banyan/blockchain"
	"banyan/config"
	"banyan/crypto"
	"banyan/election"
	"banyan/identity"
	"banyan/local_timeout"
	"banyan/log"
	"banyan/mempool"
	"banyan/message"
	"banyan/node"
	"banyan/protocol"
	"strconv"
)

type Replica struct {
	node.Node
	Safety
	election.Election
	lt              *local_timeout.LocalTimeout
	start           chan bool // signal to start the node
	isStarted       atomic.Bool
	isByz           bool
	strategy        string
	timer           *time.Timer // timeout for each rank
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
	lastHeightTime       time.Time
	experimentStarted    bool

	/* client workload (config.workload == "client") */

	clientDriven bool
	pool         *mempool.MemPool
	pending      *pendingRequests
	payloadRand  *rand.Rand
	// proposeWait is the delay a request spends queued at the node that
	// received it before some block includes it — the cost of waiting for
	// this node's turn to propose. commitWait extends that to the moment the
	// including block commits. Both are measured entirely on the receiving
	// node, so neither carries cross-machine clock skew.
	proposeWait       *waitStats
	commitWait        *waitStats
	committedRequests int
	// Requests turned away at arrival for a bad client signature. Reported so
	// a run that is rejecting everything -- a client and nodes configured with
	// different signing schemes, say -- says so, rather than looking like a
	// protocol that delivers nothing.
	requestsRejected int
}

// WarmupHeights is how many heights the chain runs before the measurement
// window opens, giving the committee time to reach steady state. The report
// arrays are also dumped from this height on.
const WarmupHeights = 3

// NewReplica creates a new replica instance
func NewReplica(id identity.NodeID, alg string, isByz bool) *Replica {
	r := new(Replica)
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
	r.clientDriven = config.GetConfig().IsClientDriven()
	// Clients sign with the same scheme the nodes are configured for. They
	// cannot read it from here -- they run as separate processes and are given
	// their targets on the command line -- so the harness passes it to both
	// sides and this is where the node half is set.
	crypto.SetClientScheme(config.GetConfig().GetSignatureScheme())
	r.pool = mempool.NewMemPool(config.GetConfig().MemSize)
	r.pending = newPendingRequests()
	// Seeded per node so two replicas don't generate identical payloads; the
	// value is never consensus-relevant, only block filler.
	r.payloadRand = rand.New(rand.NewSource(time.Now().UnixNano() + int64(id.Node())))
	r.proposeWait = newWaitStats(5000, rand.New(rand.NewSource(1)))
	r.commitWait = newWaitStats(5000, rand.New(rand.NewSource(2)))
	r.isByz = isByz
	r.strategy = config.GetConfig().Strategy
	r.lt = local_timeout.NewLocalTimeout()
	// Buffered: the one receiver is ListenLocalEvent, which Start()
	// launches and which may not have reached `<-r.start` when the
	// first event arrives. A slot means the signal never blocks.
	r.start = make(chan bool, 1)
	r.eventChan = make(chan interface{}, 100)
	r.committedBlocks = make(chan *blockchain.Block, 100)
	r.forkedBlocks = make(chan *blockchain.Block, 100)
	r.Register(blockchain.Block{}, r.HandleBlock)
	r.Register(blockchain.NotarizationShare{}, r.HandleNotarizationShare)
	r.Register(blockchain.FinalizationShare{}, r.HandleFinalizationShare)
	r.Register(message.Query{}, r.handleQuery)
	r.Register(message.Request{}, r.handleRequest)
	gob.Register(blockchain.Block{})
	gob.Register(blockchain.NotarizationShare{})
	gob.Register(blockchain.FinalizationShare{})

	switch alg {
	case "icc":
		r.Safety = protocol.NewIcc(r.Node, r.Election, r.lt, r.committedBlocks, r.forkedBlocks)
	case "banyan":
		r.Safety = protocol.NewBanyan(r.Node, r.Election, r.lt, r.committedBlocks, r.forkedBlocks, config.GetConfig().F, config.GetConfig().P)
	default:
		r.Safety = protocol.NewBanyan(r.Node, r.Election, r.lt, r.committedBlocks, r.forkedBlocks, config.GetConfig().F, config.GetConfig().P)
	}
	return r
}

/* Message Handlers */

func (r *Replica) HandleBlock(block blockchain.Block) {
	log.Debugf("[%v] received a block from %v, height is %v, id: %x, prevID: %x", r.ID(), block.Proposer, block.Height, block.ID, block.PrevID)
	r.eventChan <- block
}

func (r *Replica) HandleNotarizationShare(vote blockchain.NotarizationShare) {
	log.Debugf("[%v] received a N share frm %v, blockID is %x", r.ID(), vote.Voter, vote.BlockID)
	r.eventChan <- vote
}

func (r *Replica) HandleFinalizationShare(vote blockchain.FinalizationShare) {
	log.Debugf("[%v] received a F share frm %v, blockID is %x", r.ID(), vote.Voter, vote.BlockID)
	r.eventChan <- vote
}

// handleQuery replies a query with the statistics of the node
func (r *Replica) handleQuery(m message.Query) {
	r.startSignal()

	if !(r.experimentStarted && r.experimentStartTime.Add(r.experimentDuration).Before(time.Now())) {
		status := fmt.Sprintf("Committed blocks: %v.\n", r.committedBlockNo)
		m.Reply(message.QueryReply{Info: status})
		return
	}

	response := "blockPayloadSize\n"
	response += strconv.Itoa(r.oneBlockPayloadBytes) + "\n"

	// How long the counters below were actually accumulating. Under the client
	// workload this is the clients' warm-up *plus* their send window, so it is
	// not `bench.duration` and dividing the committed counts by that overstates
	// throughput by warmup/duration. Report it rather than making the harness
	// re-derive it from two config keys that have drifted apart before.
	response += "measurementWindowMs\n"
	response += strconv.FormatInt(r.experimentDuration.Milliseconds(), 10) + "\n"

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

	if r.clientDriven {
		response += "\n" + r.requestReport()
	}

	m.Reply(message.QueryReply{Info: response})
}

// inMeasurementWindow reports whether the experiment clock is running, so
// request stats cover the same interval as the block stats.
func (r *Replica) inMeasurementWindow() bool {
	return r.experimentStarted &&
		!r.experimentStartTime.Add(r.experimentDuration).Before(time.Now())
}

// requestReport appends the client-workload sections of the /query dump.
//
// Waits are summarized rather than dumped per request: a run can commit
// millions, so the report carries count/mean/max plus a bounded reservoir
// sample the harness turns into percentiles.
func (r *Replica) requestReport() string {
	received, dropped := r.pool.Stats()

	section := func(name string, w *waitStats) string {
		out := name + "Count\n" + strconv.FormatInt(w.count, 10) + "\n"
		out += name + "MeanMs\n" + strconv.FormatFloat(w.meanMs(), 'f', 3, 64) + "\n"
		out += name + "MaxMs\n" + strconv.FormatInt(w.maxMs, 10) + "\n"
		out += name + "SampleMs\n"
		for _, v := range w.sample {
			out += strconv.FormatInt(v, 10) + ","
		}
		return out + "\n"
	}

	response := "committedRequests\n" + strconv.Itoa(r.committedRequests) + "\n"
	response += "requestsReceived\n" + strconv.FormatInt(received, 10) + "\n"
	response += "requestsDropped\n" + strconv.FormatInt(dropped, 10) + "\n"
	response += "requestsRejected\n" + strconv.Itoa(r.requestsRejected) + "\n"
	response += "requestsPending\n" + strconv.Itoa(r.pending.size()) + "\n"
	response += "mempoolDepth\n" + strconv.Itoa(r.pool.Size()) + "\n"
	response += section("proposeWait", r.proposeWait)
	response += section("commitWait", r.commitWait)
	return response
}

// armSelfExit schedules the node to exit once the run is over, if the config
// asked for it. See config.ExitAfter: a node that outlives its harness keeps
// proposing forever and grows without bound, so a benchmark binary bounding
// its own life is the difference between a crashed run costing nothing and it
// quietly eating the machine.
func (r *Replica) armSelfExit() {
	after := config.GetConfig().ExitAfter
	if after <= 0 {
		return
	}
	grace := r.experimentDuration + time.Duration(after)*time.Second
	time.AfterFunc(grace, func() {
		log.Infof("[%v] experiment finished %v ago; exiting", r.ID(), grace)
		os.Exit(0)
	})
}

/* Processors */

func (r *Replica) processCommittedBlock(block *blockchain.Block) {
	// Open the measurement window on the first commit at or past the warm-up
	// height. Keying on height *exactly* 3 silently loses the whole run
	// whenever that height is not observed as its own commit — a commit can
	// cover several heights at once, and the fast path can carry the chain
	// past 3 before this node processes it. When that happened,
	// experimentStartTime stayed the zero value, whose window closed in year
	// 1, so every subsequent block was treated as post-window: zero committed
	// requests and zero recorded waits, while clients still got their replies.
	if !r.experimentStarted && block.Height >= WarmupHeights {
		r.experimentStartTime = time.Now()
		r.experimentStarted = true
		r.armSelfExit()
	}
	// The experimentStarted guard matters for the same reason: without it a
	// zero start time makes this test true for everything.
	if r.experimentStarted &&
		r.experimentStartTime.Add(r.experimentDuration).Before(time.Now()) &&
		(block.Height > WarmupHeights) {
		// Past the measurement window: stop recording, but still answer the
		// clients in this block. Leaving them unanswered would show up as a
		// wave of timeouts in the client's tail latency at the end of every
		// run.
		if r.clientDriven {
			r.answerCommitted(block.Payload)
		}
		return
	}

	proposeTime := block.Timestamp
	if block.Height > 1 {
		r.allBlockTimes = setDuration(r.allBlockTimes, block.Height, proposeTime.Sub(r.lastBlockProposeTime))
	}
	now := time.Now()
	r.allBlockLatency = setDuration(r.allBlockLatency, block.Height, now.Sub(proposeTime))
	if block.Proposer == r.ID() {
		r.myBlockLatency = setDuration(r.myBlockLatency, block.Height, r.allBlockLatency[block.Height])
	}
	r.committedBlockNo++
	r.lastBlockProposeTime = proposeTime
	if r.clientDriven {
		r.committedRequests += r.answerCommitted(block.Payload)
	}

	log.Infof("[%v] the block is committed, height: %v, id: %x", r.ID(), block.Height, block.ID)
}

func (r *Replica) processForkedBlock(block *blockchain.Block) {
	log.Infof("[%v] the block is forked, No. of transactions: %v, height: %v, id: %x", r.ID(), len(block.Payload), block.Height, block.ID)
}

func (r *Replica) proposeIfLeader(height int, rank int) {
	if !r.IsLeader(r.ID(), height, rank) {
		return
	}
	r.proposeBlock(height, rank)
}

func (r *Replica) proposeBlock(height int, rank int) {
	block := r.Safety.MakeProposal(height, rank, r.buildPayload())
	block.Timestamp = time.Now()
	r.Broadcast(block)
	_ = r.Safety.ProcessBlock(block)
}

// proposeRequest asks the event loop to propose, if this node is the leader
// for (height, rank). ListenLocalEvent runs on its own goroutine and must not
// touch Safety directly: the protocol state machine is not concurrency-safe,
// and a self-proposal racing an inbound block corrupts the blockchain's maps
// ("fatal error: concurrent map read and map write" in Banyan.ProcessBlock).
// Routing the request through eventChan keeps every Safety call on the single
// event-loop goroutine, which is how ReplicaView (hotstuff/streamlet) already
// handles new views.
type proposeRequest struct {
	height int
	rank   int
}

// ListenLocalEvent listens new height and timeout events
func (r *Replica) ListenLocalEvent() {
	block_production_height := 1
	block_production_rank := 0
	<-r.start
	r.eventChan <- proposeRequest{block_production_height, block_production_rank}
	r.lastHeightTime = time.Now()
	r.timer = time.NewTimer(r.lt.GetTimeoutDuration())
	for {
		r.timer.Reset(r.lt.GetTimeoutDuration())
	L:
		for {
			select {
			case new_height := <-r.lt.GetNewHeight():
				block_production_height = new_height
				block_production_rank = 0
				r.eventChan <- proposeRequest{block_production_height, block_production_rank}
				// measure round time
				now := time.Now()
				lasts := now.Sub(r.lastHeightTime)
				r.lastHeightTime = now
				log.Debugf("[%v] the last height lasted %v milliseconds", r.ID(), lasts.Milliseconds())
				break L
			case <-r.timer.C:
				block_production_rank += 1
				r.eventChan <- proposeRequest{block_production_height, block_production_rank}
				break L
			}
		}
	}
}

// ListenCommittedBlocks listens committed blocks and forked blocks from the protocols
func (r *Replica) ListenCommittedBlocks() {
	for {
		select {
		case committedBlock := <-r.committedBlocks:
			r.processCommittedBlock(committedBlock)
		case forkedBlock := <-r.forkedBlocks:
			r.processForkedBlock(forkedBlock)
		}
	}
}

func (r *Replica) startSignal() {
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
func (r *Replica) Start() {
	go r.Run()

	go r.ListenLocalEvent()
	go r.ListenCommittedBlocks()
	for {
		event := <-r.eventChan
		r.startSignal()
		switch v := event.(type) {
		case proposeRequest:
			r.proposeIfLeader(v.height, v.rank)
		case blockchain.Block:
			r.Safety.ProcessBlock(&v)
		case blockchain.NotarizationShare:
			r.Safety.ProcessNotarizationShare(&v)
		case blockchain.FinalizationShare:
			r.Safety.ProcessFinalizationShare(&v)
		}
	}
}
