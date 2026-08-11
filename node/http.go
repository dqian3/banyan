package node

import (
	"banyan/config"
	"banyan/log"
	"banyan/message"
	"io"
	"net/http"
	"net/url"
)

// http request header names
const (
	HTTPClientID  = "Id"
	HTTPCommandID = "Cid"
)

// serve serves the http REST API request from clients
func (n *node) http() {
	mux := http.NewServeMux()
	mux.HandleFunc("/query", n.handleQuery)
	mux.HandleFunc("/request", n.handleRequest)

	// http string should be in form of ":8080"
	ip, err := url.Parse(config.Configuration.HTTPAddrs[n.id])
	if err != nil {
		log.Fatal("http url parse error: ", err)
	}
	port := ":" + ip.Port()
	n.server = &http.Server{
		Addr:    port,
		Handler: mux,
	}
	log.Info("http server starting on ", port)
	log.Fatal(n.server.ListenAndServe())
}

// handleRequest accepts a client request and holds the connection open until
// the request commits, so the client's response time is its end-to-end
// latency. The body is the request payload; the Cid header carries the
// client's id for the request, echoed back on reply.
//
// Requests enter this node's mempool and are proposed by this node when its
// turn comes — they are never forwarded to the current leader — so the
// queueing delay a client sees here is the delay the replica reports as
// proposeWait.
func (n *node) handleRequest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id := r.Header.Get(HTTPCommandID)
	if id == "" {
		http.Error(w, "missing "+HTTPCommandID+" header", http.StatusBadRequest)
		return
	}

	req := message.Request{
		ID:      id,
		Payload: body,
		// Buffered so a commit never blocks on a client that has gone away;
		// Request.Reply also drops rather than waits.
		C: make(chan message.RequestReply, 1),
	}
	n.TxChan <- req

	select {
	case reply := <-req.C:
		if reply.Err != "" {
			http.Error(w, reply.Err, http.StatusServiceUnavailable)
			return
		}
		if _, err := io.WriteString(w, reply.ID); err != nil {
			log.Error(err)
		}
	case <-r.Context().Done():
		// Client hung up or timed out; the request may still commit, which
		// is the client's problem to detect, not ours to block on.
	}
}

func (n *node) handleQuery(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var query message.Query
	query.C = make(chan message.QueryReply)
	n.TxChan <- query
	reply := <-query.C
	_, err := io.WriteString(w, reply.Info)
	if err != nil {
		log.Error(err)
	}
}
