package crypto

// Client request signing.
//
// Blocks used to carry client requests as opaque bytes: nothing signed them
// and nothing checked them, so banyan was the only protocol in the evaluation
// paying no per-request signature cost. Aspen verifies a client's Ed25519
// signature on every request before it can be ordered, and a PBFT or Zyzzyva
// backup re-verifies every client signature embedded in a PRE-PREPARE. This
// is the equivalent: a client signs each request, the node that receives it
// verifies before admitting it to the mempool, and every other node verifies
// it again out of the block that carries it.
//
// Keys are derived from the client id rather than distributed, the same way
// node keys are. That is not a PKI -- anyone can derive any client's private
// key -- but the *work* is the work the other protocols do, which is what the
// benchmark is comparing. See the README's note on what banyan's signatures
// do and do not establish.

import (
	"encoding/binary"
	"sync"

	"banyan/identity"
)

// clientNodeID adapts a client id for the shared (ECDSA) keygen path, which is
// keyed by NodeID. Never zero: ECDSA keygen reads from StaticRand, whose Read
// returns the node number as its byte count, and a count of zero makes
// io.ReadFull spin forever.
func clientNodeID(clientID uint32) identity.NodeID {
	return identity.NewNodeID(int(clientID) + 1)
}

var (
	clientKeyMu   sync.RWMutex
	clientPrivate = map[uint32]PrivateKey{}
	clientPublic  = map[uint32]PublicKey{}
	clientScheme  string
)

// SetClientScheme selects the signing scheme used for client requests.
//
// Separate from SetKeys because clients run as their own processes and never
// load the node config: the harness passes the scheme to both sides, and a
// mismatch shows up as every request failing verification rather than as
// silently unsigned traffic.
func SetClientScheme(signer string) {
	clientKeyMu.Lock()
	defer clientKeyMu.Unlock()
	if signer == "" {
		signer = ECDSA_P256
	}
	if signer != clientScheme {
		// Cached keys belong to the old scheme.
		clientPrivate = map[uint32]PrivateKey{}
		clientPublic = map[uint32]PublicKey{}
	}
	clientScheme = signer
}

func clientSchemeOrDefault() string {
	if clientScheme == "" {
		return ECDSA_P256
	}
	return clientScheme
}

// ClientPrivateKey returns (and caches) the signing key for a client id.
func ClientPrivateKey(clientID uint32) (PrivateKey, error) {
	clientKeyMu.RLock()
	k, ok := clientPrivate[clientID]
	clientKeyMu.RUnlock()
	if ok {
		return k, nil
	}

	clientKeyMu.Lock()
	defer clientKeyMu.Unlock()
	if k, ok := clientPrivate[clientID]; ok {
		return k, nil
	}
	k, err := newClientKey(clientSchemeOrDefault(), clientID)
	if err != nil {
		return nil, err
	}
	clientPrivate[clientID] = k
	clientPublic[clientID] = k.PublicKey()
	return k, nil
}

// ClientPublicKey returns (and caches) the verification key for a client id.
//
// The cache is what keeps per-request verification affordable: deriving an
// Ed25519 public key costs a scalar multiplication, which would otherwise be
// paid again on every request rather than once per client.
func ClientPublicKey(clientID uint32) (PublicKey, error) {
	clientKeyMu.RLock()
	pk, ok := clientPublic[clientID]
	clientKeyMu.RUnlock()
	if ok {
		return pk, nil
	}
	if _, err := ClientPrivateKey(clientID); err != nil {
		return nil, err
	}
	clientKeyMu.RLock()
	defer clientKeyMu.RUnlock()
	return clientPublic[clientID], nil
}

func newClientKey(signer string, clientID uint32) (PrivateKey, error) {
	if signer == ED25519 {
		return newEd25519Key("client", clientID), nil
	}
	if signer == NONE {
		return &none_PrivateKey{SignAlg: signer}, nil
	}
	// ECDSA_P256 and friends go through the shared path. Its keygen ignores
	// the identity (see ed25519.go), so every client shares one ECDSA keypair
	// exactly as every node does -- the cost is right, the identity is not.
	return GenerateKey(signer, clientNodeID(clientID))
}

// SignedRequestBytes is the message a client signs and a node verifies.
//
// The client id is bound in so a signature cannot be replayed under another
// client's identity, and the request id so it cannot be replayed for another
// request carrying the same payload.
func SignedRequestBytes(clientID uint32, id string, payload []byte) []byte {
	buf := make([]byte, 0, 4+len(id)+1+len(payload))
	var idb [4]byte
	binary.BigEndian.PutUint32(idb[:], clientID)
	buf = append(buf, idb[:]...)
	buf = append(buf, id...)
	// Length-delimit rather than just concatenating: without a separator the
	// pair (id="a", payload="bc") and (id="ab", payload="c") sign the same
	// bytes, and one client's signature would verify for the other request.
	buf = append(buf, 0)
	buf = append(buf, payload...)
	return buf
}

// SignRequest produces a client's signature over one request.
func SignRequest(clientID uint32, id string, payload []byte) (Signature, error) {
	k, err := ClientPrivateKey(clientID)
	if err != nil {
		return nil, err
	}
	return k.Sign(SignedRequestBytes(clientID, id, payload), nil)
}

// VerifyRequest checks a client's signature over one request.
func VerifyRequest(clientID uint32, id string, payload []byte, sig Signature) bool {
	pk, err := ClientPublicKey(clientID)
	if err != nil || pk == nil {
		return false
	}
	ok, err := pk.Verify(sig, SignedRequestBytes(clientID, id, payload))
	return err == nil && ok
}
