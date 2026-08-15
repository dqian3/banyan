package crypto

// Ed25519 signing, added so banyan's per-request verification costs the same
// as the rest of the evaluation's. Aspen, PBFT and Zyzzyva all verify an
// Ed25519 signature per client request; leaving banyan on ECDSA P256 (70 us a
// verify here against Ed25519's ~25 us) would have measured the curve as much
// as the protocol.
//
// Unlike the ECDSA path this derives a distinct key per identity. ECDSA keys
// come from StaticRand, a reader that returns a byte count without writing
// bytes, so every node's key is derived from the same all-zero scalar and any
// node can forge any other's signature. Seeding from the identity keeps
// keygen deterministic -- every node computes the same committee without a
// key distribution step, which is what the benchmark needs -- while still
// giving each identity its own keypair.

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
)

type ed25519PrivateKey struct {
	SignAlg string
	priv    ed25519.PrivateKey
}

type ed25519PublicKey struct {
	SignAlg string
	pub     ed25519.PublicKey
}

// ed25519Seed derives a scheme-separated 32-byte seed for an identity.
//
// `domain` keeps node keys and client keys in different spaces, so client 1
// and node 1 are not the same principal -- without it a client could sign
// blocks.
func ed25519Seed(domain string, id uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], id)
	sum := sha256.Sum256(append([]byte("banyan/ed25519/"+domain+"/"), buf[:]...))
	return sum[:]
}

func newEd25519Key(domain string, id uint32) *ed25519PrivateKey {
	priv := ed25519.NewKeyFromSeed(ed25519Seed(domain, id))
	return &ed25519PrivateKey{SignAlg: ED25519, priv: priv}
}

func (k *ed25519PrivateKey) Algorithm() string { return k.SignAlg }

func (k *ed25519PrivateKey) PublicKey() PublicKey {
	return &ed25519PublicKey{
		SignAlg: k.SignAlg,
		pub:     k.priv.Public().(ed25519.PublicKey),
	}
}

// Sign matches the ECDSA path's convention: when `hasher` is nil the message
// is signed as given, otherwise its hash is. Ed25519 hashes internally either
// way, so this only decides what gets fed in.
func (k *ed25519PrivateKey) Sign(msg []byte, hasher Hasher) (Signature, error) {
	if hasher != nil {
		msg = hasher.ComputeHash(msg)
	}
	return Signature{ed25519.Sign(k.priv, msg)}, nil
}

func (k *ed25519PublicKey) Algorithm() string { return k.SignAlg }

func (k *ed25519PublicKey) Verify(sig Signature, msg Hash) (bool, error) {
	// One element, unlike ECDSA's (r, s). A malformed signature is a failed
	// verification rather than an error: callers treat error as "could not
	// check" and a wrong shape is something we checked and rejected.
	if len(sig) != 1 {
		return false, nil
	}
	if len(sig[0]) != ed25519.SignatureSize {
		return false, nil
	}
	return ed25519.Verify(k.pub, msg, sig[0]), nil
}
