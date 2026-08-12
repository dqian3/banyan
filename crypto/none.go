package crypto

// The NONE signing scheme: keys that sign nothing and verify everything.
//
// It exists so banyan can be measured with signature work removed, the way the
// other protocols in the evaluation can be. Selected with `signer: "NONE"` in
// the config, so it goes through the same GetSignatureScheme path as the real
// schemes rather than adding a second switch.
//
// This removes signing and verification only. Hashing, serialisation and the
// message flow are untouched, so the difference against an ECDSA run is the
// signature cost and nothing else.
//
// Obviously not safe: every signature verifies. Benchmarking only.

// noneSignature is a fixed two-element signature, matching the shape ECDSA
// produces (r, s), so nothing downstream sees a differently shaped value.
var noneSignature = Signature{{0}, {0}}

type none_PrivateKey struct {
	SignAlg string
}

type none_PublicKey struct {
	SignAlg string
}

func (priv *none_PrivateKey) Algorithm() string {
	return priv.SignAlg
}

func (priv *none_PrivateKey) Sign(msg []byte, hasher Hasher) (Signature, error) {
	return noneSignature, nil
}

func (priv *none_PrivateKey) PublicKey() PublicKey {
	return &none_PublicKey{SignAlg: priv.SignAlg}
}

func (pub *none_PublicKey) Algorithm() string {
	return pub.SignAlg
}

func (pub *none_PublicKey) Verify(sig Signature, hash Hash) (bool, error) {
	return true, nil
}
