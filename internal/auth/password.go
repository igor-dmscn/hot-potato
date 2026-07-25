package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Argon2id cost, RFC 9106's second recommended option: memory-hard enough to
// make a GPU farm expensive, cheap enough to sit in a login request.
const (
	argonMemory  = 64 * 1024 // KiB — 64 MiB
	argonTime    = 3
	argonThreads = 4
	saltLen      = 16
	keyLen       = 32
)

var errBadHash = errors.New("not an argon2id hash")

// HashPassword returns a PHC-encoded argon2id hash: the parameters travel with
// the digest, so raising the cost later does not invalidate stored hashes.
func HashPassword(plain string) (string, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("read salt: %w", err)
	}
	key := argon2.IDKey([]byte(plain), salt, argonTime, argonMemory, argonThreads, keyLen)
	b64 := base64.RawStdEncoding
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, argonMemory, argonTime, argonThreads,
		b64.EncodeToString(salt), b64.EncodeToString(key)), nil
}

// VerifyPassword compares in constant time. It does the same work whatever the
// answer, which is what makes it safe to call with a hash belonging to nobody
// — see the decoy in Service.Login.
func VerifyPassword(encoded, plain string) bool {
	p, salt, want, err := parseHash(encoded)
	if err != nil {
		return false
	}
	got := argon2.IDKey([]byte(plain), salt, p.time, p.memory, p.threads, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1
}

type argonParams struct {
	memory, time uint32
	threads      uint8
}

func parseHash(encoded string) (p argonParams, salt, key []byte, err error) {
	// $argon2id$v=19$m=65536,t=3,p=4$<salt>$<key>
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return p, nil, nil, errBadHash
	}
	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil || version != argon2.Version {
		return p, nil, nil, errBadHash
	}
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &p.memory, &p.time, &p.threads); err != nil {
		return p, nil, nil, errBadHash
	}
	b64 := base64.RawStdEncoding
	if salt, err = b64.DecodeString(parts[4]); err != nil {
		return p, nil, nil, errBadHash
	}
	if key, err = b64.DecodeString(parts[5]); err != nil {
		return p, nil, nil, errBadHash
	}
	if len(salt) == 0 || len(key) == 0 {
		return p, nil, nil, errBadHash
	}
	return p, salt, key, nil
}
