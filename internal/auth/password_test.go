package auth

import (
	"strings"
	"testing"

	"golang.org/x/crypto/argon2"
)

func TestHashVerifyRoundTrip(t *testing.T) {
	t.Parallel()

	encoded, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if !strings.HasPrefix(encoded, "$argon2id$v=19$m=65536,t=3,p=4$") {
		t.Errorf("encoding = %q, want the PHC form with the cost recorded", encoded)
	}
	if !VerifyPassword(encoded, "correct horse battery staple") {
		t.Error("VerifyPassword rejected the right password")
	}
	if VerifyPassword(encoded, "correct horse battery stapl") {
		t.Error("VerifyPassword accepted the wrong password")
	}
}

func TestHashIsSaltedPerCall(t *testing.T) {
	t.Parallel()

	a, _ := HashPassword("same")
	b, _ := HashPassword("same")
	if a == b {
		t.Error("two hashes of one password are identical, so the salt is not random")
	}
}

func TestVerifyRejectsMalformedHashes(t *testing.T) {
	t.Parallel()

	for name, encoded := range map[string]string{
		"empty":          "",
		"not a hash":     "hunter2",
		"wrong scheme":   "$argon2i$v=19$m=65536,t=3,p=4$c2FsdA$a2V5",
		"missing fields": "$argon2id$v=19$m=65536,t=3,p=4$c2FsdA",
		"future version": "$argon2id$v=20$m=65536,t=3,p=4$c2FsdA$a2V5",
		"bad params":     "$argon2id$v=19$m=lots,t=3,p=4$c2FsdA$a2V5",
		"bad base64":     "$argon2id$v=19$m=65536,t=3,p=4$!!!!$a2V5",
		"empty salt":     "$argon2id$v=19$m=65536,t=3,p=4$$a2V5",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if VerifyPassword(encoded, "anything") {
				t.Errorf("VerifyPassword(%q) accepted a malformed hash", encoded)
			}
		})
	}
}

// The cost travels with the digest so it can be raised without invalidating
// what is already stored.
func TestParseHashRecoversParameters(t *testing.T) {
	t.Parallel()

	encoded, err := HashPassword("x")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	p, salt, key, err := parseHash(encoded)
	if err != nil {
		t.Fatalf("parseHash: %v", err)
	}
	if p.memory != argonMemory || p.time != argonTime || p.threads != argonThreads {
		t.Errorf("params = %+v, want m=%d t=%d p=%d", p, argonMemory, argonTime, argonThreads)
	}
	if len(salt) != saltLen || len(key) != keyLen {
		t.Errorf("salt/key = %d/%d bytes, want %d/%d", len(salt), len(key), saltLen, keyLen)
	}
	if argon2.Version != 19 {
		t.Errorf("x/crypto argon2 version is now %d; the encoding says v=19", argon2.Version)
	}
}
