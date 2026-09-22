package jwt

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	golangjwt "github.com/golang-jwt/jwt/v5"
)

// Generating an RSA key takes a noticeable fraction of a second, so tests share two.
var testKeys = sync.OnceValues(func() (*rsa.PrivateKey, *rsa.PrivateKey) {
	a, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	b, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return a, b
})

func TestSignVerify_RoundTripsEveryClaim(t *testing.T) {
	key, _ := testKeys()
	token, err := NewSigner(key).Sign("user-1", "a@b.com", "organizer", 15*time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	claims, err := NewVerifier(&key.PublicKey).Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if claims.UserID != "user-1" || claims.Email != "a@b.com" || claims.Role != "organizer" {
		t.Errorf("claims = %+v, want user-1 / a@b.com / organizer", claims)
	}
	if claims.IssuedAt == nil || claims.ExpiresAt == nil {
		t.Fatal("iat and exp must both be set")
	}
	if got := claims.ExpiresAt.Sub(claims.IssuedAt.Time); got != 15*time.Minute {
		t.Errorf("token lifetime = %v, want 15m", got)
	}
}

func TestVerify_RejectsExpiredToken(t *testing.T) {
	key, _ := testKeys()
	token, err := NewSigner(key).Sign("u", "a@b.com", "user", -time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	if _, err := NewVerifier(&key.PublicKey).Verify(token); err == nil {
		t.Fatal("an expired token was accepted")
	}
}

func TestVerify_RejectsATokenSignedByAnotherKey(t *testing.T) {
	trusted, attacker := testKeys()
	token, err := NewSigner(attacker).Sign("u", "a@b.com", "admin", time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	if _, err := NewVerifier(&trusted.PublicKey).Verify(token); err == nil {
		t.Fatal("a token signed with a different private key was accepted")
	}
}

// Changing the role inside a signed token must break the signature.
func TestVerify_RejectsATamperedPayload(t *testing.T) {
	key, _ := testKeys()
	token, err := NewSigner(key).Sign("u", "a@b.com", "user", time.Minute)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	parts := strings.Split(token, ".")
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	forged := strings.Replace(string(payload), `"role":"user"`, `"role":"admin"`, 1)
	if forged == string(payload) {
		t.Fatal("test setup: the role claim was not found to tamper with")
	}
	parts[1] = base64.RawURLEncoding.EncodeToString([]byte(forged))

	if _, err := NewVerifier(&key.PublicKey).Verify(strings.Join(parts, ".")); err == nil {
		t.Fatal("a token with a tampered role was accepted")
	}
}

// The classic JWT attack: sign with HS256 using the server's public key as the
// HMAC secret. It must be rejected because only RS256 is accepted.
func TestVerify_RejectsAlgorithmConfusion(t *testing.T) {
	key, _ := testKeys()
	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})

	forged, err := golangjwt.NewWithClaims(golangjwt.SigningMethodHS256, Claims{
		UserID: "attacker", Email: "x@y.z", Role: "admin",
		RegisteredClaims: golangjwt.RegisteredClaims{ExpiresAt: golangjwt.NewNumericDate(time.Now().Add(time.Hour))},
	}).SignedString(pubPEM)
	if err != nil {
		t.Fatalf("building the forged token: %v", err)
	}

	if _, err := NewVerifier(&key.PublicKey).Verify(forged); err == nil {
		t.Fatal("an HS256 token signed with the public key was accepted")
	}
}

func TestVerify_RejectsUnsignedTokens(t *testing.T) {
	key, _ := testKeys()
	unsigned, err := golangjwt.NewWithClaims(golangjwt.SigningMethodNone, Claims{
		UserID: "attacker", Role: "admin",
		RegisteredClaims: golangjwt.RegisteredClaims{ExpiresAt: golangjwt.NewNumericDate(time.Now().Add(time.Hour))},
	}).SignedString(golangjwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("building the unsigned token: %v", err)
	}

	if _, err := NewVerifier(&key.PublicKey).Verify(unsigned); err == nil {
		t.Fatal(`a token with alg "none" was accepted`)
	}
}

func TestVerify_RejectsGarbage(t *testing.T) {
	key, _ := testKeys()
	v := NewVerifier(&key.PublicKey)
	for _, in := range []string{"", "not.a.token", "a.b", strings.Repeat("x", 500)} {
		if _, err := v.Verify(in); err == nil {
			t.Errorf("Verify(%q) accepted garbage", in)
		}
	}
}

func TestLoadKeys_RoundTripThroughPEMFiles(t *testing.T) {
	key, _ := testKeys()
	dir := t.TempDir()

	privPath := filepath.Join(dir, "private.pem")
	writePEM(t, privPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pubPath := filepath.Join(dir, "public.pem")
	writePEM(t, pubPath, "PUBLIC KEY", pubDER)

	priv, err := LoadPrivateKey(privPath)
	if err != nil {
		t.Fatalf("LoadPrivateKey: %v", err)
	}
	pub, err := LoadPublicKey(pubPath)
	if err != nil {
		t.Fatalf("LoadPublicKey: %v", err)
	}

	token, err := NewSigner(priv).Sign("u", "a@b.com", "user", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewVerifier(pub).Verify(token); err != nil {
		t.Errorf("a token signed with the loaded private key did not verify with the loaded public key: %v", err)
	}
}

func TestLoadKeys_RejectBadInput(t *testing.T) {
	dir := t.TempDir()
	notPEM := filepath.Join(dir, "junk.pem")
	if err := os.WriteFile(notPEM, []byte("this is not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	wrongType := filepath.Join(dir, "wrong.pem")
	writePEM(t, wrongType, "PUBLIC KEY", []byte("not a real key body"))

	for name, load := range map[string]func() error{
		"private: missing file": func() error { _, err := LoadPrivateKey(filepath.Join(dir, "absent.pem")); return err },
		"private: not PEM":      func() error { _, err := LoadPrivateKey(notPEM); return err },
		"private: bad key body": func() error { _, err := LoadPrivateKey(wrongType); return err },
		"public: missing file":  func() error { _, err := LoadPublicKey(filepath.Join(dir, "absent.pem")); return err },
		"public: not PEM":       func() error { _, err := LoadPublicKey(notPEM); return err },
		"public: bad key body":  func() error { _, err := LoadPublicKey(wrongType); return err },
	} {
		if err := load(); err == nil {
			t.Errorf("%s: got nil error, want a failure", name)
		}
	}
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}
