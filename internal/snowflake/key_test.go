package snowflake

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoadPrivateKey: Given a generated RSA key serialized to PEM, when
// LoadPrivateKey runs, then the returned key material should match exactly.
func TestLoadPrivateKey(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}

	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey() error = %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test_key.p8")
	block := &pem.Block{Type: "PRIVATE KEY", Bytes: der}
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	loaded, err := LoadPrivateKey(path, "")
	if err != nil {
		t.Fatalf("LoadPrivateKey() error = %v", err)
	}

	if loaded.N.Cmp(key.N) != 0 {
		t.Fatal("loaded key does not match generated key")
	}
}

func TestLoadPrivateKeyMissingFile(t *testing.T) {
	t.Parallel()

	_, err := LoadPrivateKey(filepath.Join(t.TempDir(), "missing.p8"), "")
	if err == nil || !strings.Contains(err.Error(), "read private key") {
		t.Fatalf("expected read failure, got %v", err)
	}
}

func TestLoadPrivateKeyInvalidPEM(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "bad_key.p8")
	if err := os.WriteFile(path, []byte("not a key"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err := LoadPrivateKey(path, "")
	if err == nil || !strings.Contains(err.Error(), "parse private key") {
		t.Fatalf("expected parse failure, got %v", err)
	}
}

func TestLoadPrivateKeyEncryptedWithPassphrase(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}

	der := x509.MarshalPKCS1PrivateKey(key)
	block, err := x509.EncryptPEMBlock(rand.Reader, "RSA PRIVATE KEY", der, []byte("correct"), x509.PEMCipherAES256)
	if err != nil {
		t.Fatalf("EncryptPEMBlock() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "encrypted_key.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	loaded, err := LoadPrivateKey(path, "correct")
	if err != nil {
		t.Fatalf("LoadPrivateKey() error = %v", err)
	}
	if loaded.N.Cmp(key.N) != 0 {
		t.Fatal("loaded encrypted key does not match generated key")
	}
}

func TestLoadPrivateKeyWrongPassphrase(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}

	der := x509.MarshalPKCS1PrivateKey(key)
	block, err := x509.EncryptPEMBlock(rand.Reader, "RSA PRIVATE KEY", der, []byte("correct"), x509.PEMCipherAES256)
	if err != nil {
		t.Fatalf("EncryptPEMBlock() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "encrypted_key.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err = LoadPrivateKey(path, "wrong")
	if err == nil || !strings.Contains(err.Error(), "parse private key") {
		t.Fatalf("expected parse failure with wrong passphrase, got %v", err)
	}
}

func TestLoadPrivateKeyRejectsNonRSAKey(t *testing.T) {
	t.Parallel()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}

	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey() error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "ec_key.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err = LoadPrivateKey(path, "")
	if err == nil || !strings.Contains(err.Error(), "is not an RSA key") {
		t.Fatalf("expected non-RSA key error, got %v", err)
	}
}
