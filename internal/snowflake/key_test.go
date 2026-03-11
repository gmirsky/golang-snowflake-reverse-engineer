package snowflake

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

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
