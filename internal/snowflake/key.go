// Package snowflake provides a Snowflake-backed Repository implementation and
// key-pair authentication helpers.
package snowflake

import (
	"crypto/rsa"
	"fmt"
	"os"

	"golang.org/x/crypto/ssh"
)

// LoadPrivateKey reads a PEM-encoded PKCS#8 RSA private key from path.
// When passphrase is non-empty the key is decrypted before parsing.
func LoadPrivateKey(path string, passphrase string) (*rsa.PrivateKey, error) {
	keyBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read private key: %w", err)
	}

	var parsed any
	if passphrase == "" {
		parsed, err = ssh.ParseRawPrivateKey(keyBytes)
	} else {
		parsed, err = ssh.ParseRawPrivateKeyWithPassphrase(keyBytes, []byte(passphrase))
	}
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	// Only RSA keys are accepted; reject EC or other key types explicitly.
	rsaKey, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key %s is not an RSA key", path)
	}

	return rsaKey, nil
}
