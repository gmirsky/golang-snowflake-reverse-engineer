package snowflake

import (
	"crypto/rsa"
	"fmt"
	"os"

	"golang.org/x/crypto/ssh"
)

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

	rsaKey, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key %s is not an RSA key", path)
	}

	return rsaKey, nil
}
