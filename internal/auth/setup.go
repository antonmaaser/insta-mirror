package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func prepareSessionDir(dir string) error {
	path := filepath.Join(dir, "sessions")
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	return os.Chmod(path, 0700)
}

func readSecretFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("secret file must be a regular file accessible only by its owner")
	}
	if info.Size() > 4096 {
		return nil, errors.New("secret file too large")
	}
	password, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	password = []byte(strings.TrimRight(string(password), "\r\n"))
	if len(password) < 12 {
		clear(password)
		return nil, errors.New("session passphrase must contain at least 12 bytes")
	}
	return password, nil
}

// Setup creates a session key once. Existing keys are validated, never replaced.
func Setup(dir, secretFile string) error {
	if secretFile == "" {
		return errors.New("setup requires a session key file path")
	}
	if err := prepareSessionDir(dir); err != nil {
		return err
	}
	password, err := readSecretFile(secretFile)
	if err == nil {
		clear(password)
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	entries, err := os.ReadDir(filepath.Join(dir, "sessions"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".enc") {
			return errors.New("saved sessions exist but the session key is missing; restore the original key or select it with --secret-file")
		}
	}
	parent := filepath.Dir(secretFile)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return err
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	defer clear(key)
	f, err := os.CreateTemp(parent, ".session-key-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	encoded := []byte(hex.EncodeToString(key) + "\n")
	defer clear(encoded)
	if _, err := f.Write(encoded); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// Publish only the complete key, without overwriting a concurrent setup's key.
	if err := os.Link(f.Name(), secretFile); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create session key: %w", err)
		}
		password, err := readSecretFile(secretFile)
		clear(password)
		return err
	}
	d, err := os.Open(parent)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
