package auth

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"go.mau.fi/mautrix-meta/pkg/instameow"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"golang.org/x/crypto/scrypt"
	"golang.org/x/term"
)

var accountPattern = regexp.MustCompile(`^[0-9]+$`)

func ValidAccount(id string) bool { return accountPattern.MatchString(id) }

type Session struct {
	Account string           `json:"account"`
	Cookies *cookies.Cookies `json:"cookies"`
	State   json.RawMessage  `json:"state,omitempty"`
	Claim   string           `json:"claim,omitempty"`
}
type Vault struct {
	dir      string
	password []byte
	mu       sync.Mutex
}

func Prompt(label string, secret bool) (string, error) {
	f, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return "", errors.New("interactive terminal required; use a protected --secret-file for unattended unlocking")
	}
	defer f.Close()
	fmt.Fprint(f, label+": ")
	if secret {
		b, err := term.ReadPassword(int(f.Fd()))
		fmt.Fprintln(f)
		return string(b), err
	}
	var line []byte
	b := make([]byte, 1)
	for {
		n, err := f.Read(b)
		if n > 0 {
			if b[0] == '\n' {
				break
			}
			line = append(line, b[0])
			if len(line) > 4096 {
				return "", errors.New("input too long")
			}
		}
		if err != nil {
			return "", err
		}
	}
	return strings.TrimSpace(string(line)), nil
}
func Unlock(dir, secretFile string) (*Vault, error) {
	if err := os.MkdirAll(filepath.Join(dir, "sessions"), 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(filepath.Join(dir, "sessions"), 0700); err != nil {
		return nil, err
	}
	var password []byte
	if secretFile != "" {
		info, err := os.Lstat(secretFile)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return nil, errors.New("secret file must be a regular file accessible only by its owner")
		}
		if info.Size() > 4096 {
			return nil, errors.New("secret file too large")
		}
		password, err = os.ReadFile(secretFile)
		if err != nil {
			return nil, err
		}
		password = []byte(strings.TrimRight(string(password), "\r\n"))
	} else {
		v, err := Prompt("Session encryption passphrase", true)
		if err != nil {
			return nil, err
		}
		password = []byte(v)
	}
	if len(password) < 12 {
		return nil, errors.New("session passphrase must contain at least 12 bytes")
	}
	return &Vault{dir: filepath.Join(dir, "sessions"), password: password}, nil
}
func (v *Vault) Close() { clear(v.password) }
func aead(password, salt []byte) (cipher.AEAD, error) {
	key, err := scrypt.Key(password, salt, 32768, 8, 1, 32)
	if err != nil {
		return nil, err
	}
	defer clear(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
func (v *Vault) Save(session *Session) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !ValidAccount(session.Account) {
		return errors.New("invalid account ID")
	}
	raw, err := json.Marshal(session)
	if err != nil {
		return err
	}
	defer clear(raw)
	salt := make([]byte, 16)
	if _, err = rand.Read(salt); err != nil {
		return err
	}
	box, err := aead(v.password, salt)
	if err != nil {
		return err
	}
	nonce := make([]byte, box.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return err
	}
	header := append([]byte("IGDM1"), salt...)
	header = append(header, nonce...)
	encrypted := box.Seal(header, nonce, raw, []byte(session.Account))
	f, err := os.CreateTemp(v.dir, ".session-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(encrypted); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(v.dir, session.Account+".enc")); err != nil {
		return err
	}
	d, err := os.Open(v.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func (v *Vault) Load(account string) (*Session, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !ValidAccount(account) {
		return nil, errors.New("--account requires the numeric Instagram ds_user_id")
	}
	path := filepath.Join(v.dir, account+".enc")
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 16<<20 {
		return nil, errors.New("session file has unsafe permissions or size")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) < 49 || string(b[:5]) != "IGDM1" {
		return nil, errors.New("invalid encrypted session")
	}
	box, err := aead(v.password, b[5:21])
	if err != nil {
		return nil, err
	}
	raw, err := box.Open(nil, b[21:33], b[33:], []byte(account))
	if err != nil {
		return nil, errors.New("cannot decrypt session; check the passphrase")
	}
	defer clear(raw)
	s := &Session{Cookies: &cookies.Cookies{Platform: types.Instagram}}
	if err = json.Unmarshal(raw, s); err != nil {
		return nil, err
	}
	if s.Account != account || s.Cookies == nil || s.Cookies.Get(cookies.IGCookieDSUserID) != account {
		return nil, errors.New("session identity mismatch")
	}
	s.Cookies.Platform = types.Instagram
	s.Cookies.SetWWWClaim(s.Claim)
	return s, nil
}
func Login(ctx context.Context, client *instameow.Client, importCookies bool) (*Session, error) {
	if importCookies {
		raw, err := Prompt("Instagram cookies as a JSON object", true)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(raw), client.GetCookies()); err != nil {
			return nil, errors.New("cookies must be a JSON object mapping cookie names to values")
		}
	} else {
		username, err := Prompt("Instagram username", false)
		if err != nil {
			return nil, err
		}
		password, err := Prompt("Instagram password", true)
		if err != nil {
			return nil, err
		}
		challenge, err := client.CreateInstagramWebSession(ctx, username, password)
		password = ""
		if err != nil {
			return nil, errors.New("Instagram web login failed; complete any challenge in Instagram or use --cookies")
		}
		if challenge != nil {
			code, err := Prompt("Instagram verification code", true)
			if err != nil {
				return nil, err
			}
			if err = client.CompleteInstagramWebSessionTwoFactor(ctx, code); err != nil {
				return nil, errors.New("Instagram verification failed; complete the challenge in Instagram or use --cookies")
			}
		}
	}
	if len(client.GetCookies().GetMissingCookieNames()) > 0 {
		return nil, errors.New("session is missing required Instagram cookies")
	}
	if _, _, err := client.LoadIndex(ctx); err != nil {
		return nil, errors.New("cannot validate Instagram session; check the account challenge, cookies and connectivity")
	}
	account := client.GetCookies().Get(cookies.IGCookieDSUserID)
	if !ValidAccount(account) {
		return nil, errors.New("Instagram session has no numeric account identity")
	}
	state, err := client.DumpState()
	if err != nil {
		return nil, err
	}
	return &Session{Account: account, Cookies: client.GetCookies(), State: state, Claim: client.GetCookies().GetWWWClaim()}, nil
}
