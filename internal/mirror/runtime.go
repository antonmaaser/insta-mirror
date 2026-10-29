package mirror

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-meta/internal/auth"
	"go.mau.fi/mautrix-meta/internal/store"
	"go.mau.fi/mautrix-meta/pkg/instameow"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

func newClient(c *cookies.Cookies) *instameow.Client {
	return instameow.NewClient(instameow.ClientParams{Cookies: c, Log: zerolog.New(os.Stderr).Level(zerolog.Disabled), DisableTyping: true})
}
func Run(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "help" {
		fmt.Fprintln(os.Stdout, "igdm-mirror {login|sync --once|sync --follow|serve|status} [--data-dir DIR] [--account ID] [--secret-file FILE]")
		return nil
	}
	command := args[0]
	if command != "login" && command != "sync" && command != "serve" && command != "status" {
		return fmt.Errorf("unknown command %q", command)
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	defaultDir := os.Getenv("IGDM_DATA_DIR")
	if defaultDir == "" {
		base, err := os.UserConfigDir()
		if err != nil {
			return err
		}
		defaultDir = filepath.Join(base, "igdm-mirror")
	}
	dir := fs.String("data-dir", defaultDir, "private data directory")
	account := fs.String("account", "", "numeric Instagram account ID")
	secret := fs.String("secret-file", os.Getenv("IGDM_SECRET_FILE"), "owner-only session passphrase file")
	imported := fs.Bool("cookies", false, "import cookies interactively instead of password login")
	once := fs.Bool("once", false, "finish pending historical work and exit")
	follow := fs.Bool("follow", false, "maintain realtime synchronization")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	db, err := store.Open(filepath.Join(*dir, "mirror.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	if command == "status" {
		status, err := db.Status(ctx, *account)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"accounts_and_sync": status})
	}
	if command == "sync" && *once == *follow {
		return fmt.Errorf("sync requires exactly one of --once or --follow")
	}
	vault, err := auth.Unlock(*dir, *secret)
	if err != nil {
		return err
	}
	defer vault.Close()
	if command == "login" {
		c := newClient(&cookies.Cookies{Platform: types.Instagram})
		defer c.Disconnect()
		session, err := auth.Login(ctx, c, *imported)
		if err != nil {
			return err
		}
		if err = vault.Save(session); err != nil {
			return err
		}
		if err = db.SetState(ctx, session.Account, "account", "", map[string]any{"session_saved": true, "last_validated_ms": time.Now().UnixMilli()}); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"account_id": session.Account, "session_saved": true})
	}
	session, err := vault.Load(*account)
	if err != nil {
		return err
	}
	c := newClient(session.Cookies)
	defer c.Disconnect()
	if len(session.State) > 0 {
		if err = c.LoadState(session.State); err != nil && err != instameow.ErrCachedStateTooOld {
			return fmt.Errorf("invalid cached connection state")
		}
	}
	return fmt.Errorf("%s synchronization is not yet available", command)
}
