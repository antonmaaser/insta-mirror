package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-meta/internal/auth"
	"go.mau.fi/mautrix-meta/internal/mcp"
	"go.mau.fi/mautrix-meta/internal/media"
	"go.mau.fi/mautrix-meta/internal/store"
	"go.mau.fi/mautrix-meta/pkg/instameow"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

func newClient(c *cookies.Cookies) *instameow.Client {
	return instameow.NewClient(instameow.ClientParams{Cookies: c, Log: zerolog.New(os.Stderr).Level(zerolog.Disabled), DisableTyping: true, RawGraphQLResponse: captureGraphQL})
}
func Run(ctx context.Context, args []string) (runErr error) {
	if len(args) == 0 || args[0] == "--help" || args[0] == "help" {
		fmt.Fprintln(os.Stdout, "igdm-mirror {setup|login|sync --once|sync --follow|serve|status} [--data-dir DIR] [--account ID] [--secret-file FILE]")
		fmt.Fprintln(os.Stdout, "Run setup before login. Defaults: data/ and session.key beside the executable; override with IGDM_DATA_DIR and IGDM_SECRET_FILE.")
		return nil
	}
	command := args[0]
	if command != "setup" && command != "login" && command != "sync" && command != "serve" && command != "status" {
		return fmt.Errorf("unknown command %q", command)
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	defaultDir := os.Getenv("IGDM_DATA_DIR")
	defaultSecret := os.Getenv("IGDM_SECRET_FILE")
	if defaultDir == "" || defaultSecret == "" {
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		executable, err = filepath.EvalSymlinks(executable)
		if err != nil {
			return err
		}
		base := filepath.Dir(executable)
		if defaultDir == "" {
			defaultDir = filepath.Join(base, "data")
		}
		if defaultSecret == "" {
			defaultSecret = filepath.Join(base, "session.key")
		}
	}
	dir := fs.String("data-dir", defaultDir, "private data directory")
	account := fs.String("account", "", "numeric Instagram account ID")
	secret := fs.String("secret-file", defaultSecret, "owner-only session passphrase file")
	imported := fs.Bool("cookies", false, "import cookies interactively instead of password login")
	once := fs.Bool("once", false, "finish pending historical work and exit")
	follow := fs.Bool("follow", false, "maintain realtime synchronization")
	httpAddress := fs.String("http", "", "optional MCP HTTP listen address, e.g. 127.0.0.1:8080")
	httpToken := fs.String("http-token-file", "", "owner-only MCP bearer token file")
	allowNetwork := fs.Bool("allow-network", false, "allow explicit HTTP network exposure")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	if *dir == "" {
		return fmt.Errorf("data directory must not be empty")
	}
	if command == "setup" {
		if err := auth.Setup(*dir, *secret); err != nil {
			return err
		}
	}
	db, err := store.Open(filepath.Join(*dir, "mirror.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	if command == "setup" {
		mediaDir := filepath.Join(*dir, "media")
		if err := os.MkdirAll(mediaDir, 0700); err != nil {
			return err
		}
		if err := os.Chmod(mediaDir, 0700); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"ready_for_login": true, "data_dir": *dir, "secret_file": *secret})
	}
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
	if *account == "" {
		files, err := filepath.Glob(filepath.Join(*dir, "sessions", "*.enc"))
		if err != nil {
			return err
		}
		var ids []string
		for _, file := range files {
			id := strings.TrimSuffix(filepath.Base(file), ".enc")
			if auth.ValidAccount(id) {
				ids = append(ids, id)
			}
		}
		if len(ids) != 1 {
			return fmt.Errorf("select an account with --account; found %d saved sessions", len(ids))
		}
		*account = ids[0]
	}
	session, err := vault.Load(*account)
	if err != nil {
		return err
	}
	cache, err := media.New(db, filepath.Join(*dir, "media"))
	if err != nil {
		return err
	}
	defer cache.Close()
	workCtx, abort := context.WithCancelCause(ctx)
	defer abort(context.Canceled)
	mediaDone := make(chan struct{})
	go func() {
		defer close(mediaDone)
		for {
			if err := cache.Sync(workCtx, session.Account); err != nil && workCtx.Err() == nil {
				_ = db.SetState(ctx, session.Account, "account", "media", store.State{Error: "media acquisition failed; retry pending"})
			}
			timer := time.NewTimer(time.Minute)
			select {
			case <-workCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	defer func() { abort(context.Canceled); <-mediaDone }()
	e := &Engine{DB: db, Account: session.Account, Abort: abort, Reconcile: make(chan struct{}, 1)}
	c := instameow.NewClient(instameow.ClientParams{Cookies: session.Cookies, Log: zerolog.New(os.Stderr).Level(zerolog.Disabled), DisableTyping: true, RawGraphQLResponse: captureGraphQL, RawDeltaResponse: e.captureDeltas, EventHandler: e.event})
	e.Client = c
	defer func() {
		c.Disconnect()
		state, err := c.DumpState()
		if err == nil {
			session.State = state
			session.Claim = session.Cookies.GetWWWClaim()
			err = vault.Save(session)
		}
		if runErr == nil && err != nil {
			runErr = err
		}
	}()
	e.SaveConnection = func(state json.RawMessage) error {
		session.State = state
		session.Claim = session.Cookies.GetWWWClaim()
		return vault.Save(session)
	}
	if len(session.State) > 0 {
		if err = c.LoadState(session.State); err != nil && !errors.Is(err, instameow.ErrCachedStateTooOld) {
			return fmt.Errorf("invalid cached connection state")
		}
	}
	if command == "serve" {
		done := make(chan error, 1)
		go func() {
			syncErr := e.Initialize(workCtx)
			if syncErr == nil {
				syncErr = e.PrepareRealtime(workCtx)
			}
			if syncErr == nil {
				syncErr = e.Follow(workCtx)
			}
			if syncErr != nil && workCtx.Err() == nil {
				_ = db.SetState(ctx, session.Account, "reconciliation", "runtime", store.State{Error: "synchronization stopped; inspect coverage and authentication"})
				fmt.Fprintln(os.Stderr, "synchronization stopped; inspect status")
			}
			done <- syncErr
		}()
		serveErr := mcp.New(db, session.Account, cache).Run(ctx, *httpAddress, *httpToken, *allowNetwork)
		abort(context.Canceled)
		c.Disconnect()
		<-done
		return serveErr
	}
	if err = e.Initialize(workCtx); err != nil {
		return err
	}
	if *follow {
		if err = e.PrepareRealtime(workCtx); err != nil {
			return err
		}
		return e.Follow(workCtx)
	}
	historyErr := e.History(workCtx)
	abort(context.Canceled)
	<-mediaDone
	if historyErr != nil {
		return historyErr
	}
	return cache.Sync(ctx, session.Account)
}
