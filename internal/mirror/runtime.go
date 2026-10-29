package mirror

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"go.mau.fi/mautrix-meta/internal/store"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-meta/pkg/instameow"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

func newClient(c *cookies.Cookies) *instameow.Client {
	return instameow.NewClient(instameow.ClientParams{
		Cookies: c, Log: zerolog.New(os.Stderr).Level(zerolog.ErrorLevel), DisableTyping: true,
	})
}

func Run(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "help" {
		fmt.Fprintln(os.Stdout, "igdm-mirror {login|sync --once|sync --follow|serve|status}")
		return nil
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	defaultDir := os.Getenv("IGDM_DATA_DIR")
	if defaultDir == "" {
		base, err := os.UserConfigDir()
		if err != nil {
			return err
		}
		defaultDir = filepath.Join(base, "igdm-mirror")
	}
	dir := fs.String("data-dir", defaultDir, "private data directory")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	db, err := store.Open(filepath.Join(*dir, "mirror.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	client := newClient(&cookies.Cookies{Platform: types.Instagram})
	defer client.Disconnect()
	if err := ctx.Err(); err != nil {
		return err
	}
	switch args[0] {
	case "status":
		status, err := db.Status(ctx, "")
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"authenticated": client.IsAuthenticated(), "sync": status})
	case "login", "sync", "serve":
		return fmt.Errorf("%s requires persistent account storage, which is not configured", args[0])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
