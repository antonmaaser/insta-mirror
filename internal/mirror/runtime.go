package mirror

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

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
	client := newClient(&cookies.Cookies{Platform: types.Instagram})
	defer client.Disconnect()
	if err := ctx.Err(); err != nil {
		return err
	}
	switch args[0] {
	case "status":
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"authenticated": client.IsAuthenticated()})
	case "login", "sync", "serve":
		return fmt.Errorf("%s requires persistent account storage, which is not configured", args[0])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
