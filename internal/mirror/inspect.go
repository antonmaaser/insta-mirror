package mirror

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"go.mau.fi/mautrix-meta/internal/projection"
	"go.mau.fi/mautrix-meta/internal/store"
)

func runCommand(ctx context.Context, args []string, defaultDir string) error {
	if len(args) > 0 && (args[0] == "help" || args[0] == "--help") {
		fmt.Fprintln(os.Stdout, "igdm-mirror cmd [--data-dir DIR | --db FILE] [--account ID] [--export-dir DIR]\n  Interactive: ↑/↓ select, → inspect, ← chats, e export, q/Esc quit\n  Direct: cmd chats [--limit 30] [--cursor CURSOR]\n          cmd export --chat LOCAL_ID --output FILE")
		return nil
	}
	action := "ui"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		action = args[0]
		args = args[1:]
	}
	if action != "ui" && action != "chats" && action != "export" {
		return fmt.Errorf("unknown cmd action %q", action)
	}
	fs := flag.NewFlagSet("cmd "+action, flag.ContinueOnError)
	dir := fs.String("data-dir", defaultDir, "mirror data directory")
	path := fs.String("db", "", "existing mirror SQLite file; overrides --data-dir")
	account := fs.String("account", "", "numeric source account ID; inferred only if exactly one is archived")
	var chat, output, cursor string
	var limit int
	exportDir := ""
	if action == "ui" {
		fs.StringVar(&exportDir, "export-dir", "", "transcript directory; defaults to exports/ beside the database")
	} else if action == "chats" {
		fs.IntVar(&limit, "limit", 30, "maximum chats, 1 through 100")
		fs.StringVar(&cursor, "cursor", "", "next cursor from a previous listing")
	} else {
		fs.StringVar(&chat, "chat", "", "local thread ID from cmd chats")
		fs.StringVar(&output, "output", "", "new transcript file; existing files are never replaced")
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected cmd arguments")
	}
	if action == "chats" && (limit < 1 || limit > 100) {
		return errors.New("limit must be 1 through 100")
	}
	if action == "export" && (chat == "" || output == "") {
		return errors.New("export requires --chat and --output")
	}
	if *path == "" {
		if *dir == "" {
			return errors.New("data directory must not be empty")
		}
		*path = filepath.Join(*dir, "mirror.db")
	}
	db, err := store.OpenReadOnly(ctx, *path)
	if err != nil {
		return err
	}
	defer db.Close()
	err = db.ReadSnapshot(ctx, func(snapshot *store.Store) error {
		accounts, err := snapshot.Accounts(ctx)
		if err != nil {
			return err
		}
		if len(accounts) == 0 {
			return errors.New("no locally mirrored accounts; acquire data with sync first")
		}
		if *account == "" {
			if len(accounts) != 1 {
				return fmt.Errorf("select --account from the locally mirrored accounts: %s", strings.Join(accounts, ", "))
			}
			*account = accounts[0]
		}
		found := false
		for _, id := range accounts {
			if id == *account {
				found = true
				break
			}
		}
		if !found {
			return errors.New("account is not present in the mirror database")
		}
		return nil
	})
	if err != nil {
		return err
	}
	if action == "ui" {
		if exportDir == "" {
			exportDir = filepath.Join(filepath.Dir(*path), "exports")
		}
		return runTerminal(ctx, db, *path, *account, exportDir)
	}
	return db.ReadSnapshot(ctx, func(snapshot *store.Store) error {
		if action == "chats" {
			return inspectChats(ctx, snapshot, *account, cursor, limit)
		}
		chatRecord, err := selectedChat(ctx, snapshot, *account, chat)
		if err != nil {
			return err
		}
		if err = exportTranscript(ctx, snapshot, *path, chatRecord, output); err != nil {
			return err
		}
		target, err := filepath.Abs(output)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "Transcript exported: %s\n", target)
		return nil
	})
}
func inspectChats(ctx context.Context, db *store.Store, account, cursor string, limit int) error {
	page, err := db.Query(ctx, "threads", store.Query{Account: account, Cursor: cursor, Limit: limit})
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "Account: %s\nREFERENCE\tTITLE\tPARTICIPANTS\tLAST ACTIVITY (UTC)\n", account)
	for _, record := range page.Records {
		thread := record.Thread
		title := ""
		if thread.Title != nil {
			title = *thread.Title
		}
		var participants []string
		for i, member := range thread.Members {
			if i == 8 {
				participants = append(participants, "…")
				break
			}
			participants = append(participants, member.Name)
		}
		stamp := "unknown"
		if thread.ActivityAt != 0 {
			stamp = time.UnixMilli(thread.ActivityAt).UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(w, "%d\t%q\t%q\t%s\n", record.ID, title, strings.Join(participants, ", "), stamp)
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if page.Next != "" {
		fmt.Fprintf(os.Stdout, "Next cursor: %s\n", page.Next)
	}
	return nil
}
func exportTranscript(ctx context.Context, db *store.Store, dbPath string, chat store.Record, output string) error {
	target, err := filepath.Abs(output)
	if err != nil {
		return err
	}
	source, err := filepath.Abs(dbPath)
	if err != nil {
		return err
	}
	source, err = filepath.EvalSymlinks(source)
	if err != nil {
		return err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(target))
	if err != nil {
		return err
	}
	target = filepath.Join(parent, filepath.Base(target))
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if target == source+suffix {
			return errors.New("transcript output must not be a database or SQLite sidecar file")
		}
	}
	if _, err = os.Lstat(target); err == nil {
		return errors.New("transcript output already exists; choose a new file")
	} else if !os.IsNotExist(err) {
		return err
	}
	f, err := os.CreateTemp(parent, ".transcript-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	w := bufio.NewWriter(f)
	if err = projection.WriteTranscript(ctx, db, chat, w); err != nil {
		return err
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = os.Link(f.Name(), target); err != nil {
		return fmt.Errorf("publish transcript without replacing existing files: %w", err)
	}
	return nil
}

func selectedChat(ctx context.Context, db *store.Store, account, identity string) (store.Record, error) {
	page, err := db.Query(ctx, "threads", store.Query{Account: account, SourceID: identity, Limit: 2})
	if err != nil {
		return store.Record{}, err
	}
	if len(page.Records) != 1 {
		return store.Record{}, errors.New("chat reference is unavailable or ambiguous; use a local thread ID from cmd chats")
	}
	return page.Records[0], nil
}
