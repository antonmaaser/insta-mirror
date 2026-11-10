package mirror

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"database/sql"
	"go.mau.fi/mautrix-meta/internal/store"
)

const syncLogLimit = 1000

type syncLog struct {
	mu       sync.Mutex
	path     string
	lines    [syncLogLimit]string
	next     int
	count    int
	reported bool
}

func openSyncLog(path string) (*syncLog, error) {
	l := &syncLog{path: path}
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("sync log must be a regular file")
		}
		if err := os.Chmod(path, 0600); err != nil {
			return nil, err
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			l.append(scanner.Text())
		}
		err = scanner.Err()
		closeErr := f.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if err := l.save(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *syncLog) append(line string) {
	l.lines[l.next] = line
	l.next = (l.next + 1) % syncLogLimit
	if l.count < syncLogLimit {
		l.count++
	}
}

func (l *syncLog) save() error {
	f, err := os.CreateTemp(filepath.Dir(l.path), ".sync-log-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	w := bufio.NewWriter(f)
	for i := 0; i < l.count; i++ {
		if _, err := fmt.Fprintln(w, l.lines[(l.next-l.count+i+syncLogLimit)%syncLogLimit]); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), l.path)
}

func (l *syncLog) decisions(account, acquisition string, items []store.Decision) {
	if l == nil || len(items) == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, item := range items {
		line := fmt.Sprintf("%s %s %s local_id=%d account=%q context=%q fields=%q reason=%q", time.Now().UTC().Format(time.RFC3339Nano), item.Action, item.Kind, item.ID, account, acquisition, item.Fields, item.Reason)
		l.append(line)
		fmt.Fprintln(os.Stderr, line)
	}
	if err := l.save(); err != nil {
		if !l.reported {
			fmt.Fprintln(os.Stderr, "cannot update sync log")
		}
		l.reported = true
	} else {
		l.reported = false
	}
}

func (e *Engine) ingestTransaction(ctx context.Context, acquisition string, apply func(*ingestion) error) error {
	b := &ingestion{ctx: ctx, account: e.Account}
	err := e.DB.Transaction(ctx, func(tx *sql.Tx) error {
		b.tx = tx
		return apply(b)
	})
	if err == nil {
		e.Log.decisions(e.Account, acquisition, b.decisions)
		return nil
	}
	var rolledBack []store.Decision
	for _, d := range b.decisions {
		if d.Action == "conflict" || d.Action == "deferred" {
			rolledBack = append(rolledBack, d)
		}
	}
	rolledBack = append(rolledBack, store.Decision{Kind: "acquisition", Action: "rolled_back", Reason: err.Error()})
	e.Log.decisions(e.Account, acquisition, rolledBack)
	return err
}
