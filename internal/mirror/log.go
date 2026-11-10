package mirror

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
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

type createdItem struct {
	kind, account string
	rowID         int64
}

func (l *syncLog) created(items []createdItem) {
	if len(items) == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, item := range items {
		line := fmt.Sprintf("%s stored %s version=%d account=%q", time.Now().UTC().Format(time.RFC3339Nano), strings.TrimSuffix(item.kind, "s"), item.rowID, item.account)
		l.append(line)
		fmt.Fprintln(os.Stderr, line)
	}
	// Diagnostics must not turn an already committed source transaction into a
	// failed ingestion. Keep console logging if the retained file cannot be saved.
	if err := l.save(); err != nil {
		if !l.reported {
			fmt.Fprintf(os.Stderr, "cannot update sync log: %v\n", err)
		}
		l.reported = true
	} else {
		l.reported = false
	}
}

// Source transactions report only newly inserted rows, after a successful
// commit. An upsert that merely refreshes observation time produces no entry.
func (e *Engine) archiveTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	if e.Log == nil {
		return e.DB.Transaction(ctx, fn)
	}
	var items []createdItem
	err := e.DB.Transaction(ctx, func(tx *sql.Tx) error {
		var before int64
		if err := tx.QueryRowContext(ctx, `SELECT coalesce(max(version_id),0) FROM source_versions`).Scan(&before); err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT kind,version_id,account_id FROM source_versions WHERE version_id>? AND kind IN ('threads','messages','users','deltas') ORDER BY version_id`, before)
		if err != nil {
			return err
		}
		for rows.Next() {
			var item createdItem
			if err = rows.Scan(&item.kind, &item.rowID, &item.account); err != nil {
				rows.Close()
				return err
			}
			items = append(items, item)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		e.Log.created(items)
	}
	return err
}
