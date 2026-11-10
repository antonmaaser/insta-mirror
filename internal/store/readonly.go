package store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
)

// OpenReadOnly reads the live SQLite database, including its WAL. It does not
// initialize a schema, change permissions, or change the journal mode.
func OpenReadOnly(ctx context.Context, path string) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("mirror database must be a regular file")
	}
	u := url.URL{Scheme: "file", Path: abs}
	q := url.Values{"mode": {"ro"}, "_query_only": {"true"}, "_pragma": {"busy_timeout(10000)"}, "_txlock": {"deferred"}}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	var version int
	if err = db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		db.Close()
		return nil, err
	}
	if version != schemaVersion {
		db.Close()
		return nil, fmt.Errorf("incompatible mirror schema version %d; archive it and use a fresh data directory (no migration supported)", version)
	}
	return &Store{DB: db}, nil
}

// ReadSnapshot keeps entity, media and coverage reads on the same
// deferred read transaction. WAL writers can continue committing independently.
func (s *Store) ReadSnapshot(ctx context.Context, fn func(*Store) error) error {
	if s.readTx != nil {
		return fn(s)
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(&Store{DB: s.DB, readTx: tx})
}

func (s *Store) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if s.readTx != nil {
		return s.readTx.QueryContext(ctx, query, args...)
	}
	return s.DB.QueryContext(ctx, query, args...)
}

func (s *Store) queryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if s.readTx != nil {
		return s.readTx.QueryRowContext(ctx, query, args...)
	}
	return s.DB.QueryRowContext(ctx, query, args...)
}

func (s *Store) Accounts(ctx context.Context) ([]string, error) {
	rows, err := s.queryContext(ctx, `SELECT account_id FROM threads UNION SELECT account_id FROM messages UNION SELECT account_id FROM users UNION SELECT account_id FROM sync_state ORDER BY account_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var accounts []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		accounts = append(accounts, id)
	}
	return accounts, rows.Err()
}
