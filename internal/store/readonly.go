package store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
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

// ReadSnapshot keeps all source, delta, media and coverage reads on the same
// deferred read transaction. WAL writers can continue committing independently.
func (s *Store) ReadSnapshot(ctx context.Context, fn func(*Store) error) error {
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
	rows, err := s.queryContext(ctx, `SELECT account_id FROM threads_sources UNION SELECT account_id FROM messages_sources UNION SELECT account_id FROM users_sources UNION SELECT account_id FROM deltas_sources UNION SELECT account_id FROM sync_state ORDER BY account_id`)
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

// TranscriptMessages streams the latest stored snapshot per source identity in
// source timestamp order. A read snapshot is required for nested projections.
func (s *Store) TranscriptMessages(ctx context.Context, account, thread string, visit func(Record) error) error {
	if s.readTx == nil {
		return fmt.Errorf("transcript requires a read snapshot")
	}
	refs, err := s.ThreadReferences(ctx, account, thread)
	if err != nil {
		return err
	}
	marks := strings.TrimSuffix(strings.Repeat("?,", len(refs)), ",")
	args := []any{account}
	for range 2 {
		for _, ref := range refs {
			args = append(args, ref)
		}
	}
	rows, err := s.queryContext(ctx, `WITH ranked AS (SELECT *,row_number() OVER(PARTITION BY source_identity ORDER BY last_observed_at_ms DESC,record_id DESC) AS rank FROM messages_sources WHERE account_id=?) SELECT record_id,source_identity,acquired_thread_id,origin,observed_at_ms,last_observed_at_ms,source_hash,version_id,content_hash FROM ranked WHERE rank=1 AND (thread_fbid IN (`+marks+`) OR EXISTS (SELECT 1 FROM source_evidence ev JOIN source_observations a ON a.observation_id=ev.observation_id WHERE ev.version_id=ranked.record_id AND a.acquired_thread_id IN (`+marks+`))) ORDER BY (timestamp_ms IS NULL OR timestamp_ms=''),CAST(timestamp_ms AS INTEGER),record_id`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		r := Record{Table: "messages", Account: account}
		if err = rows.Scan(&r.RowID, &r.Identity, &r.Thread, &r.Origin, &r.Observed, &r.LastObserved, &r.Hash, &r.Version, &r.ContentHash); err != nil {
			return err
		}
		if err = s.hydrate(ctx, &r); err != nil {
			return err
		}
		if err = visit(r); err != nil {
			return err
		}
	}
	return rows.Err()
}
