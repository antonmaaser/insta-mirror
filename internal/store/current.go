package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const currentSchema = `
CREATE TABLE users (
 id INTEGER PRIMARY KEY, account_id TEXT NOT NULL, username TEXT, name TEXT, avatar_url TEXT,
 verified INTEGER, restricted INTEGER, blocking INTEGER, following INTEGER, followed_by INTEGER,
 ai_agent_type TEXT, latest_reel INTEGER, latest_besties_reel INTEGER, reel_seen_at INTEGER,
 last_event INTEGER NOT NULL DEFAULT 0, UNIQUE(account_id,id)
);
CREATE TABLE threads (
 id INTEGER PRIMARY KEY, account_id TEXT NOT NULL, title TEXT, image_url TEXT, folder TEXT,
 activity_at INTEGER, subtype TEXT, is_group INTEGER, muted INTEGER, pinned INTEGER,
 unread INTEGER, read_at INTEGER, input_mode INTEGER, reachability TEXT, deleted INTEGER,
 last_event INTEGER NOT NULL DEFAULT 0, UNIQUE(account_id,id)
);
CREATE TABLE messages (
 id INTEGER PRIMARY KEY, account_id TEXT NOT NULL, thread_id INTEGER, sender_id INTEGER, reply_id INTEGER,
 sent_at INTEGER, body TEXT, kind TEXT, snippet TEXT, attachments TEXT, edits TEXT,
 edited_at INTEGER, expires_at INTEGER, view_expires_at INTEGER, forwarded INTEGER,
 pinned INTEGER, reported INTEGER, ai_generated INTEGER, deleted INTEGER, placeholder INTEGER NOT NULL DEFAULT 1,
 last_event INTEGER NOT NULL DEFAULT 0, UNIQUE(account_id,id),
 FOREIGN KEY(account_id,thread_id) REFERENCES threads(account_id,id),
 FOREIGN KEY(account_id,sender_id) REFERENCES users(account_id,id),
 FOREIGN KEY(account_id,reply_id) REFERENCES messages(account_id,id)
);
CREATE INDEX messages_history ON messages(thread_id,sent_at,id) WHERE placeholder=0;
CREATE INDEX threads_activity ON threads(account_id,activity_at,id);
CREATE TABLE source_ids (
 account_id TEXT NOT NULL, namespace TEXT NOT NULL, scope TEXT NOT NULL DEFAULT '', remote_id TEXT NOT NULL,
 user_id INTEGER, thread_id INTEGER, message_id INTEGER,
 PRIMARY KEY(account_id,namespace,scope,remote_id),
 CHECK((user_id IS NOT NULL)+(thread_id IS NOT NULL)+(message_id IS NOT NULL)=1),
 FOREIGN KEY(account_id,user_id) REFERENCES users(account_id,id),
 FOREIGN KEY(account_id,thread_id) REFERENCES threads(account_id,id),
 FOREIGN KEY(account_id,message_id) REFERENCES messages(account_id,id)
) WITHOUT ROWID;
CREATE INDEX ids_thread ON source_ids(account_id,thread_id,namespace) WHERE thread_id IS NOT NULL;
CREATE TABLE thread_members (
 thread_id INTEGER NOT NULL REFERENCES threads(id), user_id INTEGER NOT NULL REFERENCES users(id),
 member INTEGER, nickname TEXT, admin INTEGER, read_at INTEGER,
 PRIMARY KEY(thread_id,user_id)
) WITHOUT ROWID;
CREATE TABLE message_reactions (
 message_id INTEGER NOT NULL REFERENCES messages(id), user_id INTEGER NOT NULL REFERENCES users(id),
 reaction TEXT NOT NULL, reacted_at INTEGER, log_message_id INTEGER REFERENCES messages(id),
 PRIMARY KEY(message_id,user_id)
) WITHOUT ROWID;
CREATE TABLE message_mentions (
 message_id INTEGER NOT NULL REFERENCES messages(id), offset INTEGER NOT NULL, length INTEGER NOT NULL,
 kind TEXT NOT NULL, user_id INTEGER REFERENCES users(id), thread_id INTEGER REFERENCES threads(id),
 PRIMARY KEY(message_id,offset,length,kind)
) WITHOUT ROWID;
CREATE TABLE sync_state (
 account_id TEXT NOT NULL, scope TEXT NOT NULL, scope_id TEXT NOT NULL, state TEXT NOT NULL CHECK(json_valid(state)),
 PRIMARY KEY(account_id,scope,scope_id)
) WITHOUT ROWID;
CREATE TABLE media_cache (
 account_id TEXT NOT NULL, message_id INTEGER NOT NULL, attachment TEXT NOT NULL, url_hash TEXT NOT NULL,
 status TEXT NOT NULL, hash TEXT NOT NULL, mime TEXT NOT NULL, size INTEGER NOT NULL,
 attempted INTEGER NOT NULL, attempts INTEGER NOT NULL, error TEXT NOT NULL,
 PRIMARY KEY(account_id,message_id,attachment),
 FOREIGN KEY(account_id,message_id) REFERENCES messages(account_id,id) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE INDEX media_hash ON media_cache(account_id,hash) WHERE hash!='';
`

type Alias struct {
	Namespace, Scope, RemoteID string
}

type Decision struct {
	Kind, Action, Reason string
	ID                   int64
	Fields               []string
}

type Attachment struct {
	Key        string   `json:"key"`
	Kind       string   `json:"kind,omitempty"`
	URLs       []string `json:"urls,omitempty"`
	Restricted bool     `json:"restricted,omitempty"`
}

type Edit struct {
	Body string `json:"body"`
	At   int64  `json:"at"`
}

var ErrDeferred = errors.New("identity needs a canonical source ID or a known alias")
var ErrConflict = errors.New("source IDs disagree about local identity")

func aliasColumn(kind string) string {
	switch kind {
	case "users":
		return "user_id"
	case "threads":
		return "thread_id"
	case "messages":
		return "message_id"
	default:
		panic("invalid entity kind")
	}
}

func aliasOwner(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, account, kind string, a Alias) (int64, error) {
	var id sql.NullInt64
	err := q.QueryRowContext(ctx, "SELECT "+aliasColumn(kind)+" FROM source_ids WHERE account_id=? AND namespace=? AND scope=? AND remote_id=?", account, a.Namespace, a.Scope, a.RemoteID).Scan(&id)
	if err == nil && !id.Valid {
		return 0, ErrConflict
	}
	return id.Int64, err
}

// ResolveAliases binds only IDs supplied together for the same source object.
// The caller rolls back the whole acquisition if any binding disagrees.
func ResolveAliases(ctx context.Context, tx *sql.Tx, account, kind string, aliases []Alias, canonical, expected int64) (int64, bool, bool, error) {
	id := expected
	for _, a := range aliases {
		owner, err := aliasOwner(ctx, tx, account, kind, a)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return id, false, false, err
		}
		if id != 0 && id != owner {
			return id, false, false, ErrConflict
		}
		id = owner
	}
	if canonical >= 0 && canonical < int64(len(aliases)) && id != 0 {
		a := aliases[canonical]
		var other string
		err := tx.QueryRowContext(ctx, "SELECT remote_id FROM source_ids WHERE account_id=? AND "+aliasColumn(kind)+"=? AND namespace=? AND scope=? AND remote_id<>? LIMIT 1", account, id, a.Namespace, a.Scope, a.RemoteID).Scan(&other)
		if err == nil {
			return id, false, false, ErrConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return id, false, false, err
		}
	}
	created := id == 0
	if created {
		if canonical < 0 || canonical >= int64(len(aliases)) {
			return 0, false, false, ErrDeferred
		}
		result, err := tx.ExecContext(ctx, "INSERT INTO "+kind+"(account_id) VALUES(?)", account)
		if err != nil {
			return 0, false, false, err
		}
		id, err = result.LastInsertId()
		if err != nil {
			return 0, false, false, err
		}
	}
	bound := false
	for _, a := range aliases {
		result, err := tx.ExecContext(ctx, "INSERT INTO source_ids(account_id,namespace,scope,remote_id,"+aliasColumn(kind)+") VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING", account, a.Namespace, a.Scope, a.RemoteID, id)
		if err != nil {
			return id, created, false, err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return id, created, false, err
		}
		bound = bound || n > 0
	}
	return id, created, bound, nil
}

// Patch writes only supplied fields with different values. Field names come
// from the ingestion code, never from keys supplied by Instagram.
func Patch(ctx context.Context, tx *sql.Tx, table, where string, keys []any, values map[string]any, fill bool, event int64) ([]string, error) {
	var columns []string
	for name := range values {
		columns = append(columns, name)
	}
	if len(columns) == 0 {
		return nil, nil
	}
	sort.Strings(columns)
	current := make([]any, len(columns))
	dest := make([]any, len(columns))
	for i := range dest {
		dest[i] = &current[i]
	}
	if err := tx.QueryRowContext(ctx, "SELECT "+strings.Join(columns, ",")+" FROM "+table+" WHERE "+where, keys...).Scan(dest...); err != nil {
		return nil, err
	}
	var changed, set []string
	var args []any
	for i, name := range columns {
		value := values[name]
		if fill && current[i] != nil || current[i] == value {
			continue
		}
		changed = append(changed, name)
		set = append(set, name+"=?")
		args = append(args, value)
	}
	if len(set) == 0 {
		return nil, nil
	}
	if event > 0 {
		set = append(set, "last_event=?")
		args = append(args, event)
	}
	_, err := tx.ExecContext(ctx, "UPDATE "+table+" SET "+strings.Join(set, ",")+" WHERE "+where, append(args, keys...)...)
	return changed, err
}

func JSONValue(value any) string {
	b, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("invalid stored value: %v", err))
	}
	return string(b)
}
