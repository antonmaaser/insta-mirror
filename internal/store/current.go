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

const currentSchema = `CREATE TABLE accounts (
    account_id      TEXT PRIMARY KEY,
    self_user_id    INTEGER,
    created_at_ms   INTEGER NOT NULL,
    updated_at_ms   INTEGER NOT NULL,
    FOREIGN KEY(account_id,self_user_id) REFERENCES users(account_id,user_id)
) WITHOUT ROWID;

CREATE TABLE media_assets (
    account_id      TEXT NOT NULL,
    asset_id        INTEGER PRIMARY KEY,
    canonical_key   TEXT NOT NULL,          -- Priority: 'fbid:<id>' | 'ig_cache:<key>' | 'path_sha256:<hex>'
    asset_kind      TEXT NOT NULL,          -- 'AVATAR' | 'IMAGE' | 'VIDEO' | 'AUDIO' | 'XMA_PREVIEW'
    download_status TEXT NOT NULL DEFAULT 'pending', -- 'pending' | 'available' | 'unavailable'
    content_sha256  TEXT,
    mime_type       TEXT,
    byte_size       INTEGER,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_attempt_ms INTEGER,
    last_error      TEXT,
    UNIQUE(account_id, canonical_key),
    UNIQUE(account_id, asset_id),
    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE INDEX idx_media_assets_content_hash
    ON media_assets(account_id, content_sha256) WHERE content_sha256 IS NOT NULL;

CREATE INDEX idx_media_assets_pending
    ON media_assets(account_id, download_status, attempts) WHERE download_status = 'pending';

CREATE TABLE media_asset_urls (
    account_id      TEXT NOT NULL,
    asset_id        INTEGER NOT NULL,
    url_role        TEXT NOT NULL DEFAULT 'primary', -- 'primary' | 'fallback'
    url             TEXT NOT NULL,
    expires_at_ms   INTEGER,
    observed_at_ms  INTEGER NOT NULL,
    PRIMARY KEY (account_id, asset_id, url_role),
    FOREIGN KEY (account_id, asset_id) REFERENCES media_assets(account_id, asset_id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE TABLE users (
    account_id          TEXT NOT NULL,
    user_id             INTEGER PRIMARY KEY,
    username            TEXT,
    full_name           TEXT,
    avatar_asset_id     INTEGER,
    verified            INTEGER NOT NULL DEFAULT 0,
    restricted          INTEGER NOT NULL DEFAULT 0,
    blocking            INTEGER NOT NULL DEFAULT 0,
    following           INTEGER NOT NULL DEFAULT 0,
    followed_by         INTEGER NOT NULL DEFAULT 0,
    ai_agent_type       TEXT,
    latest_reel_at      INTEGER,
    latest_besties_at   INTEGER,
    reel_seen_at        INTEGER,
    source_tier         INTEGER NOT NULL DEFAULT 1, -- 1=stub, 2=delta/snippet, 3=slide, 4=graphql_full
    fingerprint         BLOB NOT NULL,
    updated_at_ms       INTEGER NOT NULL,
    UNIQUE(account_id, user_id),
    FOREIGN KEY (account_id, avatar_asset_id) REFERENCES media_assets(account_id, asset_id),
    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE TABLE user_identities (
    account_id      TEXT NOT NULL,
    namespace       TEXT NOT NULL,          -- 'igid' | 'messaging_fbid' | 'fbid_v2'
    remote_id       TEXT NOT NULL,
    user_id         INTEGER NOT NULL,
    PRIMARY KEY (account_id, namespace, remote_id),
    FOREIGN KEY (account_id, user_id) REFERENCES users(account_id, user_id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX idx_user_identities_user ON user_identities(account_id, user_id);

CREATE TABLE threads (
    account_id      TEXT NOT NULL,
    thread_id       INTEGER PRIMARY KEY,
    title           TEXT,
    image_asset_id  INTEGER,
    folder          TEXT NOT NULL DEFAULT 'INBOX',
    subtype         TEXT,
    is_group        INTEGER NOT NULL DEFAULT 0,
    muted           INTEGER NOT NULL DEFAULT 0,
    pinned          INTEGER NOT NULL DEFAULT 0,
    unread          INTEGER NOT NULL DEFAULT 0,
    read_at_ms      INTEGER,
    activity_at_ms  INTEGER,
    input_mode      INTEGER,
    reachability    TEXT,
    capabilities    TEXT,
    deleted         INTEGER NOT NULL DEFAULT 0,
    source_tier     INTEGER NOT NULL DEFAULT 1,
    fingerprint     BLOB NOT NULL,
    updated_at_ms   INTEGER NOT NULL,
    UNIQUE(account_id, thread_id),
    FOREIGN KEY (account_id, image_asset_id) REFERENCES media_assets(account_id, asset_id),
    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE INDEX idx_threads_inbox
    ON threads(account_id, folder, deleted, activity_at_ms DESC, thread_id);

CREATE TABLE thread_identities (
    account_id      TEXT NOT NULL,
    namespace       TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT '',
    remote_id       TEXT NOT NULL,
    thread_id       INTEGER NOT NULL,
    PRIMARY KEY (account_id, namespace, scope, remote_id),
    FOREIGN KEY (account_id, thread_id) REFERENCES threads(account_id, thread_id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX idx_thread_identities_thread ON thread_identities(account_id, thread_id);

CREATE TABLE thread_members (
    account_id      TEXT NOT NULL,
    thread_id       INTEGER NOT NULL,
    user_id         INTEGER NOT NULL,
    is_active       INTEGER NOT NULL DEFAULT 1,
    is_admin        INTEGER NOT NULL DEFAULT 0,
    nickname        TEXT,
    read_at_ms      INTEGER,
    fingerprint     BLOB NOT NULL,
    PRIMARY KEY (account_id, thread_id, user_id),
    FOREIGN KEY (account_id, thread_id) REFERENCES threads(account_id, thread_id) ON DELETE CASCADE,
    FOREIGN KEY (account_id, user_id) REFERENCES users(account_id, user_id)
) WITHOUT ROWID;

CREATE TABLE messages (
    account_id          TEXT NOT NULL,
    message_id          INTEGER PRIMARY KEY,
    thread_id           INTEGER,
    sender_id           INTEGER,
    reply_to_message_id INTEGER,
    sent_at_ms          INTEGER,
    edited_at_ms        INTEGER,
    kind                TEXT,
    body                TEXT,
    snippet             TEXT,
    expires_at_ms       INTEGER,
    view_expires_at_ms  INTEGER,
    forwarded           INTEGER NOT NULL DEFAULT 0,
    pinned              INTEGER NOT NULL DEFAULT 0,
    reported            INTEGER NOT NULL DEFAULT 0,
    ai_generated        INTEGER NOT NULL DEFAULT 0,
    bot_response_id     TEXT,
    deleted             INTEGER NOT NULL DEFAULT 0,
    tombstone_reason    TEXT,
    is_placeholder      INTEGER NOT NULL DEFAULT 1,
    source_tier         INTEGER NOT NULL DEFAULT 1,
    fingerprint         BLOB NOT NULL,
    updated_at_ms       INTEGER NOT NULL,
    CHECK (is_placeholder = 1 OR (thread_id IS NOT NULL AND sent_at_ms IS NOT NULL)),
    UNIQUE(account_id, message_id),
    FOREIGN KEY (account_id, thread_id) REFERENCES threads(account_id, thread_id),
    FOREIGN KEY (account_id, sender_id) REFERENCES users(account_id, user_id),
    FOREIGN KEY (account_id, reply_to_message_id) REFERENCES messages(account_id, message_id),
    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);

CREATE INDEX idx_messages_thread_history
    ON messages(account_id, thread_id, sent_at_ms DESC, message_id) WHERE is_placeholder = 0;

CREATE TABLE message_identities (
    account_id      TEXT NOT NULL,
    namespace       TEXT NOT NULL,          -- 'mid' | 'otid' | 'item_id'
    remote_id       TEXT NOT NULL,
    message_id      INTEGER NOT NULL,
    PRIMARY KEY (account_id, namespace, remote_id),
    FOREIGN KEY (account_id, message_id) REFERENCES messages(account_id, message_id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX idx_message_identities_msg ON message_identities(account_id, message_id);

CREATE TABLE message_attachments (
    account_id          TEXT NOT NULL,
    message_id          INTEGER NOT NULL,
    ordinal             INTEGER NOT NULL,
    asset_id            INTEGER NOT NULL,
    attachment_kind     TEXT NOT NULL,
    remote_entity_id    TEXT,
    remote_entity_type  INTEGER,
    title               TEXT,
    subtitle            TEXT,
    target_url          TEXT,
    PRIMARY KEY (account_id, message_id, ordinal),
    FOREIGN KEY (account_id, message_id) REFERENCES messages(account_id, message_id) ON DELETE CASCADE,
    FOREIGN KEY (account_id, asset_id) REFERENCES media_assets(account_id, asset_id)
) WITHOUT ROWID;

CREATE TABLE message_edits (
    account_id      TEXT NOT NULL,
    message_id      INTEGER NOT NULL,
    edit_index      INTEGER NOT NULL,
    edited_at_ms    INTEGER NOT NULL,
    previous_body   TEXT NOT NULL,
    PRIMARY KEY (account_id, message_id, edit_index),
    FOREIGN KEY (account_id, message_id) REFERENCES messages(account_id, message_id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE TABLE message_reactions (
    account_id      TEXT NOT NULL,
    message_id      INTEGER NOT NULL,
    user_id         INTEGER NOT NULL,
    reaction        TEXT NOT NULL,
    reacted_at_ms   INTEGER,
    log_message_id  INTEGER,
    PRIMARY KEY (account_id, message_id, user_id),
    FOREIGN KEY (account_id, message_id) REFERENCES messages(account_id, message_id) ON DELETE CASCADE,
    FOREIGN KEY (account_id, user_id) REFERENCES users(account_id, user_id),
    FOREIGN KEY (account_id, log_message_id) REFERENCES messages(account_id, message_id)
) WITHOUT ROWID;

CREATE TABLE message_mentions (
    account_id          TEXT NOT NULL,
    message_id          INTEGER NOT NULL,
    offset              INTEGER NOT NULL,
    length              INTEGER NOT NULL,
    kind                TEXT NOT NULL,
    mentioned_user_id   INTEGER,
    mentioned_thread_id INTEGER,
    PRIMARY KEY (account_id, message_id, offset, length, kind),
    FOREIGN KEY (account_id, message_id) REFERENCES messages(account_id, message_id) ON DELETE CASCADE,
    FOREIGN KEY (account_id, mentioned_user_id) REFERENCES users(account_id, user_id),
    FOREIGN KEY (account_id, mentioned_thread_id) REFERENCES threads(account_id, thread_id)
) WITHOUT ROWID;

CREATE TABLE sync_cursors (
    account_id          TEXT NOT NULL,
    scope               TEXT NOT NULL,      -- 'inbox' | 'head' | 'backfill'
    scope_id            TEXT NOT NULL,
    revision            INTEGER NOT NULL DEFAULT 0,
    target TEXT, candidate TEXT, generation INTEGER NOT NULL DEFAULT 0,
    cursor              TEXT,
    anchor_remote_id    TEXT,
    page_fingerprint    BLOB,
    complete            INTEGER NOT NULL DEFAULT 0,
    last_success_ms     INTEGER NOT NULL,
    PRIMARY KEY (account_id, scope, scope_id),
    FOREIGN KEY(account_id) REFERENCES accounts(account_id)
) WITHOUT ROWID;


CREATE TABLE operational_state (
 account_id TEXT NOT NULL REFERENCES accounts(account_id), scope TEXT NOT NULL, scope_id TEXT NOT NULL,
 revision INTEGER NOT NULL DEFAULT 0, anchor TEXT, target TEXT, candidate TEXT, generation INTEGER NOT NULL DEFAULT 0,
 page_hash TEXT, cursor TEXT, complete INTEGER NOT NULL DEFAULT 0, seq_id INTEGER NOT NULL DEFAULT 0,
 timestamp_ms INTEGER NOT NULL DEFAULT 0, last_success_ms INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '',
 connected INTEGER, observed_at_ms INTEGER, subscribed_seq_id INTEGER, latest_seq_id INTEGER, failures INTEGER,
 session_saved INTEGER, last_validated_ms INTEGER, established_at_ms INTEGER, coverage TEXT,
 PRIMARY KEY(account_id,scope,scope_id)
) WITHOUT ROWID;
CREATE TABLE operational_gaps (
 account_id TEXT NOT NULL, scope TEXT NOT NULL, scope_id TEXT NOT NULL, ordinal INTEGER NOT NULL, detail TEXT NOT NULL,
 PRIMARY KEY(account_id,scope,scope_id,ordinal),
 FOREIGN KEY(account_id,scope,scope_id) REFERENCES operational_state(account_id,scope,scope_id) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE TABLE thread_pinned_messages (
 account_id TEXT NOT NULL, thread_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
 PRIMARY KEY(account_id,thread_id,message_id),
 FOREIGN KEY(account_id,thread_id) REFERENCES threads(account_id,thread_id) ON DELETE CASCADE,
 FOREIGN KEY(account_id,message_id) REFERENCES messages(account_id,message_id)
) WITHOUT ROWID;
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
	var missing []Alias
	for _, a := range aliases {
		if canonical >= 0 && canonical < int64(len(aliases)) && a.Namespace == aliases[canonical].Namespace && a.Scope == aliases[canonical].Scope && a.RemoteID != aliases[canonical].RemoteID {
			return id, false, false, ErrConflict
		}
		owner, err := aliasOwner(ctx, tx, account, kind, a)
		if errors.Is(err, sql.ErrNoRows) {
			missing = append(missing, a)
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
	for _, a := range missing {
		_, err := tx.ExecContext(ctx, "INSERT INTO source_ids(account_id,namespace,scope,remote_id,"+aliasColumn(kind)+") VALUES(?,?,?,?,?)", account, a.Namespace, a.Scope, a.RemoteID, id)
		if err != nil {
			return id, created, false, err
		}
	}
	return id, created, len(missing) > 0, nil
}

// Patch writes only supplied fields with different values. Field names come
// from the ingestion code, never from keys supplied by Instagram.
func Patch(ctx context.Context, tx *sql.Tx, table, where string, keys []any, values map[string]any, fill bool) ([]string, []string, error) {
	var columns []string
	for name := range values {
		columns = append(columns, name)
	}
	if len(columns) == 0 {
		return nil, nil, nil
	}
	sort.Strings(columns)
	current := make([]any, len(columns))
	dest := make([]any, len(columns))
	for i := range dest {
		dest[i] = &current[i]
	}
	if err := tx.QueryRowContext(ctx, "SELECT "+strings.Join(columns, ",")+" FROM "+table+" WHERE "+where, keys...).Scan(dest...); err != nil {
		return nil, nil, err
	}
	var changed, ignored, set []string
	var args []any
	for i, name := range columns {
		value := values[name]
		if current[i] == value {
			continue
		}
		if fill && current[i] != nil {
			ignored = append(ignored, name)
			continue
		}
		changed = append(changed, name)
		set = append(set, name+"=?")
		args = append(args, value)
	}
	if len(set) == 0 {
		return nil, ignored, nil
	}
	_, err := tx.ExecContext(ctx, "UPDATE "+table+" SET "+strings.Join(set, ",")+" WHERE "+where, append(args, keys...)...)
	return changed, ignored, err
}

func JSONValue(value any) string {
	b, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("invalid stored value: %v", err))
	}
	return string(b)
}
