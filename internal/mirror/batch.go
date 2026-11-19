package mirror

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/internal/store"
)

type entityKey struct {
	kind string
	id   int64
}
type entityStage struct {
	created      bool
	values       map[string]any
	original     map[string]any
	content      []byte
	fingerprint  []byte
	tier         int
	originalTier int
	attachments  *[]store.Attachment
	edits        *[]store.Edit
}

func entityColumn(kind string) string {
	switch kind {
	case "users":
		return "user_id"
	case "threads":
		return "thread_id"
	case "messages":
		return "message_id"
	}
	panic("invalid entity kind")
}
func (b *ingestion) incomingTier() int {
	if b.inboxPreview {
		return 2
	}
	if b.tier != 0 {
		return b.tier
	}
	return 4
}
func (b *ingestion) entity(kind string, id int64) (*entityStage, error) {
	if b.entities == nil {
		b.entities = map[entityKey]*entityStage{}
	}
	key := entityKey{kind, id}
	if v := b.entities[key]; v != nil {
		return v, nil
	}
	rows, err := b.tx.QueryContext(b.ctx, "SELECT * FROM "+kind+" WHERE account_id=? AND "+entityColumn(kind)+"=?", b.account, id)
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, err
	}
	if !rows.Next() {
		rows.Close()
		return nil, sql.ErrNoRows
	}
	values := make([]any, len(columns))
	dest := make([]any, len(columns))
	for i := range dest {
		dest[i] = &values[i]
	}
	err = rows.Scan(dest...)
	rows.Close()
	if err != nil {
		return nil, err
	}
	v := &entityStage{values: map[string]any{}, original: map[string]any{}}
	for i, name := range columns {
		v.values[name] = values[i]
		v.original[name] = values[i]
	}
	v.tier = int(v.values["source_tier"].(int64))
	v.originalTier = v.tier
	v.fingerprint, _ = v.values["fingerprint"].([]byte)
	v.content, _, err = store.CanonicalContent(b.ctx, b.tx, b.account, kind, id)
	if err != nil {
		return nil, err
	}
	b.entities[key] = v
	return v, nil
}
func (b *ingestion) scan(kind string, id int64, columns []string, dest ...any) error {
	v, err := b.entity(kind, id)
	if err != nil {
		return err
	}
	if len(columns) != len(dest) {
		return errors.New("invalid entity scan")
	}
	for i, col := range columns {
		value := v.values[col]
		switch out := dest[i].(type) {
		case *sql.NullInt64:
			*out = sql.NullInt64{}
			if value != nil {
				out.Int64 = value.(int64)
				out.Valid = true
			}
		case *sql.NullBool:
			*out = sql.NullBool{}
			if value != nil {
				out.Bool = value.(int64) != 0
				out.Valid = true
			}
		case *sql.NullString:
			*out = sql.NullString{}
			if value != nil {
				out.String = value.(string)
				out.Valid = true
			}
		case *int64:
			if value == nil {
				*out = 0
			} else {
				*out = value.(int64)
			}
		default:
			return fmt.Errorf("unsupported entity scan %s", col)
		}
	}
	return nil
}
func (b *ingestion) memo(kind string, id int64, o sourceObject, extra ...any) bool {
	if b.seenEntities == nil {
		b.seenEntities = map[entityKey]map[string]bool{}
	}
	key := entityKey{kind, id}
	if b.seenEntities[key] == nil {
		b.seenEntities[key] = map[string]bool{}
	}
	fingerprint := fmt.Sprintf("%d:%t:%s", b.incomingTier(), b.fill, store.Hash([]byte(store.JSONValue(struct {
		Source  sourceObject
		Context []any
	}{o, extra}))))
	if b.seenEntities[key][fingerprint] {
		return true
	}
	b.seenEntities[key][fingerprint] = true
	return false
}
func (b *ingestion) patch(kind string, id int64, values map[string]any, fill bool) ([]string, error) {
	return b.patchTier(kind, id, values, fill, b.incomingTier())
}
func (b *ingestion) patchTier(kind string, id int64, values map[string]any, fill bool, tier int) ([]string, error) {
	v, err := b.entity(kind, id)
	if err != nil {
		return nil, err
	}
	var fields []string
	for key, value := range values {
		lower := tier < v.tier
		authoritative := key == "deleted" || key == "read_at_ms" || key == "unread" || key == "pinned"
		// Explicit source events carry authoritative mutations independently of snapshot completeness.
		if b.event > 0 && key != "attachments" && key != "mentions" && key != "reply_to_message_id" {
			authoritative = true
		}
		if lower && !authoritative && !(tier == 1 && key == "thread_id" && v.values[key] == nil) {
			continue
		}
		if key == "attachments" || key == "edits" {
			if fill && !v.created && v.originalTier >= 3 && b.event == 0 {
				continue
			}
			if key == "attachments" {
				var items []store.Attachment
				if value != nil {
					switch a := value.(type) {
					case []store.Attachment:
						items = a
					default:
						return nil, errors.New("invalid attachments")
					}
				}
				v.attachments = &items
			} else {
				var items []store.Edit
				if value != nil {
					switch a := value.(type) {
					case []store.Edit:
						items = a
					default:
						return nil, errors.New("invalid edits")
					}
				}
				v.edits = &items
			}
			fields = append(fields, key)
			continue
		}
		if key == "avatar_url" || key == "image_url" {
			column, assetKind := "avatar_asset_id", "AVATAR"
			if key == "image_url" {
				column, assetKind = "image_asset_id", "IMAGE"
			}
			var asset any
			if raw, ok := value.(string); ok && raw != "" {
				stable, err := store.MediaKey("", []string{raw})
				if err != nil {
					return nil, err
				}
				asset, err = store.EnsureAsset(b.ctx, b.tx, b.account, stable, assetKind, []string{raw}, false)
				if err != nil {
					return nil, err
				}
			}
			key, value = column, asset
		}
		if _, ok := v.values[key]; !ok {
			return nil, fmt.Errorf("unknown normalized column %s.%s", kind, key)
		}
		if value == nil && tier > v.tier {
			continue
		}
		if fill && !v.created && v.originalTier > 1 && v.values[key] != nil && b.event == 0 {
			continue
		}
		if reflect.DeepEqual(v.values[key], value) {
			continue
		}
		v.values[key] = value
		fields = append(fields, key)
	}
	if tier > v.tier && (kind != "messages" || (v.values["thread_id"] != nil && v.values["sent_at_ms"] != nil)) {
		v.tier = tier
	}
	sort.Strings(fields)
	return fields, nil
}
func (b *ingestion) edits(id int64) ([]store.Edit, error) {
	v, err := b.entity("messages", id)
	if err != nil {
		return nil, err
	}
	if v.edits != nil {
		return *v.edits, nil
	}
	return (&store.Store{}).EditsFromTx(b.ctx, b.tx, b.account, id)
}
func (b *ingestion) flush() error {
	keys := make([]entityKey, 0, len(b.entities))
	for key := range b.entities {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].kind != keys[j].kind {
			return keys[i].kind < keys[j].kind
		}
		return keys[i].id < keys[j].id
	})
	for _, key := range keys {
		v := b.entities[key]
		if v.attachments != nil {
			if err := store.ReplaceAttachments(b.ctx, b.tx, b.account, key.id, *v.attachments); err != nil {
				return err
			}
		}
		if v.edits != nil {
			if err := store.ReplaceEdits(b.ctx, b.tx, b.account, key.id, *v.edits); err != nil {
				return err
			}
		}
		// Compute from merged scalars and the already staged normalized children before writing the entity.
		raw, hash, err := store.CanonicalMergedContent(b.ctx, b.tx, b.account, key.kind, key.id, v.values)
		if err != nil {
			return err
		}
		if bytes.Equal(raw, v.content) && bytes.Equal(hash[:], v.fingerprint) && v.tier <= v.originalTier {
			continue
		}
		columns := []string{}
		for column, value := range v.values {
			if column == "source_tier" || column == "fingerprint" || column == "updated_at_ms" {
				continue
			}
			if !reflect.DeepEqual(value, v.original[column]) {
				columns = append(columns, column)
			}
		}
		sort.Strings(columns)
		sets := []string{}
		args := []any{}
		for _, col := range columns {
			sets = append(sets, col+"=?")
			args = append(args, v.values[col])
		}
		sets = append(sets, "source_tier=?", "fingerprint=?", "updated_at_ms=?")
		args = append(args, v.tier, hash[:], time.Now().UnixMilli(), b.account, key.id)
		query := "UPDATE " + key.kind + " SET " + strings.Join(sets, ",") + " WHERE account_id=? AND " + entityColumn(key.kind) + "=?"
		if v.created {
			columns = nil
			args = nil
			for col := range v.values {
				if col == "fingerprint" || col == "source_tier" || col == "updated_at_ms" {
					continue
				}
				columns = append(columns, col)
			}
			sort.Strings(columns)
			for _, col := range columns {
				args = append(args, v.values[col])
			}
			columns = append(columns, "source_tier", "fingerprint", "updated_at_ms")
			args = append(args, v.tier, hash[:], time.Now().UnixMilli())
			query = "INSERT INTO " + key.kind + "(" + strings.Join(columns, ",") + ") VALUES(" + strings.TrimSuffix(strings.Repeat("?,", len(args)), ",") + ")"
		}
		if _, err = b.tx.ExecContext(b.ctx, query, args...); err != nil {
			return err
		}
	}
	return b.fingerprintMembers()
}
func (b *ingestion) fingerprintMembers() error {
	keys := make([][2]int64, 0, len(b.touchedMembers))
	for key := range b.touchedMembers {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	for _, key := range keys {
		var active, admin int64
		var nickname sql.NullString
		var read sql.NullInt64
		var previous []byte
		err := b.tx.QueryRowContext(b.ctx, `SELECT is_active,is_admin,nickname,read_at_ms,fingerprint FROM thread_members WHERE account_id=? AND thread_id=? AND user_id=?`, b.account, key[0], key[1]).Scan(&active, &admin, &nickname, &read, &previous)
		if err != nil {
			return err
		}
		raw := store.JSONValue(struct {
			Thread, User, Active, Admin int64
			Nickname                    sql.NullString
			Read                        sql.NullInt64
		}{key[0], key[1], active, admin, nickname, read})
		hash := sha256.Sum256([]byte(raw))
		if !bytes.Equal(previous, hash[:]) {
			if _, err = b.tx.ExecContext(b.ctx, `UPDATE thread_members SET fingerprint=? WHERE account_id=? AND thread_id=? AND user_id=?`, hash[:], b.account, key[0], key[1]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *ingestion) pinMetadata(thread int64, o sourceObject) error {
	raw, ok := o["pinned_messages_metadata"]
	if !ok {
		raw, ok = o["pinned_messages"]
	}
	if !ok || string(raw) == "null" || b.fill {
		return nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		node, e := decodeObject(raw)
		if e != nil {
			return errors.New("invalid pinned message metadata")
		}
		raw = node["pinned_messages"]
		if len(raw) == 0 {
			raw = node["edges"]
		}
		if len(raw) == 0 {
			return errors.New("unrecognized pinned message metadata")
		}
		if json.Unmarshal(raw, &items) != nil {
			return errors.New("invalid pinned message metadata list")
		}
	}
	ids := []int64{}
	for _, item := range items {
		node, e := decodeObject(item)
		ref := sourceObject{"id": item}.id("id")
		if e == nil {
			if nested := node["node"]; object(nested) {
				node, e = decodeObject(nested)
				if e != nil {
					return e
				}
			}
			ref = node.id("message_id")
			if ref == "" {
				ref = node.id("id")
			}
		}
		if ref == "" {
			return errors.New("pinned metadata lacks message identity")
		}
		id, e := b.messageRef(ref, thread)
		if e != nil {
			return e
		}
		ids = append(ids, id)
	}
	desired := map[int64]bool{}
	for _, id := range ids {
		desired[id] = true
	}
	rows, err := b.tx.QueryContext(b.ctx, `SELECT message_id FROM thread_pinned_messages WHERE account_id=? AND thread_id=?`, b.account, thread)
	if err != nil {
		return err
	}
	existing := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		existing[id] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for id := range existing {
		if desired[id] {
			continue
		}
		if _, err = b.tx.ExecContext(b.ctx, `DELETE FROM thread_pinned_messages WHERE account_id=? AND thread_id=? AND message_id=?`, b.account, thread, id); err != nil {
			return err
		}
		if _, err = b.patchTier("messages", id, map[string]any{"pinned": int64(0)}, false, 1); err != nil {
			return err
		}
	}
	for id := range desired {
		if !existing[id] {
			if _, err = b.tx.ExecContext(b.ctx, `INSERT INTO thread_pinned_messages VALUES(?,?,?)`, b.account, thread, id); err != nil {
				return err
			}
		}
		if _, err = b.patchTier("messages", id, map[string]any{"pinned": int64(1)}, false, 1); err != nil {
			return err
		}
	}

	return nil
}

func (b *ingestion) pendingThreadMedia(id string) (bool, error) {
	var pending bool
	err := b.tx.QueryRowContext(b.ctx, `SELECT EXISTS(SELECT 1 FROM media_assets a WHERE a.account_id=? AND a.download_status='pending' AND (EXISTS(SELECT 1 FROM message_attachments x JOIN messages m ON m.account_id=x.account_id AND m.message_id=x.message_id WHERE x.account_id=a.account_id AND x.asset_id=a.asset_id AND m.thread_id=?) OR EXISTS(SELECT 1 FROM threads t WHERE t.account_id=a.account_id AND t.thread_id=? AND t.image_asset_id=a.asset_id) OR EXISTS(SELECT 1 FROM thread_members tm JOIN users u ON u.account_id=tm.account_id AND u.user_id=tm.user_id WHERE tm.account_id=a.account_id AND tm.thread_id=? AND u.avatar_asset_id=a.asset_id)))`, b.account, id, id, id).Scan(&pending)
	return pending, err
}

// Reserve local IDs in the write transaction; deferred foreign keys allow
// forward references while each new entity is inserted only once at flush.
func (b *ingestion) reserve(kind string) (int64, error) {
	if b.nextID == nil {
		b.nextID = map[string]int64{}
		b.defaults = map[string]map[string]any{}
	}
	if _, ok := b.nextID[kind]; !ok {
		var last int64
		if err := b.tx.QueryRowContext(b.ctx, "SELECT coalesce(max("+entityColumn(kind)+"),0) FROM "+kind).Scan(&last); err != nil {
			return 0, err
		}
		b.nextID[kind] = last
		rows, err := b.tx.QueryContext(b.ctx, "PRAGMA table_info("+kind+")")
		if err != nil {
			return 0, err
		}
		defaults := map[string]any{}
		for rows.Next() {
			var cid, notnull, pk int
			var name, typ string
			var value sql.NullString
			if err = rows.Scan(&cid, &name, &typ, &notnull, &value, &pk); err != nil {
				rows.Close()
				return 0, err
			}
			var parsed any
			if value.Valid {
				if typ == "INTEGER" {
					n, e := strconv.ParseInt(value.String, 10, 64)
					if e != nil {
						rows.Close()
						return 0, e
					}
					parsed = n
				} else {
					parsed = strings.Trim(value.String, "'")
				}
			}
			defaults[name] = parsed
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return 0, err
		}
		b.defaults[kind] = defaults
	}
	if b.nextID[kind] == math.MaxInt64 {
		return 0, errors.New("local identity space exhausted")
	}
	b.nextID[kind]++
	id := b.nextID[kind]
	v := &entityStage{created: true, values: map[string]any{}, original: map[string]any{}, tier: 1, originalTier: 1}
	for k, value := range b.defaults[kind] {
		v.values[k] = value
	}
	v.values["account_id"] = b.account
	v.values[entityColumn(kind)] = id
	v.values["source_tier"] = int64(1)
	if b.entities == nil {
		b.entities = map[entityKey]*entityStage{}
	}
	b.entities[entityKey{kind, id}] = v
	return id, nil
}
