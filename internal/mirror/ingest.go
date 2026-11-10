package mirror

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"go.mau.fi/mautrix-meta/internal/media"
	"go.mau.fi/mautrix-meta/internal/store"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
)

type sourceObject map[string]json.RawMessage

func decodeObject(raw json.RawMessage) (sourceObject, error) {
	var o sourceObject
	if json.Unmarshal(raw, &o) != nil || o == nil {
		return nil, errors.New("expected source object")
	}
	return o, nil
}

func (o sourceObject) id(key string) string {
	raw := o[key]
	var s string
	if json.Unmarshal(raw, &s) == nil {
		if s != "0" {
			return s
		}
		return ""
	}
	if len(raw) == 0 {
		return ""
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return ""
		}
	}
	if string(raw) == "0" {
		return ""
	}
	return string(raw)
}

func (o sourceObject) fields(spec map[string]string) (map[string]any, error) {
	values := make(map[string]any)
	for key, column := range spec {
		raw, present := o[key]
		if !present {
			continue
		}
		if string(raw) == "null" {
			switch column {
			case "title", "image_url", "avatar_url", "nickname":
				values[column] = ""
			}
			continue
		}
		var v any
		switch column {
		case "activity_at", "sent_at", "edited_at", "expires_at", "view_expires_at", "read_at", "input_mode", "latest_reel", "latest_besties_reel", "reel_seen_at":
			n, err := strconv.ParseInt(o.id(key), 10, 64)
			if string(raw) == "0" || string(raw) == `"0"` {
				n, err = 0, nil
			}
			if err != nil {
				return nil, fmt.Errorf("invalid source integer field %s", key)
			}
			v = n
		case "verified", "restricted", "blocking", "following", "followed_by", "is_group", "muted", "pinned", "unread", "forwarded", "reported", "ai_generated", "admin", "member":
			var b bool
			if json.Unmarshal(raw, &b) != nil {
				return nil, fmt.Errorf("invalid source boolean field %s", key)
			}
			if b {
				v = int64(1)
			} else {
				v = int64(0)
			}
		default:
			var s string
			if json.Unmarshal(raw, &s) != nil {
				return nil, fmt.Errorf("invalid source text field %s", key)
			}
			v = s
		}
		values[column] = v
	}
	return values, nil
}

func addAlias(out []store.Alias, ns, scope, value string) []store.Alias {
	if value == "" {
		return out
	}
	a := store.Alias{Namespace: ns, Scope: scope, RemoteID: value}
	for _, old := range out {
		if old == a {
			return out
		}
	}
	return append(out, a)
}

type ingestion struct {
	ctx       context.Context
	tx        *sql.Tx
	account   string
	fill      bool
	event     int64
	decisions []store.Decision
}

func (b *ingestion) resolve(kind string, aliases []store.Alias, primary string, expected int64) (int64, bool, bool, error) {
	canonical := int64(-1)
	for i, a := range aliases {
		if a.Namespace == primary {
			canonical = int64(i)
			break
		}
	}
	id, created, bound, err := store.ResolveAliases(b.ctx, b.tx, b.account, kind, aliases, canonical, expected)
	if err != nil {
		action := "deferred"
		if errors.Is(err, store.ErrConflict) {
			action = "conflict"
		}
		b.decisions = append(b.decisions, store.Decision{Kind: kind, ID: id, Action: action, Reason: err.Error()})
	}
	return id, created, bound, err
}

func (b *ingestion) decision(kind string, id int64, created, bound bool, fields []string) {
	action, reason := "unchanged", "values match"
	if bound {
		fields = append(fields, "source_ids")
	}
	if len(fields) > 0 {
		action, reason = "updated", ""
	}
	if created {
		action, reason = "created", "canonical source ID"
	}
	b.decisions = append(b.decisions, store.Decision{Kind: kind, ID: id, Action: action, Reason: reason, Fields: fields})
}

func (b *ingestion) ignored(kind string, id int64, fields []string) {
	if len(fields) > 0 {
		b.decisions = append(b.decisions, store.Decision{Kind: kind, ID: id, Action: "ignored_stale", Reason: "kept newer or fuller information", Fields: fields})
	}
}

func (b *ingestion) unsupported(kind string, id int64, o sourceObject, spec map[string]string, consumed ...string) {
	keys := make(map[string]bool, len(consumed))
	for _, key := range consumed {
		keys[key] = true
	}
	var fields []string
	for key := range o {
		if !keys[key] && spec[key] == "" {
			fields = append(fields, key)
		}
	}
	if len(fields) > 0 {
		sort.Strings(fields)
		b.decisions = append(b.decisions, store.Decision{Kind: kind, ID: id, Action: "ignored_unsupported", Reason: "field outside current mirror", Fields: fields})
	}
}

func (b *ingestion) patch(kind string, id int64, values map[string]any, fill bool) ([]string, error) {
	changed, ignored, err := store.Patch(b.ctx, b.tx, kind, "account_id=? AND id=?", []any{b.account, id}, values, fill)
	b.ignored(kind, id, ignored)
	return changed, err
}

func (b *ingestion) userRef(ns, id string) (int64, error) {
	owner, created, bound, err := b.resolve("users", addAlias(nil, ns, "", id), "user.igid", 0)
	if err == nil {
		b.decision("users", owner, created, bound, nil)
	}
	return owner, err
}

var profileFields = map[string]string{
	"username": "username", "full_name": "name", "profile_pic_url": "avatar_url", "is_verified": "verified",
	"ai_agent_type": "ai_agent_type", "latest_reel_media": "latest_reel", "latest_besties_reel_media": "latest_besties_reel",
	"reel_media_seen_timestamp": "reel_seen_at",
}

func (b *ingestion) user(raw json.RawMessage, igid, fbid, name string, fill bool) (int64, error) {
	o := sourceObject{}
	var err error
	if len(raw) > 0 && string(raw) != "null" {
		o, err = decodeObject(raw)
		if err != nil {
			return 0, err
		}
	}
	var aliases []store.Alias
	for _, value := range []string{igid, o.id("id"), o.id("pk"), o.id("igid")} {
		aliases = addAlias(aliases, "user.igid", "", value)
	}
	for _, value := range []string{fbid, o.id("interop_messaging_user_fbid")} {
		aliases = addAlias(aliases, "user.messaging_fbid", "", value)
	}
	aliases = addAlias(aliases, "user.fbid_v2", "", o.id("fbid_v2"))
	id, created, bound, err := b.resolve("users", aliases, "user.igid", 0)
	if err != nil {
		return 0, err
	}
	values, err := o.fields(profileFields)
	if err != nil {
		return 0, err
	}
	if rawStatus := o["friendship_status"]; len(rawStatus) > 0 && string(rawStatus) != "null" {
		status, err := decodeObject(rawStatus)
		if err != nil {
			return 0, err
		}
		spec := map[string]string{"is_restricted": "restricted", "blocking": "blocking", "following": "following", "followed_by": "followed_by"}
		flags, err := status.fields(spec)
		if err != nil {
			return 0, err
		}
		for k, v := range flags {
			values[k] = v
		}
		b.unsupported("users", id, status, spec, "__typename")
	}
	fields, err := b.patch("users", id, values, fill)
	if err == nil && name != "" {
		more, e := b.patch("users", id, map[string]any{"name": name}, true)
		fields = append(fields, more...)
		err = e
	}
	b.decision("users", id, created, bound, fields)
	b.unsupported("users", id, o, profileFields, "id", "pk", "igid", "interop_messaging_user_fbid", "fbid_v2", "friendship_status", "__typename")
	return id, err
}

var threadFields = map[string]string{
	"thread_title": "title", "thread_image_url": "image_url", "system_folder": "folder", "last_activity_timestamp_ms": "activity_at",
	"thread_subtype": "subtype", "is_group": "is_group", "is_muted": "muted", "is_pin": "pinned", "marked_as_unread": "unread",
	"input_mode": "input_mode", "reachability_status": "reachability",
}

func (b *ingestion) thread(raw json.RawMessage, expected int64, remote string, complete bool) (int64, error) {
	o, err := decodeObject(raw)
	if err != nil {
		return 0, err
	}
	var aliases []store.Alias
	for _, value := range []string{remote, o.id("id"), o.id("thread_fbid")} {
		aliases = addAlias(aliases, "thread.fbid", "", value)
	}
	aliases = addAlias(aliases, "thread.igid", "", o.id("thread_id"))
	if scope := o.id("thread_subtype"); scope != "" {
		aliases = addAlias(aliases, "thread.key", scope, o.id("thread_key"))
	}
	id, created, bound, err := b.resolve("threads", aliases, "thread.fbid", expected)
	if err != nil {
		return 0, err
	}
	values, err := o.fields(threadFields)
	if err != nil {
		return 0, err
	}
	if _, ok := values["folder"]; !ok && len(o["folder"]) != 0 {
		fallback, err := o.fields(map[string]string{"folder": "folder"})
		if err != nil {
			return 0, err
		}
		values["folder"] = fallback["folder"]
	}
	var deleted sql.NullBool
	if err = b.tx.QueryRowContext(b.ctx, "SELECT deleted FROM threads WHERE id=?", id).Scan(&deleted); err != nil {
		return 0, err
	}
	if deleted.Bool {
		b.ignored("threads", id, []string{"deleted"})
		b.decision("threads", id, created, bound, nil)
		return id, nil
	}
	fields, err := b.patch("threads", id, values, b.fill)
	if err != nil {
		return 0, err
	}
	b.decision("threads", id, created, bound, fields)
	b.unsupported("threads", id, o, threadFields, "id", "thread_fbid", "thread_id", "thread_key", "folder", "users", "usersWithoutViewer", "viewer", "viewer_id", "nicknames", "slide_read_receipts", "admin_user_ids", "slide_messages", "pinned_messages", "__typename")
	if err = b.members(id, o, complete && !b.fill); err != nil {
		return 0, err
	}
	if complete && !b.fill && len(o["pinned_messages"]) > 0 && string(o["pinned_messages"]) != "null" {
		var items []json.RawMessage
		if json.Unmarshal(o["pinned_messages"], &items) != nil {
			return 0, errors.New("invalid pinned messages")
		}
		ids := map[int64]bool{}
		for _, item := range items {
			ref := sourceObject{"id": item}.id("id")
			if node, e := decodeObject(item); e == nil {
				ref = node.id("message_id")
				if ref == "" {
					ref = node.id("id")
				}
			}
			msg, e := b.messageRef(ref, id)
			if e != nil {
				return 0, e
			}
			ids[msg] = true
		}
		rows, e := b.tx.QueryContext(b.ctx, "SELECT id FROM messages WHERE thread_id=? AND pinned=1", id)
		if e != nil {
			return 0, e
		}
		for rows.Next() {
			var msg int64
			if e = rows.Scan(&msg); e != nil {
				rows.Close()
				return 0, e
			}
			if _, ok := ids[msg]; !ok {
				ids[msg] = false
			}
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return 0, e
		}
		for msg, pinned := range ids {
			value := int64(0)
			if pinned {
				value = 1
			}
			changed, e := b.patch("messages", msg, map[string]any{"pinned": value}, false)
			if e != nil {
				return 0, e
			}
			b.decision("messages", msg, false, false, changed)
		}
	}
	return id, nil
}

func (b *ingestion) member(thread, user int64, values map[string]any, fill bool) error {
	res, err := b.tx.ExecContext(b.ctx, "INSERT INTO thread_members(thread_id,user_id) VALUES(?,?) ON CONFLICT DO NOTHING", thread, user)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	fields, ignored, err := store.Patch(b.ctx, b.tx, "thread_members", "thread_id=? AND user_id=?", []any{thread, user}, values, fill)
	b.ignored("thread_members", thread, ignored)
	b.decision("thread_members", thread, n > 0, false, fields)
	return err
}

func (b *ingestion) members(thread int64, o sourceObject, complete bool) error {
	seen := map[int64]bool{}
	for _, key := range []string{"users", "usersWithoutViewer", "viewer"} {
		raw := o[key]
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		var items []json.RawMessage
		if key == "viewer" {
			items = []json.RawMessage{raw}
		} else if json.Unmarshal(raw, &items) != nil {
			return fmt.Errorf("invalid %s", key)
		}
		for _, item := range items {
			id, err := b.user(item, "", "", "", b.fill || key == "usersWithoutViewer")
			if err != nil {
				return err
			}
			if key == "users" {
				seen[id] = true
			}
			if key != "viewer" {
				if err = b.member(thread, id, map[string]any{"member": int64(1)}, b.fill); err != nil {
					return err
				}
			}
		}
	}
	if complete && len(o["users"]) > 0 && string(o["users"]) != "null" {
		rows, err := b.tx.QueryContext(b.ctx, "SELECT user_id FROM thread_members WHERE thread_id=? AND member=1", thread)
		if err != nil {
			return err
		}
		var removed []int64
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			if !seen[id] {
				removed = append(removed, id)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, id := range removed {
			if err = b.member(thread, id, map[string]any{"member": int64(0)}, false); err != nil {
				return err
			}
		}
	}
	for _, key := range []string{"nicknames", "slide_read_receipts", "admin_user_ids"} {
		raw := o[key]
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		var items []json.RawMessage
		if json.Unmarshal(raw, &items) != nil {
			return fmt.Errorf("invalid %s", key)
		}
		admins := map[int64]bool{}
		for _, item := range items {
			switch key {
			case "admin_user_ids":
				id, err := b.userRef("user.igid", sourceObject{"id": item}.id("id"))
				if err != nil {
					return err
				}
				admins[id] = true
				if err = b.member(thread, id, map[string]any{"admin": int64(1)}, b.fill); err != nil {
					return err
				}
			case "nicknames":
				node, err := decodeObject(item)
				if err != nil {
					return err
				}
				id, err := b.user(nil, node.id("igid"), node.id("eimu_id"), "", true)
				if err != nil {
					return err
				}
				values, err := node.fields(map[string]string{"nickname": "nickname"})
				if err != nil {
					return err
				}
				if err = b.member(thread, id, values, b.fill); err != nil {
					return err
				}
			case "slide_read_receipts":
				if err := b.receipt(thread, item); err != nil {
					return err
				}
			}
		}
		if key == "admin_user_ids" && complete && !b.fill {
			if err := b.clearAdmins(thread, admins); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *ingestion) clearAdmins(thread int64, admins map[int64]bool) error {
	rows, err := b.tx.QueryContext(b.ctx, "SELECT user_id FROM thread_members WHERE thread_id=? AND admin=1", thread)
	if err != nil {
		return err
	}
	var removed []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		if !admins[id] {
			removed = append(removed, id)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range removed {
		if err = b.member(thread, id, map[string]any{"admin": int64(0)}, false); err != nil {
			return err
		}
	}
	return nil
}

func (b *ingestion) receipt(thread int64, raw json.RawMessage) error {
	o, err := decodeObject(raw)
	if err != nil {
		return err
	}
	user, err := b.userRef("user.messaging_fbid", o.id("participant_fbid"))
	if err != nil {
		return err
	}
	values, err := o.fields(map[string]string{"watermark_timestamp_ms": "read_at"})
	if err != nil {
		return err
	}
	if at, ok := values["read_at"].(int64); ok {
		var current sql.NullInt64
		err = b.tx.QueryRowContext(b.ctx, "SELECT read_at FROM thread_members WHERE thread_id=? AND user_id=?", thread, user).Scan(&current)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if current.Valid && current.Int64 > at {
			b.decision("thread_members", thread, false, false, nil)
			return nil
		}
	}
	return b.member(thread, user, values, b.fill)
}

var messageFields = map[string]string{
	"timestamp_ms": "sent_at", "text_body": "body", "content_type": "kind", "igd_snippet": "snippet",
	"expiration_timestamp_ms": "expires_at", "view_expiration_timestamp_ms": "view_expires_at",
	"igd_is_forwarded": "forwarded", "is_pinned": "pinned", "is_reported": "reported", "is_ai_generated": "ai_generated",
}

func (b *ingestion) messageRef(remote string, thread int64) (int64, error) {
	id, created, bound, err := b.resolve("messages", addAlias(nil, "message.mid", "", remote), "message.mid", 0)
	if err != nil {
		return 0, err
	}
	fields, err := b.messageThread(id, thread)
	b.decision("messages", id, created, bound, fields)
	return id, err
}

func (b *ingestion) messageThread(id, thread int64) ([]string, error) {
	if thread == 0 {
		return nil, nil
	}
	var current sql.NullInt64
	if err := b.tx.QueryRowContext(b.ctx, "SELECT thread_id FROM messages WHERE id=?", id).Scan(&current); err != nil {
		return nil, err
	}
	if current.Valid && current.Int64 != thread {
		return nil, store.ErrConflict
	}
	return b.patch("messages", id, map[string]any{"thread_id": thread}, true)
}

func (b *ingestion) message(raw json.RawMessage, thread int64, quote bool) (int64, error) {
	o, err := decodeObject(raw)
	if err != nil {
		return 0, err
	}
	aliases := addAlias(nil, "message.mid", "", o.id("id"))
	aliases = addAlias(aliases, "message.mid", "", o.id("message_id"))
	id, created, bound, err := b.resolve("messages", aliases, "message.mid", 0)
	if err != nil {
		return 0, err
	}
	if ref := o.id("thread_fbid"); ref != "" {
		resolved, created, bound, err := b.resolve("threads", addAlias(nil, "thread.fbid", "", ref), "thread.fbid", thread)
		if err != nil {
			return 0, err
		}
		thread = resolved
		b.decision("threads", thread, created, bound, nil)
	}
	fields, err := b.messageThread(id, thread)
	if err != nil {
		return 0, err
	}
	fill := b.fill || quote
	var deleted, placeholder int64
	var edited sql.NullInt64
	if err = b.tx.QueryRowContext(b.ctx, "SELECT coalesce(deleted,0),placeholder,edited_at FROM messages WHERE id=?", id).Scan(&deleted, &placeholder, &edited); err != nil {
		return 0, err
	}
	if deleted != 0 {
		b.decision("messages", id, created, bound, fields)
		return id, nil
	}
	values, err := o.fields(messageFields)
	if err != nil {
		return 0, err
	}
	if rawSender := o["sender"]; len(rawSender) > 0 && string(rawSender) != "null" {
		sender, err := decodeObject(rawSender)
		if err != nil {
			return 0, err
		}
		igid := sender.id("igid")
		if igid == "" {
			igid = sender.id("id")
		} else if alt := sender.id("id"); alt != "" && alt != igid {
			return 0, store.ErrConflict
		}
		uid, err := b.user(sender["user_dict"], igid, o.id("sender_fbid"), sender.id("name"), fill)
		if err != nil {
			return 0, err
		}
		values["sender_id"] = uid
	} else if fbid := o.id("sender_fbid"); fbid != "" {
		uid, err := b.userRef("user.messaging_fbid", fbid)
		if err != nil {
			return 0, err
		}
		values["sender_id"] = uid
	}
	if !quote {
		var reply int64
		if ref := o.id("replied_to_message_id"); ref != "" {
			reply, err = b.messageRef(ref, thread)
			if err != nil {
				return 0, err
			}
		}
		if nested := o["replied_to_message"]; len(nested) > 0 && string(nested) != "null" {
			other, err := b.message(nested, thread, true)
			if err != nil {
				return 0, err
			}
			if reply != 0 && other != reply {
				return 0, store.ErrConflict
			}
			reply = other
		}
		if reply != 0 {
			if reply == id {
				return 0, store.ErrConflict
			}
			values["reply_id"] = reply
		}
	}
	if content := o["content"]; len(content) > 0 {
		var msg slidetypes.Message
		if string(content) != "null" && json.Unmarshal(content, &msg.Content) != nil {
			return 0, errors.New("invalid message content")
		}
		if kind, ok := values["kind"].(string); ok {
			msg.ContentType = kind
		}
		expiring := o.id("expiration_timestamp_ms") != "" || o.id("view_expiration_timestamp_ms") != ""
		values["attachments"] = store.JSONValue(media.DescribeAttachments(msg, expiring))
		if values["kind"] == nil && msg.Content.TypeName != "" {
			values["kind"] = msg.Content.TypeName
		}
		if values["body"] == nil {
			if text := displayBody(msg); text != "" {
				values["body"] = text
			}
		}
	}

	var sourceEdit int64
	if history := o["slide_edit_history"]; len(history) > 0 && string(history) != "null" {
		var entries []slidetypes.MessageEditHistoryEntry
		if json.Unmarshal(history, &entries) != nil {
			return 0, errors.New("invalid message edits")
		}
		edits := make([]store.Edit, 0, len(entries))
		for _, edit := range entries {
			var at int64
			if !edit.TimestampMS.IsZero() {
				at = edit.TimestampMS.UnixMilli()
			}
			if at > sourceEdit {
				sourceEdit = at
			}
			edits = append(edits, store.Edit{Body: edit.Body, At: at})
		}
		values["edits"] = store.JSONValue(edits)
		if sourceEdit > 0 {
			values["edited_at"] = sourceEdit
		}
	}
	if edited.Valid && (fill || sourceEdit < edited.Int64) {
		var ignored []string
		for _, key := range []string{"body", "snippet", "edits", "edited_at"} {
			if _, present := values[key]; present {
				ignored = append(ignored, key)
			}
			delete(values, key)
		}
		b.ignored("messages", id, ignored)
	}
	if fill && !quote && placeholder != 0 {
		// A full acquisition can replace text previously acquired as a quote.
		full := map[string]any{}
		for _, key := range []string{"body", "snippet", "kind", "sent_at", "attachments"} {
			if v, ok := values[key]; ok && v != nil {
				full[key] = v
				delete(values, key)
			}
		}
		more, err := b.patch("messages", id, full, false)
		if err != nil {
			return 0, err
		}
		fields = append(fields, more...)
	}
	more, err := b.patch("messages", id, values, fill)
	if err != nil {
		return 0, err
	}
	fields = append(fields, more...)
	if !quote && placeholder != 0 && thread != 0 && o.id("timestamp_ms") != "" {
		more, err = b.patch("messages", id, map[string]any{"placeholder": int64(0)}, false)
		if err != nil {
			return 0, err
		}
		fields = append(fields, more...)
	}
	for _, key := range fields {
		if key == "attachments" {
			if _, err := b.tx.ExecContext(b.ctx, `DELETE FROM media_cache WHERE account_id=? AND message_id=? AND NOT EXISTS(
 SELECT 1 FROM json_each((SELECT attachments FROM messages WHERE id=?)) a
 WHERE a.value->>'key'=media_cache.attachment AND coalesce(a.value->>'restricted',0)=(media_cache.status='restricted'))`, b.account, id, id); err != nil {
				return 0, err
			}
			break
		}
	}
	b.decision("messages", id, created, bound, fields)
	b.unsupported("messages", id, o, messageFields, "id", "message_id", "thread_fbid", "sender_fbid", "sender", "replied_to_message_id", "replied_to_message", "content", "slide_edit_history", "reactions", "mentions", "offline_threading_id", "__typename")
	if !quote {
		if err = b.messageRelations(id, thread, o, fill); err != nil {
			return 0, err
		}
	}
	return id, nil
}

func displayBody(msg slidetypes.Message) string {
	switch content := msg.Content.Content.(type) {
	case *slidetypes.MessageContentText:
		return content.TextBody
	case *slidetypes.MessageContentAdminText:
		var parts []string
		for _, fragment := range content.TextFragments {
			parts = append(parts, fragment.Plaintext)
			if fragment.LinkFragment != nil {
				parts = append(parts, " "+fragment.LinkFragment.URI)
			}
		}
		return strings.Join(parts, "")
	case *slidetypes.MessageContentXMA:
		parts := []string{}
		if content.XMATextBody != "" {
			parts = append(parts, content.XMATextBody)
		}
		if content.XMA != nil {
			for _, text := range []string{content.XMA.TitleText, content.XMA.CaptionBodyText} {
				if text != "" {
					parts = append(parts, text)
				}
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

func (b *ingestion) messageRelations(id, thread int64, o sourceObject, fill bool) error {
	for _, key := range []string{"reactions", "mentions"} {
		raw := o[key]
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		var known sql.NullInt64
		if err := b.tx.QueryRowContext(b.ctx, "SELECT "+key+"_known FROM messages WHERE id=?", id).Scan(&known); err != nil {
			return err
		}
		if fill && known.Valid {
			b.ignored("messages", id, []string{key})
			continue
		}
		var items []json.RawMessage
		if json.Unmarshal(raw, &items) != nil {
			return fmt.Errorf("invalid message %s", key)
		}
		switch key {
		case "reactions":
			seen := map[int64]bool{}
			for _, item := range items {
				user, err := b.reaction(id, thread, item, false)
				if err != nil {
					return err
				}
				seen[user] = true
			}
			rows, err := b.tx.QueryContext(b.ctx, "SELECT user_id FROM message_reactions WHERE message_id=?", id)
			if err != nil {
				return err
			}
			var removed []int64
			for rows.Next() {
				var user int64
				if err = rows.Scan(&user); err != nil {
					rows.Close()
					return err
				}
				if !seen[user] {
					removed = append(removed, user)
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, user := range removed {
				if _, err = b.tx.ExecContext(b.ctx, "DELETE FROM message_reactions WHERE message_id=? AND user_id=?", id, user); err != nil {
					return err
				}
				b.decision("message_reactions", id, false, false, []string{"removed"})
			}
		case "mentions":
			type mentionKey struct {
				offset, length int
				kind           string
			}
			seen := map[mentionKey]bool{}
			for _, item := range items {
				var m slidetypes.Mention
				if json.Unmarshal(item, &m) != nil {
					return errors.New("invalid mention")
				}
				var user, target any
				if m.UserFBID != 0 {
					switch m.ProfileRangeType {
					case slidetypes.ProfileRangeTypeProfile, slidetypes.ProfileRangeTypeAI:
						id, err := b.userRef("user.messaging_fbid", strconv.FormatInt(m.UserFBID, 10))
						if err != nil {
							return err
						}
						user = id
					case slidetypes.ProfileRangeTypeThread:
						if thread == 0 {
							return store.ErrDeferred
						}
						target = thread
					default:
						return errors.New("unsupported mention target")
					}
				}
				k := mentionKey{m.Offset, m.Length, string(m.ProfileRangeType)}
				seen[k] = true
				res, err := b.tx.ExecContext(b.ctx, `INSERT INTO message_mentions VALUES(?,?,?,?,?,?) ON CONFLICT(message_id,offset,length,kind) DO UPDATE SET user_id=excluded.user_id,thread_id=excluded.thread_id WHERE user_id IS NOT excluded.user_id OR thread_id IS NOT excluded.thread_id`, id, k.offset, k.length, k.kind, user, target)
				if err != nil {
					return err
				}
				n, err := res.RowsAffected()
				if err != nil {
					return err
				}
				var fields []string
				if n > 0 {
					fields = []string{"target"}
				}
				b.decision("message_mentions", id, false, false, fields)
			}
			rows, err := b.tx.QueryContext(b.ctx, "SELECT offset,length,kind FROM message_mentions WHERE message_id=?", id)
			if err != nil {
				return err
			}
			var removed []mentionKey
			for rows.Next() {
				var k mentionKey
				if err = rows.Scan(&k.offset, &k.length, &k.kind); err != nil {
					rows.Close()
					return err
				}
				if !seen[k] {
					removed = append(removed, k)
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, k := range removed {
				if _, err = b.tx.ExecContext(b.ctx, "DELETE FROM message_mentions WHERE message_id=? AND offset=? AND length=? AND kind=?", id, k.offset, k.length, k.kind); err != nil {
					return err
				}
				b.decision("message_mentions", id, false, false, []string{"removed"})
			}
		}
		if _, err := b.patch("messages", id, map[string]any{key + "_known": int64(1)}, false); err != nil {
			return err
		}
	}
	return nil
}

func (b *ingestion) reaction(msg, thread int64, raw json.RawMessage, remove bool) (int64, error) {
	o, err := decodeObject(raw)
	if err != nil {
		return 0, err
	}
	user, err := b.userRef("user.messaging_fbid", o.id("sender_fbid"))
	if err != nil {
		return 0, err
	}
	var exists bool
	if err = b.tx.QueryRowContext(b.ctx, "SELECT EXISTS(SELECT 1 FROM message_reactions WHERE message_id=? AND user_id=?)", msg, user).Scan(&exists); err != nil {
		return 0, err
	}
	var result sql.Result
	if remove {
		result, err = b.tx.ExecContext(b.ctx, "DELETE FROM message_reactions WHERE message_id=? AND user_id=?", msg, user)
	} else {
		value := o.id("reaction")
		if value == "" {
			return 0, errors.New("missing reaction")
		}
		values, err := o.fields(map[string]string{"reaction_timestamp_ms": "read_at"})
		if err != nil {
			return 0, err
		}
		var log any
		if ref := o.id("log_message_id"); ref != "" {
			id, err := b.messageRef(ref, thread)
			if err != nil {
				return 0, err
			}
			log = id
		}
		result, err = b.tx.ExecContext(b.ctx, `INSERT INTO message_reactions VALUES(?,?,?,?,?) ON CONFLICT(message_id,user_id) DO UPDATE SET reaction=excluded.reaction,reacted_at=excluded.reacted_at,log_message_id=excluded.log_message_id WHERE reaction IS NOT excluded.reaction OR reacted_at IS NOT excluded.reacted_at OR log_message_id IS NOT excluded.log_message_id`, msg, user, value, values["read_at"], log)
		if err != nil {
			return 0, err
		}
	}
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	var changed []string
	if n > 0 {
		changed = []string{"reaction"}
	}
	b.decision("message_reactions", msg, !exists && !remove, false, changed)
	if b.event > 0 {
		_, err = b.patch("messages", msg, map[string]any{"reactions_known": int64(1)}, false)
	}
	return user, err
}

func (b *ingestion) delta(raw json.RawMessage, delta slidetypes.Delta) error {
	o, err := decodeObject(raw)
	if err != nil {
		return err
	}
	switch delta.Data.(type) {
	case slidetypes.IgnoredEvent:
		b.decisions = append(b.decisions, store.Decision{Kind: "event", Action: "unchanged", Reason: "source no-op " + delta.TypeName})
		return nil
	case slidetypes.UnknownEvent:
		return fmt.Errorf("unsupported event %s", delta.TypeName)
	}
	var thread int64
	if nested := field(raw, "thread", "as_ig_direct_thread"); len(nested) > 0 && string(nested) != "null" {
		_, complete := delta.Data.(*slidetypes.ParticipantJoinEvent)
		thread, err = b.thread(nested, 0, o.id("thread_fbid"), complete)
	} else {
		remote := o.id("thread_fbid")
		if remote == "" {
			remote = sourceObject{"thread_fbid": field(raw, "message", "thread_fbid")}.id("thread_fbid")
		}
		var created, bound bool
		thread, created, bound, err = b.resolve("threads", addAlias(nil, "thread.fbid", "", remote), "thread.fbid", 0)
		if err == nil {
			b.decision("threads", thread, created, bound, nil)
		}
	}
	if err != nil {
		return err
	}
	var deleted sql.NullBool
	if err = b.tx.QueryRowContext(b.ctx, "SELECT deleted FROM threads WHERE id=?", thread).Scan(&deleted); err != nil {
		return err
	}
	if deleted.Bool {
		b.decision("threads", thread, false, false, nil)
		return nil
	}
	var message int64
	if nested := o["message"]; len(nested) > 0 && string(nested) != "null" {
		message, err = b.message(nested, thread, false)
		if err != nil {
			return err
		}
	} else if ref := o.id("message_id"); ref != "" {
		message, err = b.messageRef(ref, thread)
		if err != nil {
			return err
		}
	}
	threadPatch := func(spec map[string]string, node sourceObject) error {
		values, err := node.fields(spec)
		if err != nil {
			return err
		}
		changed, err := b.patch("threads", thread, values, false)
		b.decision("threads", thread, false, false, changed)
		return err
	}
	messagePatch := func(values map[string]any) error {
		if message == 0 {
			return errors.New("missing target message")
		}
		changed, err := b.patch("messages", message, values, false)
		b.decision("messages", message, false, false, changed)
		return err
	}
	if message != 0 {
		if err = b.tx.QueryRowContext(b.ctx, "SELECT deleted FROM messages WHERE id=?", message).Scan(&deleted); err != nil {
			return err
		}
		if deleted.Bool {
			b.decision("messages", message, false, false, nil)
			return nil
		}
	}
	switch d := delta.Data.(type) {
	case *slidetypes.NewMessageEvent, *slidetypes.AdminMessageEvent:
		if message == 0 {
			return errors.New("message event has no message")
		}
	case *slidetypes.CreateReactionEvent, *slidetypes.DeleteReactionEvent:
		if message == 0 {
			return errors.New("reaction has no message")
		}
		_, remove := d.(*slidetypes.DeleteReactionEvent)
		_, err = b.reaction(message, thread, o["reaction"], remove)
	case *slidetypes.MarkReadEvent:
		if d.ReadTimestampMS.IsZero() {
			return errors.New("read event has no timestamp")
		}
		values := map[string]any{"read_at": d.ReadTimestampMS.UnixMilli(), "unread": int64(0)}
		var prior sql.NullInt64
		if err = b.tx.QueryRowContext(b.ctx, "SELECT read_at FROM threads WHERE id=?", thread).Scan(&prior); err != nil {
			return err
		}
		if prior.Valid && prior.Int64 > d.ReadTimestampMS.UnixMilli() {
			b.decision("threads", thread, false, false, nil)
			return nil
		}
		changed, err := b.patch("threads", thread, values, false)
		b.decision("threads", thread, false, false, changed)
		return err
	case *slidetypes.MarkUnreadEvent:
		return threadPatch(map[string]string{"marked_as_unread": "unread"}, o)
	case *slidetypes.ReadReceiptEvent:
		return b.receipt(thread, o["read_receipt"])
	case *slidetypes.EditMessageEvent:
		if message == 0 {
			return errors.New("edit has no message")
		}
		values, err := o.fields(map[string]string{"text_body": "body", "igd_snippet": "snippet"})
		if err != nil {
			return err
		}
		if d.SlideEditHistoryEntry.TimestampMS.IsZero() {
			return errors.New("edit has no timestamp")
		}
		at := d.SlideEditHistoryEntry.TimestampMS.UnixMilli()
		var previous sql.NullInt64
		var history sql.NullString
		if err = b.tx.QueryRowContext(b.ctx, "SELECT edited_at,edits FROM messages WHERE id=?", message).Scan(&previous, &history); err != nil {
			return err
		}
		if previous.Valid && previous.Int64 > at {
			b.decision("messages", message, false, false, nil)
			return nil
		}
		values["edited_at"] = at
		if text, ok := values["body"].(string); ok {
			edits := []store.Edit{}
			if history.Valid {
				if err = json.Unmarshal([]byte(history.String), &edits); err != nil {
					return err
				}
			}
			if len(edits) == 0 || edits[len(edits)-1] != (store.Edit{Body: text, At: at}) {
				edits = append(edits, store.Edit{Body: text, At: at})
				values["edits"] = store.JSONValue(edits)
			}
		}
		return messagePatch(values)
	case *slidetypes.DeleteMessageEvent:
		if err := messagePatch(map[string]any{"deleted": int64(1), "body": nil, "snippet": nil, "attachments": nil, "edits": nil}); err != nil {
			return err
		}
		_, err := b.tx.ExecContext(b.ctx, "DELETE FROM media_cache WHERE account_id=? AND message_id=?", b.account, message)
		return err
	case *slidetypes.DeleteThreadEvent:
		changed, err := b.patch("threads", thread, map[string]any{"deleted": int64(1)}, false)
		b.decision("threads", thread, false, false, changed)
		return err
	case *slidetypes.UpdateThreadNameEvent:
		return threadPatch(map[string]string{"thread_name": "title"}, o)
	case *slidetypes.UpdateThreadImageEvent:
		image, err := decodeObject(o["thread_image"])
		if err != nil {
			return err
		}
		return threadPatch(map[string]string{"uri": "image_url"}, image)
	case *slidetypes.UpdateThreadFolderEvent:
		key := "ig_inbox_folder"
		if len(o[key]) == 0 {
			key = "folder"
		}
		return threadPatch(map[string]string{key: "folder"}, o)
	case *slidetypes.PinThreadEvent:
		return threadPatch(map[string]string{"is_pinned": "pinned"}, o)
	case *slidetypes.MuteThreadEvent:
		return threadPatch(map[string]string{"is_muted_now": "muted"}, o)
	case *slidetypes.PinMessageEvent:
		for value, ids := range [][]string{d.UnpinnedMessages, d.PinnedMessages} {
			for _, remote := range ids {
				message, err = b.messageRef(remote, thread)
				if err != nil {
					return err
				}
				if err = messagePatch(map[string]any{"pinned": int64(value)}); err != nil {
					return err
				}
			}
		}
	case *slidetypes.ParticipantJoinEvent:
		if d.Thread.AsIGDirectThread == nil {
			return errors.New("join has no participant list")
		}
	case *slidetypes.ParticipantLeaveEvent:
		user, err := b.userRef("user.messaging_fbid", o.id("left_participant_fbid"))
		if err != nil {
			return err
		}
		return b.member(thread, user, map[string]any{"member": int64(0)}, false)
	case *slidetypes.AdminChangeEvent:
		if len(o["admin_igids"]) == 0 || string(o["admin_igids"]) == "null" {
			return errors.New("admin event has no list")
		}
		admins := map[int64]bool{}
		for _, remote := range d.AdminIGIDs {
			user, err := b.userRef("user.igid", remote)
			if err != nil {
				return err
			}
			admins[user] = true
			if err = b.member(thread, user, map[string]any{"admin": int64(1)}, false); err != nil {
				return err
			}
		}
		return b.clearAdmins(thread, admins)
	default:
		return fmt.Errorf("unsupported event %s", delta.TypeName)
	}
	return err
}
