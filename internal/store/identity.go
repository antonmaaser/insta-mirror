package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func ownerColumn(kind string) string {
	switch kind {
	case "users":
		return "user_entity_id"
	case "threads":
		return "thread_entity_id"
	case "messages":
		return "message_entity_id"
	}
	return ""
}

// Namespaces describe protocol identities, not coincidentally equal field names.
// The original field and JSON type remain in the source version and evidence.
func identifierNamespace(kind, field string) string {
	switch kind {
	case "users":
		switch field {
		case "id", "pk", "igid":
			return "user.igid"
		case "interop_messaging_user_fbid", "sender_fbid", "participant_fbid", "actor_fbid", "left_participant_fbid", "viewer_fbid", "eimu_id":
			return "user.messaging_fbid"
		case "fbid_v2":
			return "user.fbid_v2"
		}
	case "threads":
		switch field {
		case "id", "thread_fbid":
			return "thread.fbid"
		case "thread_id":
			return "thread.igid"
		case "thread_key":
			return "thread.key"
		}
	case "messages":
		switch field {
		case "id", "message_id", "replied_to_message_id", "log_message_id":
			return "message.mid"
		}
	}
	return ""
}
func nodeField(n *sourceNode, key string) *sourceNode {
	var found *sourceNode
	for _, m := range n.members {
		if m.key == key {
			if found != nil {
				return nil
			}
			found = m.value
		}
	}
	return found
}
func nodeID(n *sourceNode) string {
	if n == nil || n.shape != 0 || len(n.scalar) == 0 {
		return ""
	}
	var s string
	if n.scalar[0] == '"' {
		if json.Unmarshal(n.scalar, &s) != nil {
			return ""
		}
		encoded, _ := json.Marshal(s)
		if !bytes.Equal(encoded, n.scalar) {
			return ""
		}
	} else {
		s = string(n.scalar)
		for _, c := range s {
			if c < '0' || c > '9' {
				return ""
			}
		}
	}
	if s == "" || s == "0" {
		return ""
	}
	return s
}
func nodeValue(n *sourceNode, key string) string { return nodeID(nodeField(n, key)) }

type sourceAlias struct{ namespace, scope, value, field string }

func aliases(kind string, n *sourceNode) []sourceAlias {
	var out []sourceAlias
	for _, field := range sourceColumns[kind] {
		value := nodeValue(n, field)
		if value == "" {
			continue
		}
		scope := ""
		if field == "thread_key" {
			scope = nodeValue(n, "thread_subtype")
			if scope == "" {
				continue
			}
		}
		a := sourceAlias{identifierNamespace(kind, field), scope, value, field}
		duplicate := false
		for _, old := range out {
			if old.namespace == a.namespace && old.scope == a.scope && old.value == a.value {
				duplicate = true
			}
		}
		if !duplicate {
			out = append(out, a)
		}
	}
	return out
}

// Only explicit source pairings unify identity. Different established IG user
// or long thread IDs are a conflict, not grounds for an automatic merge.
func resolveAliases(ctx context.Context, tx *sql.Tx, account, kind string, list []sourceAlias) (int64, []int64, error) {
	if len(list) == 0 {
		return 0, nil, nil
	}
	col := ownerColumn(kind)
	if col == "" {
		return 0, nil, errors.New("invalid identity kind")
	}
	owners := map[int64]bool{}
	for _, a := range list {
		var owner int64
		err := tx.QueryRowContext(ctx, `SELECT `+col+` FROM source_ids WHERE account_id=? AND namespace=? AND scope=? AND value=?`, account, a.namespace, a.scope, a.value).Scan(&owner)
		if err == nil {
			owners[owner] = true
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, nil, err
		}
	}
	if kind == "users" || kind == "threads" {
		primary := "user.igid"
		if kind == "threads" {
			primary = "thread.igid"
		}
		igids := map[string]bool{}
		for _, a := range list {
			if a.namespace == primary {
				igids[a.value] = true
			}
		}
		for owner := range owners {
			rows, err := tx.QueryContext(ctx, `SELECT value FROM source_ids WHERE account_id=? AND `+col+`=? AND namespace=?`, account, owner, primary)
			if err != nil {
				return 0, nil, err
			}
			for rows.Next() {
				var value string
				if err = rows.Scan(&value); err != nil {
					rows.Close()
					return 0, nil, err
				}
				igids[value] = true
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return 0, nil, err
			}
		}
		if len(igids) > 1 {
			return 0, nil, errIdentityConflict
		}
	}
	var root int64
	if len(owners) == 0 {
		res, err := tx.ExecContext(ctx, `INSERT INTO `+kind+`(account_id) VALUES(?)`, account)
		if err != nil {
			return 0, nil, err
		}
		root, err = res.LastInsertId()
		if err != nil {
			return 0, nil, err
		}
	} else {
		ids := make([]int64, 0, len(owners))
		for id := range owners {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		root = ids[0]
		if len(ids) > 1 {
			var revision State
			if err := ReadState(ctx, tx, account, "identity", "revision", &revision); err != nil {
				return 0, nil, err
			}
			revision.Revision++
			if err := SaveState(ctx, tx, account, "identity", "revision", revision); err != nil {
				return 0, nil, err
			}
		}
		for _, id := range ids[1:] {
			if _, err := tx.ExecContext(ctx, `UPDATE source_ids SET `+col+`=? WHERE account_id=? AND `+col+`=?`, root, account, id); err != nil {
				return 0, nil, err
			}
			// Keep every historical version immutable; flatten redirects to one hop.
			if _, err := tx.ExecContext(ctx, `UPDATE `+kind+` SET merged_into=? WHERE account_id=? AND (entity_id=? OR merged_into=?)`, root, account, id, id); err != nil {
				return 0, nil, err
			}
		}
	}
	var ids []int64
	for _, a := range list {
		var id int64
		err := tx.QueryRowContext(ctx, `INSERT INTO source_ids(account_id,namespace,scope,value,`+col+`,evidence_field) VALUES(?,?,?,?,?,?) ON CONFLICT(account_id,namespace,value,scope) DO UPDATE SET `+col+`=excluded.`+col+` RETURNING source_id`, account, a.namespace, a.scope, a.value, root, a.field).Scan(&id)
		if err != nil {
			return 0, nil, err
		}
		ids = append(ids, id)
	}
	return root, ids, nil
}

var errIdentityConflict = errors.New("conflicting established source identities; source retained without unification")

func (s *Store) ResolveEntity(ctx context.Context, account, kind, ref string) (int64, error) {
	if ownerColumn(kind) == "" {
		return 0, errors.New("invalid entity kind")
	}
	if strings.HasPrefix(ref, "entity:") {
		id, err := strconv.ParseInt(strings.TrimPrefix(ref, "entity:"), 10, 64)
		if err != nil || id <= 0 {
			return 0, errors.New("invalid local entity reference")
		}
		err = s.queryRowContext(ctx, `SELECT coalesce(merged_into,entity_id) FROM `+kind+` WHERE account_id=? AND entity_id=?`, account, id).Scan(&id)
		return id, err
	}
	field, value, err := qualified(kind, ref)
	if err != nil {
		return 0, err
	}
	rows, err := s.queryContext(ctx, `SELECT DISTINCT `+ownerColumn(kind)+` FROM source_ids WHERE account_id=? AND namespace=? AND value=?`, account, identifierNamespace(kind, field), value)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var id int64
	for rows.Next() {
		var next int64
		if err = rows.Scan(&next); err != nil {
			return 0, err
		}
		if id != 0 && id != next {
			return 0, errors.New("ambiguous source ID; use an entity reference")
		}
		id = next
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	if id == 0 {
		return 0, sql.ErrNoRows
	}
	return id, nil
}
func EntityRef(r Record) string {
	return fmt.Sprintf("igdm://accounts/%s/entities/%s/%d", r.Account, r.Table, r.Entity)
}

func duplicateIdentity(kind string, n *sourceNode) bool {
	for _, field := range sourceColumns[kind] {
		count := 0
		for _, m := range n.members {
			if m.key == field {
				count++
			}
		}
		if count > 1 {
			return true
		}
	}
	return false
}

// ResolveEntityTx uses the same identity snapshot as the acquisition commit.
func ResolveEntityTx(ctx context.Context, tx *sql.Tx, account, kind, ref string) (int64, error) {
	return (&Store{readTx: tx}).ResolveEntity(ctx, account, kind, ref)
}
