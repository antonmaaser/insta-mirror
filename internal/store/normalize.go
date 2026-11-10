package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"unicode/utf8"
)

// sourceNode deliberately retains duplicate members and number tokens. Maps and
// float64 decoding would discard source information before it reached SQLite.
type sourceMember struct {
	key        string
	encodedKey string
	value      *sourceNode
}
type sourceNode struct {
	shape    byte
	scalar   json.RawMessage
	members  []sourceMember
	elements []*sourceNode
}

// Parse directly from validated tokens: encoding/json's string decoder replaces
// unpaired surrogates and invalid UTF-8. Those unusual tokens must survive too.
func parseSource(raw []byte) (*sourceNode, error) {
	if !json.Valid(raw) {
		return nil, errors.New("invalid source JSON")
	}
	pos := 0
	return readSource(raw, &pos), nil
}
func whitespace(raw []byte, pos *int) {
	for *pos < len(raw) && strings.ContainsRune(" \t\r\n", rune(raw[*pos])) {
		*pos++
	}
}
func sourceString(raw []byte, pos *int) []byte {
	start := *pos
	*pos++
	for raw[*pos] != '"' {
		if raw[*pos] == '\\' {
			*pos++
		}
		*pos++
	}
	*pos++
	return raw[start:*pos]
}
func readSource(raw []byte, pos *int) *sourceNode {
	whitespace(raw, pos)
	n := &sourceNode{}
	switch raw[*pos] {
	case '{':
		n.shape = '{'
		*pos++
		whitespace(raw, pos)
		for raw[*pos] != '}' {
			keyRaw := sourceString(raw, pos)
			var key string
			_ = json.Unmarshal(keyRaw, &key)
			whitespace(raw, pos)
			*pos++ // colon
			value := readSource(raw, pos)
			n.members = append(n.members, sourceMember{key: key, encodedKey: canonicalString(keyRaw), value: value})
			whitespace(raw, pos)
			if raw[*pos] == ',' {
				*pos++
				whitespace(raw, pos)
			} else {
				break
			}
		}
		*pos++
		sort.SliceStable(n.members, func(i, j int) bool { return n.members[i].encodedKey < n.members[j].encodedKey })
	case '[':
		n.shape = '['
		*pos++
		whitespace(raw, pos)
		for raw[*pos] != ']' {
			n.elements = append(n.elements, readSource(raw, pos))
			whitespace(raw, pos)
			if raw[*pos] == ',' {
				*pos++
			} else {
				break
			}
			whitespace(raw, pos)
		}
		*pos++
	case '"':
		n.scalar = json.RawMessage(canonicalString(sourceString(raw, pos)))
	default:
		start := *pos
		for *pos < len(raw) && !strings.ContainsRune(",]} \t\r\n", rune(raw[*pos])) {
			*pos++
		}
		token := string(raw[start:*pos])
		if token != "null" && token != "true" && token != "false" {
			token = canonicalNumber(token)
		}
		n.scalar = json.RawMessage(token)
	}
	return n
}
func canonicalString(raw []byte) string {
	// Valid surrogate pairs decode normally; invalid pairs retain their source
	// spelling rather than being conflated with a real replacement character.
	valid := utf8.Valid(raw)
	for i := 1; i < len(raw)-1 && valid; i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if raw[i] != 'u' {
			continue
		}
		var unit uint16
		for _, c := range raw[i+1 : i+5] {
			unit = unit * 16
			switch {
			case c >= '0' && c <= '9':
				unit += uint16(c - '0')
			case c >= 'a' && c <= 'f':
				unit += uint16(c - 'a' + 10)
			default:
				unit += uint16(c - 'A' + 10)
			}
		}
		i += 4
		if unit >= 0xD800 && unit <= 0xDBFF {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				valid = false
				break
			}
			var low uint16
			for _, c := range raw[i+3 : i+7] {
				low = low * 16
				switch {
				case c >= '0' && c <= '9':
					low += uint16(c - '0')
				case c >= 'a' && c <= 'f':
					low += uint16(c - 'a' + 10)
				default:
					low += uint16(c - 'A' + 10)
				}
			}
			if low < 0xDC00 || low > 0xDFFF {
				valid = false
				break
			}
			i += 6
		} else if unit >= 0xDC00 && unit <= 0xDFFF {
			valid = false
		}
	}
	if !valid {
		return string(raw)
	}
	var value string
	_ = json.Unmarshal(raw, &value)
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
func canonicalNumber(token string) string {
	sign := ""
	if strings.HasPrefix(token, "-") {
		sign = "-"
		token = token[1:]
	}
	mantissa, exponent, hasExponent := strings.Cut(strings.ToLower(token), "e")
	power := new(big.Int)
	if hasExponent {
		power.SetString(exponent, 10)
	}
	if dot := strings.IndexByte(mantissa, '.'); dot >= 0 {
		power.Sub(power, big.NewInt(int64(len(mantissa)-dot-1)))
		mantissa = mantissa[:dot] + mantissa[dot+1:]
	}
	mantissa = strings.TrimLeft(mantissa, "0")
	if mantissa == "" {
		return "0"
	}
	trimmed := strings.TrimRight(mantissa, "0")
	power.Add(power, big.NewInt(int64(len(mantissa)-len(trimmed))))
	mantissa = trimmed
	if power.IsInt64() && power.Int64() >= -4096 && power.Int64() <= 4096 {
		exponent := int(power.Int64())
		if exponent >= 0 {
			return sign + mantissa + strings.Repeat("0", exponent)
		}
		point := len(mantissa) + exponent
		if point > 0 {
			return sign + mantissa[:point] + "." + mantissa[point:]
		}
		return sign + "0." + strings.Repeat("0", -point) + mantissa
	}
	return sign + mantissa + "e" + power.String()
}
func (n *sourceNode) bytes() []byte {
	if n.shape == 0 {
		return n.scalar
	}
	var b bytes.Buffer
	b.WriteByte(n.shape)
	if n.shape == '{' {
		for i, m := range n.members {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(m.encodedKey)
			b.WriteByte(':')
			b.Write(m.value.bytes())
		}
		b.WriteByte('}')
	} else {
		for i, v := range n.elements {
			if i > 0 {
				b.WriteByte(',')
			}
			b.Write(v.bytes())
		}
		b.WriteByte(']')
	}
	return b.Bytes()
}

type sourceLink struct {
	slot       int
	key        string
	encodedKey string
	child      int64
}

// Named kinds follow the upstream source vocabulary. Unrecognized composite
// fields are decomposed too, so future fields do not require a schema change.
func nestedKind(parent, field string, arrayElement bool) string {
	if arrayElement {
		return parent
	}
	switch field {
	case "users", "viewer", "user_dict", "usersWithoutViewer":
		return "users"
	case "message", "replied_to_message", "pinned_messages", "node":
		return "messages"
	case "as_ig_direct_thread":
		return "threads"
	case "attachments", "attachment", "videos", "ordered_photo_video_attachments", "audio_attachments", "animated_media":
		return "attachments"
	case "reactions", "reaction":
		return "reactions"
	case "mentions":
		return "mentions"
	case "slide_edit_history":
		return "edit_history"
	default:
		return field
	}
}

func putVersion(ctx context.Context, tx *sql.Tx, account, kind string, n *sourceNode) (int64, error) {
	canonical := n.bytes()
	fingerprint := "v1:" + Hash(canonical)
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT version_id FROM source_versions WHERE account_id=? AND kind=? AND content_hash=?`, account, kind, fingerprint).Scan(&id)
	if err == nil {
		restored, err := reconstruct(ctx, tx, account, id)
		if err != nil {
			return 0, err
		}
		if !bytes.Equal(restored, canonical) {
			return 0, errors.New("normalized content fingerprint collision")
		}
		return id, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	residual := &sourceNode{shape: n.shape}
	var links []sourceLink
	if n.shape == '{' {
		for slot, m := range n.members {
			if m.value.shape == 0 {
				residual.members = append(residual.members, m)
				continue
			}
			child, err := putVersion(ctx, tx, account, nestedKind(kind, m.key, false), m.value)
			if err != nil {
				return 0, err
			}
			links = append(links, sourceLink{slot: slot, key: m.key, encodedKey: m.encodedKey, child: child})
		}
	} else if n.shape == '[' {
		for slot, v := range n.elements {
			if v.shape == 0 {
				residual.elements = append(residual.elements, v)
				continue
			}
			child, err := putVersion(ctx, tx, account, nestedKind(kind, "", true), v)
			if err != nil {
				return 0, err
			}
			links = append(links, sourceLink{slot: slot, child: child})
		}
	} else {
		return 0, errors.New("source version must be an object or array")
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO source_versions(account_id,kind,content_hash,residual) VALUES(?,?,?,?)`, account, kind, fingerprint, string(residual.bytes()))
	if err != nil {
		return 0, err
	}
	id, err = result.LastInsertId()
	if err != nil {
		return 0, err
	}
	for _, l := range links {
		if _, err = tx.ExecContext(ctx, `INSERT INTO source_links(account_id,parent_id,slot,field,encoded_key,child_id) VALUES(?,?,?,?,?,?)`, account, id, l.slot, l.key, l.encodedKey, l.child); err != nil {
			return 0, err
		}
	}
	return id, nil
}

type sourceQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// One recursive query loads a version graph. Reconstruction does not require
// additional connections while a read snapshot is held.
func reconstruct(ctx context.Context, q sourceQuerier, account string, id int64) ([]byte, error) {
	rows, err := q.QueryContext(ctx, `WITH RECURSIVE graph(id) AS (
 SELECT version_id FROM source_versions WHERE account_id=? AND version_id=?
 UNION SELECT l.child_id FROM source_links l JOIN graph g ON l.parent_id=g.id WHERE l.account_id=?
 ) SELECT v.version_id,v.residual,l.slot,l.field,l.encoded_key,l.child_id FROM graph g JOIN source_versions v ON v.version_id=g.id LEFT JOIN source_links l ON l.parent_id=v.version_id AND l.account_id=v.account_id ORDER BY v.version_id,l.slot`, account, id, account)
	if err != nil {
		return nil, err
	}
	nodes := map[int64]*sourceNode{}
	links := map[int64][]sourceLink{}
	for rows.Next() {
		var vid int64
		var residual string
		var slot, child sql.NullInt64
		var field, encodedKey sql.NullString
		if err = rows.Scan(&vid, &residual, &slot, &field, &encodedKey, &child); err != nil {
			rows.Close()
			return nil, err
		}
		if nodes[vid] == nil {
			nodes[vid], err = parseSource([]byte(residual))
			if err != nil {
				rows.Close()
				return nil, err
			}
		}
		if child.Valid {
			links[vid] = append(links[vid], sourceLink{slot: int(slot.Int64), key: field.String, encodedKey: encodedKey.String, child: child.Int64})
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if nodes[id] == nil {
		return nil, sql.ErrNoRows
	}
	done := map[int64]bool{}
	active := map[int64]bool{}
	var build func(int64) error
	build = func(vid int64) error {
		if done[vid] {
			return nil
		}
		if active[vid] {
			return errors.New("cyclic normalized source")
		}
		active[vid] = true
		n := nodes[vid]
		if n == nil {
			return errors.New("missing normalized child")
		}
		for _, l := range links[vid] {
			if err := build(l.child); err != nil {
				return err
			}
			if n.shape == '{' {
				if l.slot < 0 || l.slot > len(n.members) {
					return fmt.Errorf("invalid source member slot %d", l.slot)
				}
				n.members = append(n.members, sourceMember{})
				copy(n.members[l.slot+1:], n.members[l.slot:])
				n.members[l.slot] = sourceMember{key: l.key, encodedKey: l.encodedKey, value: nodes[l.child]}
			} else if n.shape == '[' {
				if l.slot < 0 || l.slot > len(n.elements) {
					return fmt.Errorf("invalid source element slot %d", l.slot)
				}
				n.elements = append(n.elements, nil)
				copy(n.elements[l.slot+1:], n.elements[l.slot:])
				n.elements[l.slot] = nodes[l.child]
			} else {
				return errors.New("invalid normalized container")
			}
		}
		delete(active, vid)
		done[vid] = true
		return nil
	}
	if err = build(id); err != nil {
		return nil, err
	}
	return nodes[id].bytes(), nil
}

func (s *Store) hydrate(ctx context.Context, r *Record) error {
	var err error
	if s.readTx != nil {
		r.Raw, err = reconstruct(ctx, s.readTx, r.Account, r.Version)
	} else {
		r.Raw, err = reconstruct(ctx, s.DB, r.Account, r.Version)
	}
	r.Representation = "reconstructed_source"
	if err != nil {
		return err
	}
	rows, err := s.queryContext(ctx, `SELECT p.origin,p.acquired_thread_id,p.source_hash,p.observed_at_ms,p.last_observed_at_ms FROM source_acquisitions p JOIN source_records r ON r.record_id=p.record_id WHERE r.account_id=? AND r.record_id=? ORDER BY p.acquisition_id`, r.Account, r.RowID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var a Acquisition
		if err = rows.Scan(&a.Origin, &a.Thread, &a.Hash, &a.Observed, &a.LastObserved); err != nil {
			return err
		}
		r.Acquisitions = append(r.Acquisitions, a)
	}
	return rows.Err()
}
