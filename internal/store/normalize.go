package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/big"
	"sort"
	"strconv"
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

// Paths use member ordinals (including duplicate keys) and array positions.
// A null hole in the residual is replaced only when a child relation exists.
type sourceLink struct {
	path          []int
	field         string
	child, source int64
}

func pathAppend(path []int, i int) []int { return append(append([]int(nil), path...), i) }
func joinField(parent, field string) string {
	if parent == "" {
		return field
	}
	return parent + "." + field
}

func extractedKind(owner, field, path string) string {
	if path != field && !(owner == "threads" && path == "slide_messages.edges.node") && !(owner == "messages" && path == "sender.user_dict") && !(owner == "deltas" && path == "thread.as_ig_direct_thread") && !(owner == "mailbox_pages" && (path == "threads_by_folder.edges.node.as_ig_direct_thread" || path == "pinned_threads_v2.as_ig_direct_thread")) {
		return ""
	}
	switch owner {
	case "mailbox_pages":
		if field == "as_ig_direct_thread" {
			return "threads"
		}
	case "threads":
		switch field {
		case "users", "viewer", "usersWithoutViewer", "group_creator":
			return "users"
		case "node", "pinned_messages":
			return "messages"
		case "slide_read_receipts":
			return "read_receipts"
		case "nicknames":
			return "nicknames"
		}
	case "messages":
		switch field {
		case "user_dict":
			if path == "sender.user_dict" {
				return "users"
			}
		case "replied_to_message":
			return "messages"
		case "content":
			return "content"
		case "reactions":
			return "reactions"
		case "mentions":
			return "mentions"
		case "slide_edit_history":
			return "edit_history"
		}
	case "deltas":
		switch field {
		case "message":
			return "messages"
		case "reaction":
			return "reactions"
		case "read_receipt":
			return "read_receipts"
		case "as_ig_direct_thread":
			return "threads"
		}
	case "content":
		switch field {
		case "xma":
			return "xma"
		case "attachments", "attachment", "videos", "ordered_photo_video_attachments", "audio_attachments", "animated_media":
			return "attachments"
		}
	case "xma":
		switch field {
		case "preview_image", "xmaPreviewImage", "header_icon":
			return "media_descriptors"
		case "cta_buttons":
			return "cta_buttons"
		}
	}
	return ""
}
func referenceKind(owner, field string, n *sourceNode) (string, string) {
	switch owner {
	case "messages":
		switch field {
		case "thread_fbid":
			return "threads", field
		case "sender_fbid":
			return "users", field
		case "replied_to_message_id":
			return "messages", field
		}
	case "deltas":
		switch field {
		case "thread_fbid":
			return "threads", field
		case "message_id":
			return "messages", field
		case "actor_fbid", "left_participant_fbid", "viewer_fbid":
			return "users", field
		}
	case "reactions":
		switch field {
		case "sender_fbid":
			return "users", field
		case "log_message_id":
			return "messages", field
		}
	case "read_receipts":
		if field == "participant_fbid" {
			return "users", field
		}
	case "nicknames":
		if field == "eimu_id" {
			return "users", field
		} else if field == "igid" {
			return "users", "id"
		}
	case "mentions":
		if field == "user_fbid" && nodeValue(n, "profile_range_type") == "PROFILE" {
			return "users", "sender_fbid"
		}
	}
	return "", ""
}

func putVersion(ctx context.Context, tx *sql.Tx, account, kind string, n *sourceNode) (int64, error) {
	canonical := n.bytes()
	fingerprint := "v2:" + Hash(canonical)
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
	err = nil
	var owner int64
	var ownIDs []int64
	identityError := ""
	if kind == "thread_boundaries" {
		identityError = "thread response unavailable; retained as an acquisition boundary, not an empty conversation"
	}

	if ownerColumn(kind) != "" {
		if duplicateIdentity(kind, n) {
			identityError = "duplicate source identity fields; content retained without unification"
		} else {
			sourceAliases := aliases(kind, n)
			if len(sourceAliases) == 0 {
				identityError = "source object has no usable identity; content retained without entity assignment"
			}
			owner, ownIDs, err = resolveAliases(ctx, tx, account, kind, sourceAliases)
		}
		if errors.Is(err, errIdentityConflict) {
			identityError = err.Error()
		} else if err != nil {
			return 0, err
		}
	}
	var links []sourceLink
	var walk func(*sourceNode, []int, string, string) (*sourceNode, error)
	walk = func(node *sourceNode, path []int, field, extract string) (*sourceNode, error) {
		if node.shape == '{' && extract != "" && len(node.members) > 0 {
			child, err := putVersion(ctx, tx, account, extract, node)
			if err != nil {
				return nil, err
			}
			links = append(links, sourceLink{path: path, field: field, child: child})
			return &sourceNode{scalar: json.RawMessage("null")}, nil
		}
		residual := &sourceNode{shape: node.shape, scalar: node.scalar}
		if node.shape == '{' {
			// A sender wrapper or nickname explicitly pairs IG and messaging IDs,
			// but does not require another user/profile content record.
			if field == "sender" || kind == "nicknames" {
				igKey, msgKey := "igid", "eimu_id"
				if field == "sender" {
					msgKey = "id"
				}
				var paired []sourceAlias
				if v := nodeValue(node, igKey); v != "" {
					paired = append(paired, sourceAlias{"user.igid", "", v, joinField(field, igKey)})
				}
				if v := nodeValue(node, msgKey); v != "" {
					paired = append(paired, sourceAlias{"user.messaging_fbid", "", v, joinField(field, msgKey)})
				}
				if len(paired) > 1 {
					_, _, err := resolveAliases(ctx, tx, account, "users", paired)
					if err != nil && !errors.Is(err, errIdentityConflict) {
						return nil, err
					}
					if err != nil {
						identityError = err.Error()
					}
				}
			}
			for i, m := range node.members {
				if kind == "mailbox_pages" && m.key == "as_ig_direct_thread" && string(m.value.scalar) == "null" && extractedKind(kind, m.key, joinField(field, m.key)) == "threads" {
					identityError = "thread source unavailable; source boundary retained without assuming an empty conversation"
				}

				p := pathAppend(path, i)
				f := joinField(field, m.key)
				extractKind := extractedKind(kind, m.key, f)
				child, err := walk(m.value, p, f, extractKind)
				if err != nil {
					return nil, err
				}
				residual.members = append(residual.members, sourceMember{key: m.key, encodedKey: m.encodedKey, value: child})
				if m.value.shape == 0 {
					rk, rf := "", ""
					if field == "" {
						rk, rf = referenceKind(kind, m.key, node)
					}
					if field == "sender" {
						if m.key == "id" {
							rk, rf = "users", "sender_fbid"
						} else if m.key == "igid" {
							rk, rf = "users", "id"
						}
					}
					if rk != "" {
						if value := nodeID(m.value); value != "" {
							a := sourceAlias{identifierNamespace(rk, rf), "", value, f}
							_, ids, err := resolveAliases(ctx, tx, account, rk, []sourceAlias{a})
							if err != nil {
								return nil, err
							}
							links = append(links, sourceLink{path: p, field: f, source: ids[0]})
						}
					}
				}
			}
		} else if node.shape == '[' {
			for i, v := range node.elements {
				child, err := walk(v, pathAppend(path, i), field, extract)
				if err != nil {
					return nil, err
				}
				residual.elements = append(residual.elements, child)
				if value := nodeID(v); value != "" {
					rk, rf := "", ""
					if kind == "threads" || kind == "deltas" {
						switch field {
						case "admin_user_ids", "admin_igids":
							rk, rf = "users", "id"
						case "pinned_messages", "unpinned_messages":
							rk, rf = "messages", "id"
						}
					}
					if rk != "" {
						_, ids, err := resolveAliases(ctx, tx, account, rk, []sourceAlias{{namespace: identifierNamespace(rk, rf), value: value, field: field}})
						if err != nil {
							return nil, err
						}
						links = append(links, sourceLink{path: pathAppend(path, i), field: field, source: ids[0]})
					}
				}
			}
		}
		return residual, nil
	}
	residual, err := walk(n, nil, "", "")
	if err != nil {
		return 0, err
	}
	var u, t, m any
	switch kind {
	case "users":
		if owner != 0 {
			u = owner
		}
	case "threads":
		if owner != 0 {
			t = owner
		}
	case "messages":
		if owner != 0 {
			m = owner
		}
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO source_versions(account_id,kind,user_entity_id,thread_entity_id,message_entity_id,content_hash,residual,source_error) VALUES(?,?,?,?,?,?,?,?)`, account, kind, u, t, m, fingerprint, string(residual.bytes()), identityError)
	if err != nil {
		return 0, err
	}
	id, err = result.LastInsertId()
	if err != nil {
		return 0, err
	}
	for _, l := range links {
		path, _ := json.Marshal(l.path)
		var child, source any
		if l.child != 0 {
			child = l.child
		} else {
			source = l.source
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO source_relations(account_id,parent_id,path,field,child_id,source_id) VALUES(?,?,?,?,?,?)`, account, id, string(path), l.field, child, source); err != nil {
			return 0, err
		}
		if l.source != 0 {
			if _, err = tx.ExecContext(ctx, `UPDATE source_ids SET evidence_version_id=?,evidence_field=? WHERE source_id=? AND evidence_version_id IS NULL`, id, l.field, l.source); err != nil {
				return 0, err
			}
		}
	}
	for _, alias := range ownIDs {
		if _, err = tx.ExecContext(ctx, `UPDATE source_ids SET evidence_version_id=? WHERE source_id=? AND evidence_version_id IS NULL`, id, alias); err != nil {
			return 0, err
		}
	}
	return id, nil
}

type sourceQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func reconstruct(ctx context.Context, q sourceQuerier, account string, id int64) ([]byte, error) {
	rows, err := q.QueryContext(ctx, `WITH RECURSIVE graph(id) AS (
 SELECT version_id FROM source_versions WHERE account_id=? AND version_id=?
 UNION SELECT r.child_id FROM source_relations r JOIN graph g ON r.parent_id=g.id WHERE r.account_id=? AND r.child_id IS NOT NULL
 ) SELECT v.version_id,v.residual,r.path,r.child_id FROM graph g JOIN source_versions v ON v.version_id=g.id LEFT JOIN source_relations r ON r.parent_id=v.version_id AND r.child_id IS NOT NULL ORDER BY v.version_id,r.path`, account, id, account)
	if err != nil {
		return nil, err
	}
	nodes := map[int64]*sourceNode{}
	links := map[int64][]sourceLink{}
	for rows.Next() {
		var vid int64
		var residual string
		var path sql.NullString
		var child sql.NullInt64
		if err = rows.Scan(&vid, &residual, &path, &child); err != nil {
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
			var p []int
			if err = json.Unmarshal([]byte(path.String), &p); err != nil {
				rows.Close()
				return nil, err
			}
			links[vid] = append(links[vid], sourceLink{path: p, child: child.Int64})
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
		for _, l := range links[vid] {
			if err := build(l.child); err != nil {
				return err
			}
			n := nodes[vid]
			if n == nil || nodes[l.child] == nil {
				return errors.New("missing normalized source")
			}
			for i, slot := range l.path {
				var dest **sourceNode
				if n.shape == '{' && slot >= 0 && slot < len(n.members) {
					dest = &n.members[slot].value
				} else if n.shape == '[' && slot >= 0 && slot < len(n.elements) {
					dest = &n.elements[slot]
				} else {
					return errors.New("invalid normalized source location")
				}
				if i == len(l.path)-1 {
					*dest = nodes[l.child]
				} else {
					n = *dest
				}
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
	if err != nil {
		return err
	}
	r.Representation = "reconstructed_source"
	r.HashScope = "representative_acquisition_root"
	if ownerColumn(r.Table) != "" {
		if err = s.queryRowContext(ctx, `SELECT entity_id FROM `+r.Table+`_versions WHERE account_id=? AND version_id=?`, r.Account, r.Version).Scan(&r.Entity); err != nil {
			return err
		}
		r.EntityReference = EntityRef(*r)
	}
	rows, err := s.queryContext(ctx, `WITH RECURSIVE ancestors(id) AS (SELECT version_id FROM source_versions WHERE account_id=? AND version_id=? UNION SELECT r.parent_id FROM source_relations r JOIN ancestors a ON r.child_id=a.id) SELECT DISTINCT o.observation_id,o.version_id,root.kind,o.event_identity,o.origin,o.acquired_thread_id,o.source_hash,o.observed_at_ms,o.last_observed_at_ms FROM ancestors a JOIN source_observations o ON o.version_id=a.id JOIN source_versions root ON root.version_id=o.version_id WHERE o.account_id=? AND (?=0 OR o.observation_id<=?) ORDER BY o.observation_id DESC LIMIT 33`, r.Account, r.Version, r.Account, r.watermark, r.watermark)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var a Acquisition
		if err = rows.Scan(&a.ID, &a.RootVersion, &a.RootKind, &a.Event, &a.Origin, &a.Thread, &a.Hash, &a.Observed, &a.LastObserved); err != nil {
			return err
		}
		if len(r.Acquisitions) == 32 {
			r.AcquisitionsTruncated = true
			break
		}

		if a.RootKind == "mailbox_pages" || a.RootKind == "thread_boundaries" {
			a.ExcludedFields = []string{"__token"}
		}
		r.Acquisitions = append(r.Acquisitions, a)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	basisEvent := ""
	var rootID int64
	var rootKind string
	if r.Observation != 0 {
		full := true
		if r.Table == "messages" {
			err = s.queryRowContext(ctx, `SELECT o.event_identity,o.version_id,root.kind,`+fullMessageWitness("v.version_id", "o")+` FROM source_observations o JOIN source_versions root ON root.version_id=o.version_id JOIN source_versions v ON v.version_id=? WHERE o.account_id=? AND o.observation_id=?`, r.Version, r.Account, r.Observation).Scan(&basisEvent, &rootID, &rootKind, &full)
			r.PartialMessage = !full
		} else {
			err = s.queryRowContext(ctx, `SELECT o.event_identity,o.version_id,root.kind FROM source_observations o JOIN source_versions root ON root.version_id=o.version_id WHERE o.account_id=? AND o.observation_id=?`, r.Account, r.Observation).Scan(&basisEvent, &rootID, &rootKind)
		}
		if err != nil {
			return err
		}
		r.RootReference = SourceRef(Record{Account: r.Account, Table: rootKind, RowID: rootID})
		if r.Table == "deltas" || r.Table == "messages" && full {
			if seq, ok := strings.CutPrefix(basisEvent, "uq_seq_id:"); ok {
				r.Sequence, _ = strconv.ParseInt(seq, 10, 64)
			}
		}
	}
	event := basisEvent
	if event == "" && r.Table == "deltas" {
		event = Identity("deltas", r.Raw)
	}
	event, _, _ = strings.Cut(event, ":conflict:")
	if strings.HasPrefix(event, "uq_seq_id:") {
		if _, parseErr := strconv.ParseInt(strings.TrimPrefix(event, "uq_seq_id:"), 10, 64); parseErr == nil {
			if err = s.queryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM source_observations WHERE account_id=? AND event_identity GLOB ?)`, r.Account, event+":conflict:*").Scan(&r.Conflict); err != nil {
				return err
			}
		}
	}
	if r.Table == "messages" && r.Conflict {
		r.Sequence = 0
	}
	return nil
}

// Subscription tokens are acquisition credentials, not archive content. Keep
// the surrounding page, wrappers and ordered relationships without this field.
func stripMailboxToken(node *sourceNode) {
	members := node.members[:0]
	for _, m := range node.members {
		if m.key != "__token" {
			members = append(members, m)
		}
	}
	node.members = members
}
