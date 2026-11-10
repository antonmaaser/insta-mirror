package store

import (
	"database/sql"
	"fmt"
	"strings"
)

// These are source indexes, not a second canonical message model. Scalar data
// remains in the version's residual; views expose it without duplicating it.
var indexedFields = map[string][]string{
	"threads":  {"id", "thread_id", "thread_fbid", "thread_key", "thread_subtype", "system_folder", "last_activity_timestamp_ms", "thread_title"},
	"messages": {"id", "message_id", "thread_fbid", "sender_fbid", "timestamp_ms", "replied_to_message_id", "offline_threading_id", "text_body", "igd_snippet"},
	"users":    {"id", "pk", "fbid_v2", "interop_messaging_user_fbid", "username", "full_name"},
	"deltas":   {"uq_seq_id", "__typename", "thread_fbid", "message_id", "text_body"},
}

func childResidual(parent, field string) string {
	return `(SELECT c.residual FROM source_links l JOIN source_versions c ON c.version_id=l.child_id AND c.account_id=l.account_id WHERE l.parent_id=` + parent + ` AND l.field='` + field + `' ORDER BY l.slot LIMIT 1)`
}
func createSchema(tx *sql.Tx) error {
	_, err := tx.Exec(`
 CREATE TABLE source_versions (
 version_id INTEGER PRIMARY KEY, account_id TEXT NOT NULL, kind TEXT NOT NULL,
 content_hash TEXT NOT NULL, residual TEXT NOT NULL CHECK(json_valid(residual) AND json_type(residual) IN ('object','array')),
 UNIQUE(account_id,kind,content_hash), UNIQUE(account_id,version_id));
 CREATE TABLE source_links (
 account_id TEXT NOT NULL, parent_id INTEGER NOT NULL, slot INTEGER NOT NULL CHECK(slot>=0), field TEXT NOT NULL, encoded_key TEXT NOT NULL, child_id INTEGER NOT NULL,
 PRIMARY KEY(parent_id,slot), FOREIGN KEY(account_id,parent_id) REFERENCES source_versions(account_id,version_id), FOREIGN KEY(account_id,child_id) REFERENCES source_versions(account_id,version_id));
 CREATE TABLE source_records (
 record_id INTEGER PRIMARY KEY, account_id TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('threads','messages','users','deltas')), source_identity TEXT NOT NULL, version_id INTEGER NOT NULL,
 observed_at_ms INTEGER NOT NULL, last_observed_at_ms INTEGER NOT NULL,
 UNIQUE(account_id,kind,source_identity,version_id), UNIQUE(account_id,record_id), FOREIGN KEY(account_id,version_id) REFERENCES source_versions(account_id,version_id));
 CREATE INDEX records_account_kind ON source_records(account_id,kind,record_id);
 CREATE TABLE source_acquisitions (
 acquisition_id INTEGER PRIMARY KEY, record_id INTEGER NOT NULL REFERENCES source_records(record_id), origin TEXT NOT NULL, acquired_thread_id TEXT NOT NULL, source_hash TEXT NOT NULL,
 observed_at_ms INTEGER NOT NULL, last_observed_at_ms INTEGER NOT NULL,
 UNIQUE(record_id,origin,acquired_thread_id,source_hash));
 CREATE INDEX acquisitions_thread ON source_acquisitions(acquired_thread_id,record_id);
 CREATE TRIGGER immutable_versions_update BEFORE UPDATE ON source_versions BEGIN SELECT RAISE(ABORT,'source versions are immutable'); END;
 CREATE TRIGGER immutable_versions_delete BEFORE DELETE ON source_versions BEGIN SELECT RAISE(ABORT,'source versions are immutable'); END;
 CREATE TRIGGER immutable_links_update BEFORE UPDATE ON source_links BEGIN SELECT RAISE(ABORT,'source relationships are immutable'); END;
 CREATE TRIGGER immutable_links_delete BEFORE DELETE ON source_links BEGIN SELECT RAISE(ABORT,'source relationships are immutable'); END;
 CREATE TABLE sync_state(account_id TEXT NOT NULL, scope TEXT NOT NULL, scope_id TEXT NOT NULL, state TEXT NOT NULL CHECK(json_valid(state)), PRIMARY KEY(account_id,scope,scope_id));`)
	if err != nil {
		return err
	}
	for table, fields := range indexedFields {
		query := `CREATE VIEW ` + table + ` AS SELECT r.*,v.content_hash,p.acquired_thread_id,p.origin,p.source_hash`
		for _, field := range fields {
			expr := `CASE WHEN json_type(v.residual,'$.` + field + `') IN ('integer','real') THEN v.residual -> '$.` + field + `' ELSE CAST(json_extract(v.residual,'$.` + field + `') AS TEXT) END`
			query += `,` + expr + ` AS ` + field
			// Expression indexes keep useful source fields queryable without another copy.
			if _, err = tx.Exec(`CREATE INDEX versions_` + table + `_` + field + ` ON source_versions(account_id,` + strings.ReplaceAll(expr, "v.residual", "residual") + `) WHERE kind='` + table + `'`); err != nil {
				return fmt.Errorf("source index %s.%s: %w", table, field, err)
			}
		}
		content := childResidual("v.version_id", "content")
		if table == "messages" {
			query += `,json_extract(` + content + `,'$.__typename') AS content_type,json_type(` + content + `,'$.is_reaction_action_log') AS reaction_type`
			query += `,json_extract(` + content + `,'$.text_body') AS content_text,json_extract(` + content + `,'$.xma_text_body') AS xma_text`
			query += `,json_extract(` + childResidual(`(SELECT l.child_id FROM source_links l WHERE l.parent_id=v.version_id AND l.field='content' ORDER BY l.slot LIMIT 1)`, "xma") + `,'$.caption_body_text') AS caption_text`
		}
		if table == "deltas" {
			message := childResidual("v.version_id", "message")
			query += `,json_extract(` + message + `,'$.id') AS nested_message_id,json_extract(` + message + `,'$.message_id') AS nested_message_source_id,json_extract(` + childResidual("v.version_id", "reaction") + `,'$.log_message_id') AS reaction_log_message_id`
		}
		query += ` FROM source_records r JOIN source_versions v ON v.version_id=r.version_id AND v.account_id=r.account_id JOIN source_acquisitions p ON p.acquisition_id=(SELECT acquisition_id FROM source_acquisitions WHERE record_id=r.record_id ORDER BY last_observed_at_ms DESC,acquisition_id DESC LIMIT 1) WHERE r.kind='` + table + `'`
		if _, err = tx.Exec(query); err != nil {
			return err
		}
	}
	_, err = tx.Exec(fmt.Sprintf("PRAGMA user_version=%d", schemaVersion))
	return err
}
