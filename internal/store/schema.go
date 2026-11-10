package store

import (
	"database/sql"
	"fmt"
)

// Identity tables contain no second copy of source content. A version owns its
// scalar residual and relations; wrappers and empty containers stay inline.
func createSchema(tx *sql.Tx) error {
	for _, kind := range []string{"users", "threads", "messages"} {
		if _, err := tx.Exec(`CREATE TABLE ` + kind + ` (
 entity_id INTEGER PRIMARY KEY, account_id TEXT NOT NULL, merged_into INTEGER,
 UNIQUE(account_id,entity_id), CHECK(merged_into IS NULL OR merged_into<entity_id),
 FOREIGN KEY(account_id,merged_into) REFERENCES ` + kind + `(account_id,entity_id));`); err != nil {
			return err
		}
	}
	_, err := tx.Exec(`
 CREATE TABLE source_versions (
 version_id INTEGER PRIMARY KEY, account_id TEXT NOT NULL, kind TEXT NOT NULL,
 user_entity_id INTEGER, thread_entity_id INTEGER, message_entity_id INTEGER,
 content_hash TEXT NOT NULL, residual TEXT NOT NULL CHECK(json_valid(residual)),
 source_error TEXT NOT NULL DEFAULT '',
 CHECK((user_entity_id IS NOT NULL)+(thread_entity_id IS NOT NULL)+(message_entity_id IS NOT NULL)<=1),
 CHECK(user_entity_id IS NULL OR kind='users'), CHECK(thread_entity_id IS NULL OR kind='threads'), CHECK(message_entity_id IS NULL OR kind='messages'),
 UNIQUE(account_id,kind,content_hash), UNIQUE(account_id,version_id),
 FOREIGN KEY(account_id,user_entity_id) REFERENCES users(account_id,entity_id),
 FOREIGN KEY(account_id,thread_entity_id) REFERENCES threads(account_id,entity_id),
 FOREIGN KEY(account_id,message_entity_id) REFERENCES messages(account_id,entity_id));
 CREATE TABLE source_ids (
 source_id INTEGER PRIMARY KEY, account_id TEXT NOT NULL, namespace TEXT NOT NULL, scope TEXT NOT NULL DEFAULT '', value TEXT NOT NULL,
 user_entity_id INTEGER, thread_entity_id INTEGER, message_entity_id INTEGER,
 evidence_version_id INTEGER, evidence_field TEXT NOT NULL DEFAULT '',
 CHECK((user_entity_id IS NOT NULL)+(thread_entity_id IS NOT NULL)+(message_entity_id IS NOT NULL)=1),
 UNIQUE(account_id,namespace,value,scope), UNIQUE(account_id,source_id),
 FOREIGN KEY(account_id,user_entity_id) REFERENCES users(account_id,entity_id),
 FOREIGN KEY(account_id,thread_entity_id) REFERENCES threads(account_id,entity_id),
 FOREIGN KEY(account_id,message_entity_id) REFERENCES messages(account_id,entity_id),
 FOREIGN KEY(account_id,evidence_version_id) REFERENCES source_versions(account_id,version_id));
 CREATE INDEX ids_user ON source_ids(account_id,user_entity_id,namespace) WHERE user_entity_id IS NOT NULL;
 CREATE INDEX ids_thread ON source_ids(account_id,thread_entity_id,namespace) WHERE thread_entity_id IS NOT NULL;
 CREATE INDEX ids_message ON source_ids(account_id,message_entity_id,namespace) WHERE message_entity_id IS NOT NULL;
 CREATE INDEX versions_user ON source_versions(account_id,user_entity_id) WHERE user_entity_id IS NOT NULL;
 CREATE INDEX versions_thread ON source_versions(account_id,thread_entity_id) WHERE thread_entity_id IS NOT NULL;
 CREATE INDEX versions_message ON source_versions(account_id,message_entity_id) WHERE message_entity_id IS NOT NULL;
 CREATE INDEX versions_kind ON source_versions(account_id,kind,version_id);
 CREATE TABLE source_relations (
 account_id TEXT NOT NULL, parent_id INTEGER NOT NULL,
 path TEXT NOT NULL CHECK(json_valid(path)), field TEXT NOT NULL,
 child_id INTEGER, source_id INTEGER,
 PRIMARY KEY(parent_id,path), CHECK((child_id IS NOT NULL)+(source_id IS NOT NULL)=1),
 FOREIGN KEY(account_id,parent_id) REFERENCES source_versions(account_id,version_id),
 FOREIGN KEY(account_id,child_id) REFERENCES source_versions(account_id,version_id),
 FOREIGN KEY(account_id,source_id) REFERENCES source_ids(account_id,source_id));
 CREATE INDEX relations_child ON source_relations(child_id,parent_id) WHERE child_id IS NOT NULL;
 CREATE INDEX relations_source ON source_relations(source_id,parent_id) WHERE source_id IS NOT NULL;
 CREATE INDEX relations_field ON source_relations(parent_id,field);
 CREATE TABLE source_observations (
 observation_id INTEGER PRIMARY KEY, account_id TEXT NOT NULL, version_id INTEGER NOT NULL,
 origin TEXT NOT NULL, context TEXT NOT NULL, acquired_thread_id TEXT NOT NULL,
 source_hash TEXT NOT NULL, observed_at_ms INTEGER NOT NULL, last_observed_at_ms INTEGER NOT NULL,
 event_identity TEXT NOT NULL DEFAULT '',
 FOREIGN KEY(account_id,version_id) REFERENCES source_versions(account_id,version_id));
 CREATE INDEX observation_runs ON source_observations(account_id,origin,context,observation_id DESC);
 CREATE INDEX observation_versions ON source_observations(account_id,version_id,observation_id);
 CREATE UNIQUE INDEX observation_events ON source_observations(account_id,event_identity) WHERE event_identity!='';
 CREATE TABLE sync_state(account_id TEXT NOT NULL,scope TEXT NOT NULL,scope_id TEXT NOT NULL,state TEXT NOT NULL CHECK(json_valid(state)),PRIMARY KEY(account_id,scope,scope_id));
 CREATE TRIGGER immutable_versions_update BEFORE UPDATE ON source_versions BEGIN SELECT RAISE(ABORT,'source versions are immutable'); END;
 CREATE TRIGGER immutable_versions_delete BEFORE DELETE ON source_versions BEGIN SELECT RAISE(ABORT,'source versions are immutable'); END;
 CREATE TRIGGER immutable_relations_update BEFORE UPDATE ON source_relations BEGIN SELECT RAISE(ABORT,'source relations are immutable'); END;
 CREATE TRIGGER immutable_relations_delete BEFORE DELETE ON source_relations BEGIN SELECT RAISE(ABORT,'source relations are immutable'); END;
 CREATE VIEW source_evidence AS
 WITH RECURSIVE evidence(account_id,version_id,observation_id) AS (
 SELECT account_id,version_id,observation_id FROM source_observations
 UNION SELECT r.account_id,r.child_id,e.observation_id FROM evidence e JOIN source_relations r ON r.account_id=e.account_id AND r.parent_id=e.version_id WHERE r.child_id IS NOT NULL
 ) SELECT * FROM evidence;
 CREATE VIEW source_visible AS SELECT e.account_id,e.version_id,max(e.observation_id) AS observation_id,
 min(o.observed_at_ms) AS observed_at_ms,max(o.last_observed_at_ms) AS last_observed_at_ms
 FROM source_evidence e JOIN source_observations o ON o.observation_id=e.observation_id GROUP BY e.account_id,e.version_id;
 `)
	if err != nil {
		return err
	}
	for kind, fields := range sourceFields {
		owner := "0"
		if kind != "deltas" {
			col := ownerColumn(kind)
			owner = `coalesce((SELECT coalesce(merged_into,entity_id) FROM ` + kind + ` WHERE account_id=v.account_id AND entity_id=v.` + col + `),0)`
		}
		query := `CREATE VIEW ` + kind + `_versions AS SELECT v.*,v.version_id AS record_id,` + owner + ` AS entity_id,`
		if kind == "deltas" {
			query += `coalesce('uq_seq_id:'||json_extract(v.residual,'$.uq_seq_id'),'unsequenced:'||v.version_id)`
		} else {
			query += `'entity:'||` + owner
		}
		query += ` AS source_identity`
		for _, field := range fields {
			expr := scalarSQL("v.residual", field)
			query += `,` + expr + ` AS ` + field
		}
		if kind == "messages" {
			content := childResidual("v.version_id", "content")
			query += `,json_extract(` + content + `,'$.__typename') AS content_type,json_type(` + content + `,'$.is_reaction_action_log') AS reaction_type,json_extract(` + content + `,'$.text_body') AS content_text,json_extract(` + content + `,'$.xma_text_body') AS xma_text,json_extract(` + childResidual("v.version_id", "content.xma") + `,'$.caption_body_text') AS caption_text`
		}
		if kind == "deltas" {
			msg := childResidual("v.version_id", "message")
			query += `,json_extract(` + msg + `,'$.id') AS nested_message_id,json_extract(` + msg + `,'$.message_id') AS nested_message_source_id,json_extract(` + childResidual("v.version_id", "reaction") + `,'$.log_message_id') AS reaction_log_message_id`
		}
		if _, err = tx.Exec(query + ` FROM source_versions v WHERE v.kind='` + kind + `'`); err != nil {
			return err
		}
		if _, err = tx.Exec(`CREATE VIEW ` + kind + `_sources AS SELECT v.*,p.origin,p.acquired_thread_id,p.source_hash,p.observation_id,e.observed_at_ms,e.last_observed_at_ms FROM ` + kind + `_versions v JOIN source_visible e ON e.account_id=v.account_id AND e.version_id=v.version_id JOIN source_observations p ON p.observation_id=e.observation_id`); err != nil {
			return err
		}
	}
	for kind, field := range map[string]string{"messages": "timestamp_ms", "deltas": "uq_seq_id"} {
		if _, err = tx.Exec(`CREATE INDEX versions_` + field + ` ON source_versions(account_id,CAST(` + scalarSQL("residual", field) + ` AS INTEGER)) WHERE kind='` + kind + `'`); err != nil {
			return err
		}
	}
	_, err = tx.Exec(`
 CREATE VIEW message_threads AS
 SELECT v.account_id,v.entity_id AS message_entity_id,v.version_id AS message_version_id,i.thread_entity_id
 FROM messages_versions v JOIN source_relations r ON r.parent_id=v.version_id AND r.field='thread_fbid' JOIN source_ids i ON i.source_id=r.source_id
 UNION
 SELECT t.account_id,m.entity_id,m.version_id,t.entity_id FROM threads_versions t JOIN source_relations r ON r.parent_id=t.version_id AND r.field='slide_messages.edges.node' JOIN messages_versions m ON m.version_id=r.child_id
 WHERE NOT EXISTS(SELECT 1 FROM source_relations direct WHERE direct.parent_id=m.version_id AND direct.field='thread_fbid')
 UNION
 SELECT d.account_id,m.entity_id,m.version_id,i.thread_entity_id FROM deltas_versions d JOIN source_relations r ON r.parent_id=d.version_id AND r.field='message' JOIN messages_versions m ON m.version_id=r.child_id
 JOIN source_relations tr ON tr.parent_id=d.version_id AND tr.field='thread_fbid' JOIN source_ids i ON i.source_id=tr.source_id
 WHERE NOT EXISTS(SELECT 1 FROM source_relations direct WHERE direct.parent_id=m.version_id AND direct.field='thread_fbid');
 CREATE VIEW message_senders AS SELECT DISTINCT v.account_id,v.entity_id AS message_entity_id,v.version_id AS message_version_id,i.user_entity_id
 FROM messages_versions v JOIN source_relations r ON r.parent_id=v.version_id AND r.field IN ('sender_fbid','sender.id','sender.igid') JOIN source_ids i ON i.source_id=r.source_id;
 CREATE VIEW delta_messages AS SELECT d.account_id,d.version_id,i.message_entity_id FROM deltas_versions d JOIN source_relations r ON r.parent_id=d.version_id AND r.field='message_id' JOIN source_ids i ON i.source_id=r.source_id
 UNION SELECT d.account_id,d.version_id,m.entity_id FROM deltas_versions d JOIN source_relations r ON r.parent_id=d.version_id AND r.field='message' JOIN messages_versions m ON m.version_id=r.child_id;
 CREATE VIEW delta_threads AS SELECT d.account_id,d.version_id,i.thread_entity_id FROM deltas_versions d JOIN source_relations r ON r.parent_id=d.version_id AND r.field='thread_fbid' JOIN source_ids i ON i.source_id=r.source_id
 UNION SELECT d.account_id,d.version_id,t.thread_entity_id FROM deltas_versions d JOIN source_relations r ON r.parent_id=d.version_id AND r.field='message' JOIN messages_versions m ON m.version_id=r.child_id JOIN message_threads t ON t.account_id=m.account_id AND t.message_version_id=m.version_id;
 `)
	if err != nil {
		return err
	}
	_, err = tx.Exec(fmt.Sprintf("PRAGMA user_version=%d", schemaVersion))
	return err
}

var sourceFields = map[string][]string{
	"threads":  {"id", "thread_id", "thread_fbid", "thread_key", "thread_subtype", "system_folder", "last_activity_timestamp_ms", "thread_title"},
	"messages": {"id", "message_id", "thread_fbid", "sender_fbid", "timestamp_ms", "replied_to_message_id", "offline_threading_id", "text_body", "igd_snippet"},
	"users":    {"id", "pk", "fbid_v2", "interop_messaging_user_fbid", "username", "full_name"},
	"deltas":   {"uq_seq_id", "__typename", "thread_fbid", "message_id", "text_body"},
}

func scalarSQL(residual, field string) string {
	return `CASE WHEN json_type(` + residual + `,'$.` + field + `') IN ('integer','real') THEN ` + residual + ` -> '$.` + field + `' ELSE CAST(json_extract(` + residual + `,'$.` + field + `') AS TEXT) END`
}
func childResidual(parent, field string) string {
	return `(WITH RECURSIVE children(id,field) AS (SELECT child_id,field FROM source_relations WHERE parent_id=` + parent + ` AND child_id IS NOT NULL UNION ALL SELECT r.child_id,c.field||'.'||r.field FROM children c JOIN source_relations r ON r.parent_id=c.id WHERE r.child_id IS NOT NULL) SELECT v.residual FROM children c JOIN source_versions v ON v.version_id=c.id WHERE c.field='` + field + `' LIMIT 1)`
}
