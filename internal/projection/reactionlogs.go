package projection

import (
	"context"
	"encoding/json"
	"github.com/tidwall/gjson"

	"go.mau.fi/mautrix-meta/internal/store"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
)

func isReactionLog(raw json.RawMessage) bool {
	return gjson.GetBytes(raw, "content.is_reaction_action_log").Type == gjson.True
}
func addAlias(index map[string]map[string]bool, id, identity string) {
	if id == "" {
		return
	}
	if index[id] == nil {
		index[id] = map[string]bool{}
	}
	index[id][identity] = true
}
func uniqueAlias(index map[string]map[string]bool, id string) string {
	if len(index[id]) != 1 {
		return ""
	}
	for identity := range index[id] {
		return identity
	}
	return ""
}
func reactionLogs(ctx context.Context, db *store.Store, chat store.Record) (map[string][]store.Record, []store.Record, error) {
	parents := map[string]map[string]bool{}
	logs := map[string]map[string]bool{}
	// Each log may have evidence from a retained message reaction or a delta.
	targets := map[string]map[string]bool{}
	records := []store.Record{}
	link := func(log, parent string) {
		if log == "" || parent == "" {
			return
		}
		if targets[log] == nil {
			targets[log] = map[string]bool{}
		}
		targets[log][parent] = true
	}
	err := db.TranscriptMessages(ctx, chat.Account, chat.Identity, func(record store.Record) error {
		var message slidetypes.Message
		_ = json.Unmarshal(record.Raw, &message)
		if isReactionLog(record.Raw) {
			addAlias(logs, store.String(record.Raw, "id"), record.Identity)
			addAlias(logs, store.String(record.Raw, "message_id"), record.Identity)
			records = append(records, record)
		} else {
			addAlias(parents, message.ID, record.Identity)
			addAlias(parents, message.MessageID, record.Identity)
			for _, reaction := range message.Reactions {
				if reaction != nil {
					link(reaction.LogMessageID, record.Identity)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	deltaLinks, err := db.ReactionLogLinks(ctx, chat.Account, chat.Identity)
	if err != nil {
		return nil, nil, err
	}
	for log, ids := range deltaLinks {
		for _, id := range ids {
			link(log, uniqueAlias(parents, id))
		}
	}
	resolved := map[string]map[string]bool{}
	for log, ids := range targets {
		identity := uniqueAlias(logs, log)
		if identity == "" {
			continue
		}
		if resolved[identity] == nil {
			resolved[identity] = map[string]bool{}
		}
		for id := range ids {
			resolved[identity][id] = true
		}
	}
	attached := map[string][]store.Record{}
	unresolved := []store.Record{}
	for _, record := range records {
		ids := resolved[record.Identity]
		if len(ids) != 1 {
			unresolved = append(unresolved, record)
			continue
		}
		for id := range ids {
			attached[id] = append(attached[id], record)
		}
	}
	return attached, unresolved, nil
}
