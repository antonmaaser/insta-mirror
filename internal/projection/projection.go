package projection

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"

	"go.mau.fi/mautrix-meta/internal/store"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
)

type Result struct {
	Media         []store.Media  `json:"media,omitempty"`
	Source        store.Record   `json:"source"`
	Reference     string         `json:"source_reference"`
	Resource      string         `json:"resource_uri,omitempty"`
	Snapshot      any            `json:"snapshot"`
	Reconstructed any            `json:"reconstructed,omitempty"`
	Updates       []store.Record `json:"updates,omitempty"`
	Completeness  string         `json:"completeness"`
	Uncertainty   []string       `json:"uncertainty,omitempty"`
	Deleted       bool           `json:"deleted,omitempty"`
}

func Build(ctx context.Context, db *store.Store, r store.Record, includeRaw bool) (Result, error) {
	out := Result{Source: r, Reference: store.SourceRef(r), Completeness: "partial"}
	var snapshot map[string]any
	if err := decodeObject(r.Raw, &snapshot); err != nil {
		return out, err
	}
	out.Snapshot = snapshot
	threadRefs := []string{}
	if r.Table == "messages" {
		media, err := db.Media(ctx, r.Account, r.RowID)
		if err != nil {
			return out, err
		}
		out.Media = media
		var message slidetypes.Message
		if err := json.Unmarshal(r.Raw, &message); err != nil {
			out.Uncertainty = append(out.Uncertainty, "upstream message decoding failed; source fields retained")
		}
		if message.ThreadFBID != "" {
			threadRefs = append(threadRefs, message.ThreadFBID)
		}
		if r.Thread != "" {
			threadRefs = append(threadRefs, r.Thread)
		}
		out.Resource = "igdm://accounts/" + r.Account + "/threads/" + url.PathEscape("thread_fbid:"+message.ThreadFBID) + "/messages/" + url.PathEscape(r.Identity)
		ids := []string{}
		if message.ID != "" {
			ids = append(ids, message.ID)
		}
		if message.MessageID != "" {
			ids = append(ids, message.MessageID)
		}
		deltas, truncated, err := db.Deltas(ctx, r.Account, threadRefs, ids)
		if err != nil {
			return out, err
		}
		out.Updates = deltas
		if truncated {
			out.Uncertainty = append(out.Uncertainty, "delta window exceeds 200 events")
		}
		seq := int64(0)
		if strings.HasPrefix(r.Origin, "dgw:") {
			seq, _ = strconv.ParseInt(strings.TrimPrefix(r.Origin, "dgw:"), 10, 64)
		}
		// Realtime new-message snapshots carry their source sequence. GraphQL
		// snapshots have no comparable sequence; expose updates without guessing.
		if len(deltas) > 0 && seq == 0 {
			out.Uncertainty = append(out.Uncertainty, "snapshot and realtime sequence are not directly comparable; candidate updates were not applied")
		}
		var current map[string]any
		_ = decodeObject(r.Raw, &current)
		for _, record := range deltas {
			var d slidetypes.Delta
			if err = json.Unmarshal(record.Raw, &d); err != nil {
				out.Uncertainty = append(out.Uncertainty, "unparseable delta retained")
				continue
			}
			if seq == 0 || d.UQSeqID <= seq {
				continue
			}
			switch event := d.Data.(type) {
			case *slidetypes.EditMessageEvent:
				current["text_body"] = event.TextBody
				current["igd_snippet"] = event.IGDSnippet
				if content, ok := current["content"].(map[string]any); ok {
					if _, ok = content["text_body"]; ok {
						content["text_body"] = event.TextBody
					}
				}
			case *slidetypes.DeleteMessageEvent, *slidetypes.DeleteThreadEvent:
				out.Deleted = true
			case *slidetypes.CreateReactionEvent:
				applyReaction(current, event.Reaction, false)
			case *slidetypes.DeleteReactionEvent:
				applyReaction(current, event.Reaction, true)
			case *slidetypes.NewMessageEvent, *slidetypes.AdminMessageEvent:
			default:
				out.Uncertainty = append(out.Uncertainty, "unsupported update remains in original source")
			}
		}
		if seq > 0 {
			out.Reconstructed = current
		}
	} else if r.Table == "threads" {
		var thread slidetypes.ThreadInfo
		if err := json.Unmarshal(r.Raw, &thread); err != nil {
			out.Uncertainty = append(out.Uncertainty, "upstream thread decoding failed")
		}
		out.Resource = "igdm://accounts/" + r.Account + "/threads/" + url.PathEscape(r.Identity)
		for _, v := range []string{thread.ID, thread.ThreadFBID} {
			if v != "" {
				threadRefs = append(threadRefs, v)
			}
		}
		updates, truncated, err := db.Deltas(ctx, r.Account, threadRefs, nil)
		if err != nil {
			return out, err
		}
		out.Updates = updates
		if len(updates) > 0 {
			out.Uncertainty = append(out.Uncertainty, "thread snapshot may already incorporate realtime changes; updates remain separate")
		}
		if truncated {
			out.Uncertainty = append(out.Uncertainty, "delta window exceeds 200 events")
		}
	} else {
		var user slidetypes.User
		if err := json.Unmarshal(r.Raw, &user); err != nil {
			out.Uncertainty = append(out.Uncertainty, "upstream user decoding failed")
		}
	}
	if !includeRaw {
		out.Source.Raw = nil
		for i := range out.Updates {
			out.Updates[i].Raw = nil
		}
	}
	return out, nil
}
func applyReaction(message map[string]any, reaction slidetypes.Reaction, remove bool) {
	var out []any
	if existing, ok := message["reactions"].([]any); ok {
		for _, item := range existing {
			obj, ok := item.(map[string]any)
			if !ok {
				out = append(out, item)
				continue
			}
			sender, _ := obj["sender_fbid"].(string)
			if sender != strconv.FormatInt(reaction.SenderFBID, 10) || remove && reaction.Reaction != "" && obj["reaction"] != reaction.Reaction {
				out = append(out, item)
			}
		}
	}
	if !remove {
		b, _ := json.Marshal(reaction)
		var item any
		_ = json.Unmarshal(b, &item)
		out = append(out, item)
	}
	if out == nil {
		out = []any{}
	}
	message["reactions"] = out
}

func decodeObject(b []byte, out *map[string]any) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	return decoder.Decode(out)
}
