package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"worker_service/internal/connections"

	"github.com/jackc/pgx/v5"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// A NOTICE line in a conversation - "maya joined", "leo added sam" - posted by
// a service that is not the Node server.
//
// The centred grey line both clients already draw for messageType "notif",
// which is how Node writes its own "X added Y" (NotificationMessageForConversations
// in server/reusables/models/messages.js). This is that, for everyone else:
// user_service publishes one job when an accepted invite puts somebody in a
// group or a server's channels, because Django and Node never call each other.
// Anything later that needs to say something in a thread - a role change, a
// rename - publishes the same job with its own sentence.
//
// # WHAT IT DOES, AND WHY EACH
//
//	THE MESSAGE, attributed to the actor, so the line sits in history.
//
//	THE CONVERSATION'S participant_ids, from the realm's members as they are
//	NOW. The chat list finds a conversation by participant_ids, and pushes for
//	later messages go to them: a member who joined without a message being
//	written would see no group in their chats and get no pushes for it until
//	somebody spoke. Node's SaveConversation sets them on the same write.
//
//	THE PREVIEW (last_message) and the un-archive, the way every message does.
//
//	A messages_list frame AND a contactslist frame to every member - the two
//	events Node sends for its own notices, so an open chat list picks up the
//	new group and an open thread shows the line.
//
//	A PUSH to members without a live connection, when the job asks for one -
//	Node pushes its "X added Y" the same way. Never to the actor.
//
// A realm with no chat - a voice room, a page - is skipped, as Node skips it.
type ConversationNoticePayload struct {
	ConversationID string `json:"conversation_id"`
	// Who the line is from: the person who joined, the one who did the adding.
	ActorEntityID string `json:"actor_entity_id"`
	// The line, already written by the publisher - the same arrangement as
	// send_email, so its wording lives in one place.
	Text string `json:"text"`
	Push bool   `json:"push"`
}

const eventContactsList = "contactslist"

// noticeConversationType is the conversationType a realm's notices are stored
// under, and false for a realm whose conversation has no chat to write in.
// Node's normalizeConversationType, plus its voice/page refusal.
func noticeConversationType(realmType string) (string, bool) {
	switch strings.ToLower(realmType) {
	case "voice", "page":
		return "", false
	case "server":
		return "channel", true
	case "":
		return "single", true
	default:
		return strings.ToLower(realmType), true
	}
}

// PostConversationNotice stores one notice line and announces it. See the top
// of this file for each step. Only the insert can fail the job; everything
// after it is best effort, because the line is in history either way and a
// retry would post it twice.
func PostConversationNotice(ctx context.Context, p ConversationNoticePayload) error {
	p.Text = strings.TrimSpace(p.Text)
	if p.ConversationID == "" || p.ActorEntityID == "" || p.Text == "" {
		return fmt.Errorf("%w: conversation_id, actor_entity_id and text are required", ErrDrop)
	}

	messages := connections.Collection("messages")
	if messages == nil {
		return fmt.Errorf("mongo is not connected")
	}

	realm, isRealm := realmOf(ctx, p.ConversationID)
	var members []string
	conversationType := "single"
	conversationName := ""
	if isRealm {
		var hasChat bool
		conversationType, hasChat = noticeConversationType(realm.Type)
		if !hasChat {
			return nil
		}
		conversationName = realm.Name
		members = realmMembers(ctx, p.ConversationID)
	} else {
		// Not a realm - a direct conversation. Its participants are whoever
		// the conversation already lists.
		members, conversationType = conversationFacts(ctx, p.ConversationID)
	}

	messageID := newMessageID()
	now := time.Now().UTC()

	document := bson.M{
		"messageID":      messageID,
		"conversationID": p.ConversationID,
		// Nothing sent this optimistically - a value no client's can match.
		"pendingID":        "notice-" + messageID,
		"sender":           p.ActorEntityID,
		"receivers":        stringsToBSON(members),
		"seeners":          bson.A{},
		"content":          p.Text,
		"conversationType": conversationType,
		"messageDate":      now,
		"isReply":          false,
		"replyingTo":       "",
		"reactions":        bson.A{},
		"isDeleted":        false,
		"messageType":      "notif",
	}
	if _, err := messages.InsertOne(ctx, document); err != nil {
		return fmt.Errorf("could not store the notice: %w", err)
	}

	saveNoticeConversation(ctx, p, messageID, conversationType, members, now)
	unarchiveForEveryone(ctx, p.ConversationID)

	publishToEntities(ctx, members, eventMessagesList, map[string]any{
		"status": true, "auth": true, "onseen": false, "result": "",
		"message": map[string]any{
			"conversationID": p.ConversationID,
			"entityID":       p.ActorEntityID,
		},
	})
	publishToEntities(ctx, members, eventContactsList, map[string]any{
		"status": true, "auth": true, "result": "",
		"message": p.Text,
	})

	if p.Push {
		name, avatar := senderOf(ctx, p.ActorEntityID)
		push := noticePush(p, messageID, conversationName, isRealm, name, avatar, members, now)
		if len(push.EntityIDs) > 0 {
			SendPush(ctx, push)
		}
	}
	return nil
}

type noticeRealm struct {
	Type string
	Name string
}

// realmOf reads the realm a conversation belongs to - a group's or a
// channel's conversation id IS its realm_id. False for a direct conversation.
func realmOf(ctx context.Context, conversationID string) (noticeRealm, bool) {
	pool := connections.Pool()
	if pool == nil {
		return noticeRealm{}, false
	}
	var realm noticeRealm
	err := pool.QueryRow(ctx,
		`SELECT coalesce(type, ''), coalesce(name, '')
		   FROM community_realm WHERE realm_id = $1`,
		conversationID,
	).Scan(&realm.Type, &realm.Name)
	if err != nil {
		if err != pgx.ErrNoRows {
			slog.Warn("could not read the realm", "conversation_id", conversationID, "error", err)
		}
		return noticeRealm{}, false
	}
	return realm, true
}

// realmMembers is everyone in the realm now - users, pages and bots alike, as
// Node's GetAllReceivers reads them.
func realmMembers(ctx context.Context, realmID string) []string {
	rows, err := connections.Pool().Query(ctx,
		`SELECT entity_id::text FROM community_member WHERE realm_id = $1`, realmID)
	if err != nil {
		slog.Warn("could not read the members", "realm_id", realmID, "error", err)
		return nil
	}
	defer rows.Close()

	var members []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil && id != "" {
			members = append(members, id)
		}
	}
	return members
}

// saveNoticeConversation is Node's SaveConversation for this message: the
// preview, the participants, and the conversation itself if this is the first
// thing ever written in it.
func saveNoticeConversation(ctx context.Context, p ConversationNoticePayload, messageID, conversationType string, members []string, at time.Time) {
	conversations := connections.Collection("conversations")
	if conversations == nil {
		return
	}

	set := bson.M{
		"conversationType": conversationType,
		"senderType":       "user",
		"authorRealm":      nil,
		"last_message": bson.M{
			"messageID": messageID,
			"sender":    p.ActorEntityID,
			// "text" here, "content" on the message - see updateConversationPreview.
			"text":        p.Text,
			"messageDate": at,
			"messageType": "notif",
			"isDeleted":   false,
			// A new last message, seen by nobody yet.
			"seeners": bson.A{},
		},
		"updatedAt": at,
	}
	// Never emptied: a members read that failed must not take everybody out of
	// the conversation.
	if len(members) > 0 {
		set["participant_ids"] = stringsToBSON(members)
	}

	_, err := conversations.UpdateOne(ctx,
		bson.M{"conversationID": p.ConversationID},
		bson.M{"$set": set, "$setOnInsert": bson.M{"createdAt": at}},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		slog.Warn("could not update the conversation",
			"conversation_id", p.ConversationID, "error", err)
	}
}

// senderOf is the actor's display name and picture, across all three kinds of
// entity, for the push. Empty strings when it cannot be read - the push still
// goes, the app falls back to the conversation's name.
func senderOf(ctx context.Context, entityID string) (string, string) {
	pool := connections.Pool()
	if pool == nil {
		return "", ""
	}
	var name, profile string
	err := pool.QueryRow(ctx, `
		SELECT coalesce(nullif(btrim(coalesce(u.first_name,'') || ' ' || coalesce(u.last_name,'')), ''),
		                r.name, b.name, ''),
		       coalesce(u.profile, r.profile, b.profile, '')
		  FROM entity_entity e
		  LEFT JOIN user_account u ON u.entity_id = e.id AND e.type = 'user'
		  LEFT JOIN community_realm r ON r.entity_id = e.id AND e.type = 'realm'
		  LEFT JOIN bot_bot b ON b.entity_id = e.id AND e.type = 'bot'
		 WHERE e.id::text = $1`, entityID).Scan(&name, &profile)
	if err != nil {
		return "", ""
	}
	if profile == "none" || profile == "N/A" {
		profile = ""
	}
	return name, profile
}

// noticePush is the push Node's push.sendMessage builds for its notices - the
// message channel, keyed to the conversation so the app threads it there -
// addressed to every member but the actor.
func noticePush(p ConversationNoticePayload, messageID, conversationName string, isGroup bool, senderName, senderAvatar string, members []string, at time.Time) SendPushPayload {
	receivers := make([]string, 0, len(members))
	for _, id := range members {
		if id != "" && id != p.ActorEntityID {
			receivers = append(receivers, id)
		}
	}

	title := senderName
	body := p.Text
	if isGroup {
		title = conversationName
		if senderName != "" {
			body = senderName + ": " + p.Text
		}
	}

	data := map[string]string{
		"type":           "message",
		"conversationId": p.ConversationID,
		"isGroup":        strconv.FormatBool(isGroup),
		"senderId":       p.ActorEntityID,
		"body":           p.Text,
		"sentAt":         strconv.FormatInt(at.UnixMilli(), 10),
		"messageId":      messageID,
	}
	// FCM refuses the whole message over one empty value, so the optional
	// ones are left out rather than sent blank - as Node's send() does.
	for key, value := range map[string]string{
		"conversationName": conversationName,
		"senderName":       senderName,
		"senderAvatarUrl":  senderAvatar,
	} {
		if value != "" {
			data[key] = value
		}
	}

	return SendPushPayload{
		EntityIDs: receivers,
		Channel:   ChannelMessages,
		Title:     title,
		Body:      body,
		Tag:       p.ConversationID,
		Data:      data,
	}
}

func stringsToBSON(values []string) bson.A {
	out := bson.A{}
	for _, value := range values {
		out = append(out, value)
	}
	return out
}
