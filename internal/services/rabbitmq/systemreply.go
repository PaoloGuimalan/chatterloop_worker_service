package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"time"
	"worker_service/internal/connections"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Posting a built-in command's answer, as the System bot.
//
// # A REDUCED SEND PATH, DELIBERATELY
//
// Node's /sendMessage does a great deal after saving: un-archives the thread,
// updates the preview, fans out frames, sends pushes, bumps the chat score,
// resolves mentions, queues moderation. This does four of those, and the
// omissions are each a decision rather than an oversight:
//
//	MENTIONS ARE NOT RESOLVED. /members answers with a list of handles. Run
//	through mention resolution it would mention every one of them, and one
//	command would wake the entire room.
//
//	NO PUSH NOTIFICATION. Nobody should have their phone buzz because somebody
//	asked when the conversation was created.
//
//	NO CHAT SCORE. That measures engagement between people. A system reply is
//	not engagement, and counting it would let anyone inflate a score by typing
//	/help.
//
// What IS done: the message is stored so it is in history, the conversation
// preview is updated so the chat list is not stale, the thread is un-archived
// the way any other message un-archives it, and a frame goes to every
// participant so clients see the reply without refreshing.
//
// # THE SENDER IS THE SYSTEM BOT
//
// A fixed entity id, the same way the moderator has one, so every service
// agrees who "the platform" is without a lookup or a config value that drifts
// between environments. It is a real bot_bot row, so GetSenderDetails and
// HandlesFor resolve it to a name and an avatar - which is the whole reason it
// is a bot rather than a sentinel string.
const SystemBotEntityID = "00000000-0000-4000-8000-000000000002"

// messages_list is the event every client already listens to for a new
// message. A system reply is an ordinary message, so it needs no new event -
// and a new one would be a frame every existing client silently ignores.
const eventMessagesList = "messages_list"

// PostSystemReply stores one message as the System bot and announces it.
//
// Recipients and conversation type are read from the conversation rather than
// taken from the caller. The envelope carries neither, and it should not: it
// is handed to third-party webhooks, and a member list is not theirs to
// receive. Reading here also means the reply reaches whoever is in the
// conversation NOW rather than whoever was in it when the command was typed.
//
// Best effort after the insert: a command that answered correctly should not
// be retried because a frame failed to publish, and the message is in the
// store either way.
func PostSystemReply(ctx context.Context, conversationID, text string) error {
	if conversationID == "" || text == "" {
		return nil
	}

	messages := connections.Collection("messages")
	if messages == nil {
		return fmt.Errorf("mongo is not connected")
	}

	participants, conversationType := conversationFacts(ctx, conversationID)

	messageID := newMessageID()
	now := time.Now().UTC()

	document := bson.M{
		"messageID":      messageID,
		"conversationID": conversationID,
		// Clients key an optimistic bubble on pendingID and reconcile it when
		// the stored message arrives. Nothing sent this one optimistically, so
		// it gets a value that cannot collide with a client's.
		"pendingID": "system-" + messageID,
		"sender":    SystemBotEntityID,
		// Empty for the same reason Node leaves it empty elsewhere: readers
		// derive recipients from the conversation, and a stored list goes
		// stale the moment somebody joins.
		"receivers": bson.A{},
		"seeners":   bson.A{},
		"content":   text,
		// The schema marks it required, and a reader that groups by it would
		// silently drop a message missing it.
		"conversationType": conversationType,
		"messageDate":      now,
		"isReply":          false,
		"replyingTo":       "",
		"reactions":        bson.A{},
		"isDeleted":        false,
		// "text", not a new type. A client that met an unknown messageType
		// would render nothing at all, and every client would need changing
		// before a single command could answer.
		"messageType": "text",
	}

	if _, err := messages.InsertOne(ctx, document); err != nil {
		return fmt.Errorf("could not store the reply: %w", err)
	}

	updateConversationPreview(ctx, conversationID, messageID, text, now)
	unarchiveForEveryone(ctx, conversationID)
	announceSystemReply(ctx, conversationID, participants)
	return nil
}

// conversationFacts reads the participants and the type in one lookup.
//
// Both degrade rather than fail when the conversation document is missing: the
// message still belongs in history, and a reply nobody is pushed a frame for
// is better than no reply at all.
func conversationFacts(ctx context.Context, conversationID string) ([]string, string) {
	conversations := connections.Collection("conversations")
	if conversations == nil {
		return nil, "single"
	}

	var found struct {
		ParticipantIDs   []string `bson:"participant_ids"`
		ConversationType string   `bson:"conversationType"`
	}
	err := conversations.FindOne(ctx,
		bson.M{"conversationID": conversationID}).Decode(&found)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			slog.Warn("could not read the conversation",
				"conversation_id", conversationID, "error", err)
		}
		return nil, "single"
	}

	if found.ConversationType == "" {
		found.ConversationType = "single"
	}
	return found.ParticipantIDs, found.ConversationType
}

// updateConversationPreview keeps the chat list from showing the message
// before this one as the latest.
//
// The field is "text", not "content": the stored message and the preview
// disagree on the name, and writing the message's name here would leave every
// chat list showing an empty last line.
//
// Best effort: a stale preview is cosmetic, and failing the command over it
// would not be.
func updateConversationPreview(ctx context.Context, conversationID, messageID, text string, at time.Time) {
	conversations := connections.Collection("conversations")
	if conversations == nil {
		return
	}

	_, err := conversations.UpdateOne(ctx,
		bson.M{"conversationID": conversationID},
		bson.M{"$set": bson.M{
			"last_message": bson.M{
				"messageID":   messageID,
				"sender":      SystemBotEntityID,
				"text":        text,
				"messageDate": at,
				"messageType": "text",
				"isDeleted":   false,
			},
			// Mongoose stamps this on its own saves; the Go driver does not,
			// so it is set by hand to keep the document honest.
			"updatedAt": at,
		}},
	)
	if err != nil {
		slog.Warn("could not update the conversation preview",
			"conversation_id", conversationID, "error", err)
	}
}

// unarchiveForEveryone does what every other write path does: a new message
// pulls the thread back out of the archive. Without it, somebody who archived
// the conversation would have the reply to their own command filed away.
func unarchiveForEveryone(ctx context.Context, conversationID string) {
	history := connections.Collection("chat_history")
	if history == nil {
		return
	}

	_, err := history.UpdateMany(ctx,
		bson.M{"conversationID": conversationID},
		bson.M{"$set": bson.M{"isArchived": false}},
	)
	if err != nil {
		slog.Warn("could not un-archive the conversation",
			"conversation_id", conversationID, "error", err)
	}
}

// announceSystemReply publishes the frame each participant's client is already
// listening for.
//
// The mentioner and command fields are both null: a system reply is neither. A
// bot reading this frame therefore sees an ordinary message from an entity
// that is not addressing it, which is exactly right - it must not answer the
// output of a command.
func announceSystemReply(ctx context.Context, conversationID string, participants []string) {
	if len(participants) == 0 {
		return
	}

	client := connections.RedisClient()
	if client == nil {
		slog.Warn("no redis: the reply is stored but not announced",
			"conversation_id", conversationID)
		return
	}

	body, err := json.Marshal(map[string]any{
		"logType": nil,
		"pod":     os.Getenv("POD_NAME"),
		"event":   eventMessagesList,
		"message": map[string]any{
			"status": true, "auth": true, "onseen": false,
			"result": "",
			"message": map[string]any{
				"conversationID": conversationID,
				"entityID":       SystemBotEntityID,
				"mentioner":      nil,
				"command":        nil,
			},
		},
		// The exact envelope Node and developer_service both publish. A frame
		// in a different shape is one every existing client fails to unwrap.
		"dateTime": time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		slog.Error("could not encode the frame", "error", err)
		return
	}

	for _, entityID := range participants {
		if entityID == "" {
			continue
		}
		if err := client.Publish(ctx, "events_"+entityID, body).Err(); err != nil {
			slog.Warn("could not announce to a participant",
				"entity_id", entityID, "error", err)
		}
	}
}

// newMessageID matches the 30-digit id Node mints with makeID(30) - the two
// write to the same collection, so they have to agree on the shape.
func newMessageID() string {
	const digits = "0123456789"
	out := make([]byte, 30)
	for i := range out {
		out[i] = digits[rand.Intn(len(digits))]
	}
	return string(out)
}
