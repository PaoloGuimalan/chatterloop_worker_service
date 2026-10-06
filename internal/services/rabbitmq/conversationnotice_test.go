package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A voice room and a page have no chat to write in - Node refuses them too.
func TestNoticeSkipsRealmsWithoutAChat(t *testing.T) {
	for _, realmType := range []string{"voice", "page", "VOICE"} {
		if _, ok := noticeConversationType(realmType); ok {
			t.Fatalf("%q must have no notices", realmType)
		}
	}
	for realmType, want := range map[string]string{
		"group":   "group",
		"channel": "channel",
		// Node's normalizeConversationType.
		"server":     "channel",
		"conference": "conference",
	} {
		got, ok := noticeConversationType(realmType)
		if !ok || got != want {
			t.Fatalf("%q: got %q, %v - want %q", realmType, got, ok, want)
		}
	}
}

// Nothing to say, or nobody to say it as: dropped, not retried - a retry
// could never succeed.
func TestNoticeWithoutTheEssentialsIsDropped(t *testing.T) {
	for _, p := range []ConversationNoticePayload{
		{ActorEntityID: "a", Text: "maya joined"},
		{ConversationID: "c", Text: "maya joined"},
		{ConversationID: "c", ActorEntityID: "a", Text: "   "},
	} {
		if err := PostConversationNotice(context.Background(), p); !errors.Is(err, ErrDrop) {
			t.Fatalf("%+v: want ErrDrop, got %v", p, err)
		}
	}
}

func TestNoticePushGoesToEveryoneButTheActor(t *testing.T) {
	p := ConversationNoticePayload{
		ConversationID: "R100",
		ActorEntityID:  "maya",
		Text:           "maya joined",
	}
	at := time.UnixMilli(1_790_000_000_000)
	push := noticePush(p, "M1", "Weekend Hikers", true, "Maya Reyes", "", []string{"maya", "leo", "", "ana"}, at)

	if len(push.EntityIDs) != 2 || push.EntityIDs[0] != "leo" || push.EntityIDs[1] != "ana" {
		t.Fatalf("receivers: %v", push.EntityIDs)
	}
	// The message channel, threaded on the conversation - as Node's
	// push.sendMessage.
	if push.Channel != ChannelMessages || push.Tag != "R100" {
		t.Fatalf("channel %q tag %q", push.Channel, push.Tag)
	}
	if push.Title != "Weekend Hikers" || push.Body != "Maya Reyes: maya joined" {
		t.Fatalf("title %q body %q", push.Title, push.Body)
	}
	want := map[string]string{
		"type":             "message",
		"conversationId":   "R100",
		"conversationName": "Weekend Hikers",
		"isGroup":          "true",
		"senderId":         "maya",
		"senderName":       "Maya Reyes",
		"body":             "maya joined",
		"sentAt":           "1790000000000",
		"messageId":        "M1",
	}
	for key, value := range want {
		if push.Data[key] != value {
			t.Fatalf("data[%s] = %q, want %q", key, push.Data[key], value)
		}
	}
	// FCM refuses a message over one empty value.
	if _, present := push.Data["senderAvatarUrl"]; present {
		t.Fatal("an empty avatar must be left out, not sent blank")
	}
}
