package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"go.mongodb.org/mongo-driver/bson"
	"google.golang.org/api/option"

	"worker_service/internal/connections"
)

type SendPushPayload struct {
	EntityIDs []string          `json:"entity_ids"`
	Tokens    []string          `json:"tokens"`
	Channel   string            `json:"channel"`
	Title     string            `json:"title"`
	Body      string            `json:"body"`
	Tag       string            `json:"tag"`
	ImageURL  string            `json:"image_url"`
	OSRendered bool             `json:"os_rendered"`
	Data      map[string]string `json:"data"`
}

const (
	ChannelMessages = "chatterloop_messages_v2"
	ChannelActivity = "chatterloop_activity_v2"
	SoundMessages   = "message_alert"
	SoundActivity   = "notification_alert"
)

const maxTokensPerSend = 500

var (
	fcmOnce   sync.Once
	fcmClient *messaging.Client
	fcmErr    error
)

func messagingClient(ctx context.Context) (*messaging.Client, error) {
	fcmOnce.Do(func() {
		required := map[string]string{
			"FIREBASE_PROJECT_ID":   os.Getenv("FIREBASE_PROJECT_ID"),
			"FIREBASE_CLIENT_EMAIL": os.Getenv("FIREBASE_CLIENT_EMAIL"),
			"FIREBASE_PRIVATE_KEY":  os.Getenv("FIREBASE_PRIVATE_KEY"),
		}
		var missing []string
		for name, value := range required {
			if value == "" {
				missing = append(missing, name)
			}
		}
		if len(missing) > 0 {
			fcmErr = fmt.Errorf("missing %s", strings.Join(missing, ", "))
			return
		}

		privateKey := required["FIREBASE_PRIVATE_KEY"]
		var wrapper struct {
			PrivateKey string `json:"privateKey"`
		}
		if err := json.Unmarshal([]byte(privateKey), &wrapper); err == nil && wrapper.PrivateKey != "" {
			privateKey = wrapper.PrivateKey
		}
		privateKey = strings.ReplaceAll(privateKey, "\\n", "\n")

		credentials, err := json.Marshal(map[string]string{
			"type":         "service_account",
			"project_id":   required["FIREBASE_PROJECT_ID"],
			"client_email": required["FIREBASE_CLIENT_EMAIL"],
			"private_key":  privateKey,
			"token_uri":    "https://oauth2.googleapis.com/token",
		})
		if err != nil {
			fcmErr = err
			return
		}

		app, err := firebase.NewApp(ctx, nil, option.WithCredentialsJSON(credentials))
		if err != nil {
			fcmErr = err
			return
		}
		fcmClient, fcmErr = app.Messaging(ctx)
	})

	return fcmClient, fcmErr
}

func offlineTokensFor(ctx context.Context, entityIDs []string) ([]string, error) {
	sessions := connections.Sessions()
	if sessions == nil {
		return nil, fmt.Errorf("mongo is not connected")
	}

	ids := make([]string, 0, len(entityIDs))
	for _, id := range entityIDs {
		if id != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}

	filter := bson.M{
		"entityID": bson.M{"$in": ids},
		"status":   false,
		"fcmToken": bson.M{"$nin": bson.A{nil, ""}},
	}

	cursor, err := sessions.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	seen := make(map[string]struct{})
	var tokens []string
	for cursor.Next(ctx) {
		var row struct {
			FCMToken string `bson:"fcmToken"`
		}
		if err := cursor.Decode(&row); err != nil || row.FCMToken == "" {
			continue
		}
		if _, dup := seen[row.FCMToken]; dup {
			continue
		}
		seen[row.FCMToken] = struct{}{}
		tokens = append(tokens, row.FCMToken)
	}

	return tokens, cursor.Err()
}

func pruneTokens(ctx context.Context, tokens []string) {
	if len(tokens) == 0 {
		return
	}

	sessions := connections.Sessions()
	if sessions == nil {
		return
	}

	_, err := sessions.UpdateMany(
		ctx,
		bson.M{"fcmToken": bson.M{"$in": tokens}},
		bson.M{"$set": bson.M{"fcmToken": nil}},
	)
	if err != nil {
		log.Printf("send_push: failed to prune %d dead tokens: %v\n", len(tokens), err)
	}
}

func SendPush(ctx context.Context, payload SendPushPayload) {
	client, err := messagingClient(ctx)
	if err != nil {
		log.Printf("send_push: FCM not configured, dropping notification: %v\n", err)
		return
	}

	tokens := payload.Tokens
	if len(tokens) == 0 {
		tokens, err = offlineTokensFor(ctx, payload.EntityIDs)
		if err != nil {
			log.Printf("send_push: failed to resolve tokens: %v\n", err)
			return
		}
	}

	if len(tokens) == 0 {
		return
	}

	channel, sound := ChannelActivity, SoundActivity
	if payload.Channel == ChannelMessages || payload.Channel == "messages" {
		channel, sound = ChannelMessages, SoundMessages
	}

	data := make(map[string]string, len(payload.Data))
	for key, value := range payload.Data {
		if value != "" {
			data[key] = value
		}
	}

	message := &messaging.MulticastMessage{
		Data: data,
		Android: &messaging.AndroidConfig{
			Priority: "high",
		},
	}

	if payload.OSRendered {
		message.Notification = &messaging.Notification{
			Title: payload.Title,
			Body:  payload.Body,
		}
		android := &messaging.AndroidNotification{
			ChannelID: channel,
			Sound:     sound,
		}
		if payload.Tag != "" {
			android.Tag = payload.Tag
		}
		if payload.ImageURL != "" {
			android.ImageURL = payload.ImageURL
		}
		message.Android.Notification = android
	}

	var success, failure int
	var dead []string

	for start := 0; start < len(tokens); start += maxTokensPerSend {
		end := start + maxTokensPerSend
		if end > len(tokens) {
			end = len(tokens)
		}

		chunk := tokens[start:end]
		message.Tokens = chunk

		response, err := client.SendEachForMulticast(ctx, message)
		if err != nil {
			log.Printf("send_push: multicast failed for %d tokens: %v\n", len(chunk), err)
			continue
		}

		success += response.SuccessCount
		failure += response.FailureCount

		for i, result := range response.Responses {
			if result.Success {
				continue
			}
			if messaging.IsUnregistered(result.Error) ||
				messaging.IsInvalidArgument(result.Error) {
				dead = append(dead, chunk[i])
			}
		}
	}

	pruneTokens(ctx, dead)

	log.Printf("send_push: %d delivered, %d failed, %d dead tokens pruned (channel=%s)\n",
		success, failure, len(dead), channel)
}
