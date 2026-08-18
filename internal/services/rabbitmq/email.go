package rabbitmq

import (
	"fmt"
	"log"
	"mime"
	"net/smtp"
	"os"
	"strings"
	"time"
)

// The mail this worker sends is already WRITTEN by the publisher: subject and
// body arrive rendered, and this is a dumb relay.
//
// Deliberate. The copy, the links and the FRONTEND_URL they are built from all
// live in user_service, and duplicating templates here would be two places to
// change every time the wording moves. What was actually costing anything was
// the SMTP round trip - a 30s-timeout connection held open inside a request,
// in one case inside an open database transaction - and that is what moves.
type SendEmailPayload struct {
	To      []string `json:"to"`
	From    string   `json:"from"`
	Subject string   `json:"subject"`
	Body    string   `json:"body"`
}

// SendEmail relays one already-rendered message.
//
// `From` also serves as the SMTP username: user_service sends as two identities
// (a verification address and a noreply address) that share one password, so the
// payload picks the identity and EMAIL_PASSWORD authenticates it.
func SendEmail(to []string, from, subject, body string) {
	host := os.Getenv("EMAIL_HOST")
	port := os.Getenv("EMAIL_PORT")
	password := os.Getenv("EMAIL_PASSWORD")

	if port == "" {
		port = "587"
	}

	if missing := requiredEmailEnv(host, password); missing != "" {
		log.Printf("send_email: not configured, dropping message to %d recipient(s): %s\n", len(to), missing)
		return
	}

	recipients := make([]string, 0, len(to))
	for _, address := range to {
		address = strings.TrimSpace(address)
		if address != "" {
			recipients = append(recipients, address)
		}
	}

	if len(recipients) == 0 || from == "" {
		log.Println("send_email: no recipients or no sender, nothing to do")
		return
	}

	// RFC 5322 wants CRLF, and the headers matter for deliverability: a message
	// with no Date or MIME-Version is far more likely to be filed as spam.
	// The subject is Q-encoded so a non-ASCII display name or emoji survives.
	var msg strings.Builder
	msg.WriteString("From: ");msg.WriteString(from);msg.WriteString("\r\n")
	msg.WriteString("To: ");msg.WriteString(strings.Join(recipients, ", "));msg.WriteString("\r\n")
	msg.WriteString("Subject: ");msg.WriteString(mime.QEncoding.Encode("utf-8", subject));msg.WriteString("\r\n")
	msg.WriteString("Date: ");msg.WriteString(time.Now().Format(time.RFC1123Z));msg.WriteString("\r\n")
	msg.WriteString("MIME-Version: 1.0\r\n")
	msg.WriteString("Content-Type: text/plain; charset=\"UTF-8\"\r\n")
	msg.WriteString("Content-Transfer-Encoding: 8bit\r\n")
	msg.WriteString("\r\n")
	msg.WriteString(strings.ReplaceAll(body, "\n", "\r\n"))

	auth := smtp.PlainAuth("", from, password, host)

	// SendMail upgrades to STARTTLS when the server advertises it, which is what
	// port 587 does - the same TLS the Django backend was negotiating.
	err := smtp.SendMail(host+":"+port, auth, from, recipients, []byte(msg.String()))
	if err != nil {
		// Logged, not retried: with auto-ack the message is already gone, and a
		// failed notification must not be able to wedge the queue.
		log.Printf("send_email: failed to send %q to %d recipient(s): %v\n",
			subject, len(recipients), err)
		return
	}

	log.Printf("send_email: sent %q to %d recipient(s) as %s\n", subject, len(recipients), from)
}

func requiredEmailEnv(host, password string) string {
	var missing []string
	if host == "" {
		missing = append(missing, "EMAIL_HOST")
	}
	if password == "" {
		missing = append(missing, "EMAIL_PASSWORD")
	}
	if len(missing) == 0 {
		return ""
	}
	return fmt.Sprintf("missing %s", strings.Join(missing, ", "))
}
