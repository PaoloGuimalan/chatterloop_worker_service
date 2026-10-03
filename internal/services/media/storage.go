// Package media deletes stored files once nothing uses them: the
// media_release consumer. Its jobs come from the Node server and user_service
// (content deleted) and from cron_service's media_cleanup sweep (uploads left
// unfinished or unused) - the rules for what may go live only here.
//
// The upload side (presigned links, confirming uploads) lives in the Node
// server's reusables/media; this package only ever removes. Both read the same
// storage settings from the environment.
package media

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// Storage is the one bucket Chatterloop writes to, through any S3-compatible
// provider. Moving providers is a change of environment variables:
//
//	STORAGE_ENDPOINT / SPACES_ENDPOINT          origin API endpoint
//	STORAGE_REGION / SPACES_REGION              signing region
//	STORAGE_BUCKET / SPACES_BUCKET
//	STORAGE_KEY / SPACES_KEY, STORAGE_SECRET / SPACES_SECRET
//	STORAGE_CDN_ENDPOINT / SPACES_CDN_ENDPOINT  the provider's CDN host
//	STORAGE_PUBLIC_BASE_URL                     our media domain
type Storage struct {
	client     *s3.Client
	Bucket     string
	hosts      map[string]bool // hosts that address the bucket directly
	originHost string          // path-style: https://<origin>/<bucket>/<key>
}

func env(name string) string {
	if v := os.Getenv("STORAGE_" + name); v != "" {
		return v
	}
	return os.Getenv("SPACES_" + name)
}

func hostOf(raw string) string {
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Host)
}

// NewStorage builds a Storage; the client is nil-safe to leave out in tests.
func NewStorage(bucket, endpoint, cdnEndpoint, publicBaseURL string, client *s3.Client) *Storage {
	s := &Storage{
		client:     client,
		Bucket:     bucket,
		hosts:      map[string]bool{},
		originHost: hostOf(endpoint),
	}
	for _, h := range []string{
		hostOf(publicBaseURL),
		strings.ToLower(bucket + "." + cdnEndpoint),
		strings.ToLower(bucket + "." + s.originHost),
	} {
		if h != "" && !strings.HasPrefix(h, ".") && !strings.HasSuffix(h, ".") {
			s.hosts[h] = true
		}
	}
	return s
}

var (
	defaultStorage     *Storage
	defaultStorageOnce sync.Once
)

// DefaultStorage is the bucket from the environment, or nil when it is not
// configured (deletion is then skipped, never guessed).
func DefaultStorage() *Storage {
	defaultStorageOnce.Do(func() {
		bucket, endpoint := env("BUCKET"), env("ENDPOINT")
		if bucket == "" || endpoint == "" {
			return
		}
		region := env("REGION")
		if region == "" {
			region = "us-east-1"
		}
		client := s3.NewFromConfig(aws.Config{
			Region:      region,
			Credentials: credentials.NewStaticCredentialsProvider(env("KEY"), env("SECRET"), ""),
		}, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			// Not every S3-compatible store accepts the SDK's default request
			// checksums; only send them where the API requires one.
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		})
		defaultStorage = NewStorage(bucket, endpoint, env("CDN_ENDPOINT"), os.Getenv("STORAGE_PUBLIC_BASE_URL"), client)
	})
	return defaultStorage
}

// KeyFromURL is the key a link of ours points at - through the media domain,
// the CDN host or the origin - or "" for anything else: other buckets,
// Firebase, pasted links.
func (s *Storage) KeyFromURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return ""
	}
	host := strings.ToLower(u.Host)
	path := u.EscapedPath()
	if !s.hosts[host] {
		prefix := "/" + s.Bucket + "/"
		if s.originHost == "" || host != s.originHost || !strings.HasPrefix(path, prefix) {
			return ""
		}
		path = path[len(prefix)-1:]
	}
	segments := strings.Split(strings.TrimLeft(path, "/"), "/")
	for i, seg := range segments {
		if decoded, err := url.PathUnescape(seg); err == nil {
			segments[i] = decoded
		}
	}
	return strings.Join(segments, "/")
}

// Remove deletes a key. Deleting a missing key succeeds, as S3 treats it.
func (s *Storage) Remove(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})
	return err
}

// AbortMultipart drops an unfinished multipart upload's parts.
func (s *Storage) AbortMultipart(ctx context.Context, key, uploadID string) error {
	_, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchUpload" {
		return nil // already completed or aborted
	}
	return err
}
