package analytics

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain" 
	"github.com/riferrei/srclient"
)

const (
	defaultBroker            = "kafka-1st-1:1092"
	defaultSchemaRegistryURL = "http://schema-registry-1st:8081"
	defaultUsername          = "admin"
	defaultPassword          = "admin-secret"
	defaultCaPath            = "/usr/local/bin/ca.crt"
	defaultAnalyticsTopic    = "event"
)

type Service struct {
	writer *kafka.Writer
	schema *srclient.Schema
}

type Event struct {
	EventType   string   `avro:"event_type"`
	User        string   `avro:"user"`
	Query       string   `avro:"query"`
	Found       bool     `avro:"found"`
	ResultCount int      `avro:"result_count"`
	ProductIDs  []string `avro:"product_ids"`
	Timestamp   string   `avro:"timestamp"`
}

func getEnv(key, fallback string) string {
	if val, exists := os.LookupEnv(key); exists {
		return val
	}
	return fallback
}

func loadTLSConfig(caPath string) (*tls.Config, error) {
	cfg := &tls.Config{InsecureSkipVerify: true}
	if caPath == "" {
		return cfg, nil
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return cfg, nil
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return cfg, nil
	}
	cfg.RootCAs = pool
	cfg.InsecureSkipVerify = false
	return cfg, nil
}

func NewService() (*Service, error) {
	topic := getEnv("ANALYTICS_TOPIC", defaultAnalyticsTopic)
	registryURL := getEnv("SCHEMA_REGISTRY_URL", defaultSchemaRegistryURL)
	subject := getEnv("ANALYTICS_SCHEMA_SUBJECT", topic+"-value")
	broker := getEnv("BROKER", defaultBroker)
	username := getEnv("SASL_USERNAME", defaultUsername)
	password := getEnv("SASL_PASSWORD", defaultPassword)
	caPath := getEnv("CA_PATH", defaultCaPath)

	tlsConfig, err := loadTLSConfig(caPath)
	if err != nil {
		return nil, fmt.Errorf("load TLS: %w", err)
	}

	// Используем plain.Mechanism
	transport := &kafka.Transport{
		TLS:  tlsConfig,
		SASL: plain.Mechanism{
			Username: username,
			Password: password,
		},
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		Transport:    transport,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}


	sr := srclient.CreateSchemaRegistryClient(registryURL)
	sr.SetCredentials(username, password)
	sr.CodecCreationEnabled(true)
	schema, err := sr.GetLatestSchema(subject)
	if err != nil {
		return nil, fmt.Errorf("get schema %s: %w", subject, err)
	}

	return &Service{writer: writer, schema: schema}, nil
}

func (s *Service) Close() error {
	if s == nil || s.writer == nil {
		return nil
	}
	return s.writer.Close()
}

func (s *Service) encode(event Event) ([]byte, error) {
	native := map[string]interface{}{
		"event_type":   event.EventType,
		"user":         event.User,
		"query":        event.Query,
		"found":        event.Found,
		"result_count": event.ResultCount,
		"product_ids":  event.ProductIDs,
		"timestamp":    event.Timestamp,
	}
	bin, err := s.schema.Codec().BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 5+len(bin))
	out[0] = 0
	binary.BigEndian.PutUint32(out[1:5], uint32(s.schema.ID()))
	copy(out[5:], bin)
	return out, nil
}

func (s *Service) Publish(ctx context.Context, key string, event Event) error {
	if event.ProductIDs == nil {
		event.ProductIDs = []string{}
	}
	if event.Timestamp == "" {
		event.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}

	payload, err := s.encode(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	msg := kafka.Message{Value: payload}
	if key != "" {
		msg.Key = []byte(key)
	}
	if err := s.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write event: %w", err)
	}
	return nil
}

func (s *Service) PublishSearch(ctx context.Context, user, query string, productIDs []string) error {
	if productIDs == nil {
		productIDs = []string{}
	}
	return s.Publish(ctx, user, Event{
		EventType:   "product_search",
		User:        user,
		Query:       query,
		Found:       len(productIDs) > 0,
		ResultCount: len(productIDs),
		ProductIDs:  productIDs,
		Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
	})
}