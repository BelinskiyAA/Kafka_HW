package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/riferrei/srclient"
)

const (
	broker            = "rc1a-ftd2fnd2qsdf6mgh.mdb.yandexcloud.net:9091"
	schemaRegistryURL = "https://rc1a-ftd2fnd2qsdf6mgh.mdb.yandexcloud.net:443"
	username          = "test"
	password          = "testtest"
	yandexCAPath      = "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
)

func main() {
	topic := "practice7"
	subject := topic + "-value"

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"security.protocol": "SASL_SSL",
		// "sasl.mechanism":    "SCRAM-SHA-512",
		"sasl.mechanism":    "PLAIN",
		"sasl.username":     username,
		"sasl.password":     password,
		"ssl.ca.location":   yandexCAPath,
	})
	if err != nil {
		log.Fatalf("producer: %s", err)
	}
	defer producer.Close()

	fmt.Println("Start schema")
	httpClient, err := httpClientWithYandexCA()
	if err != nil {
		log.Fatalf("tls: %s", err)
	}
	sr := srclient.NewSchemaRegistryClient(schemaRegistryURL, srclient.WithClient(httpClient))
	sr.SetCredentials(username, password)
	sr.CodecCreationEnabled(true)

	fmt.Println("Get schema")
	schema, err := sr.GetLatestSchema(subject)
	if err != nil {
		log.Fatalf("schema %s: %s", subject, err)
	}
	fmt.Printf("schema subject=%s id=%d\n", subject, schema.ID())

	fmt.Println("Payload")
	payload, err := encodeAvro(schema, map[string]any{"message_id": "mess_777", "user_id": "123"})
	if err != nil {
		log.Fatalf("encode: %s", err)
	}

	fmt.Println("Start send")
	if err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
	}, nil); err != nil {
		log.Fatalf("produce: %s", err)
	}

	producer.Flush(15_000)
	fmt.Println("message sent")
}

func encodeAvro(schema *srclient.Schema, native map[string]any) ([]byte, error) {
	bin, err := schema.Codec().BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 5+len(bin))
	out[0] = 0
	binary.BigEndian.PutUint32(out[1:5], uint32(schema.ID()))
	copy(out[5:], bin)
	return out, nil
}

func httpClientWithYandexCA() (*http.Client, error) {
	caCert, err := os.ReadFile(yandexCAPath)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("invalid CA: %s", yandexCAPath)
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}, nil
}
