package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/riferrei/srclient"
)

const (
	broker            = "rc1a-ftd2fnd2qsdf6mgh.mdb.yandexcloud.net:9091"
	schemaRegistryURL = "https://rc1a-ftd2fnd2qsdf6mgh.mdb.yandexcloud.net:443"
	username          = "practice7"
	password          = "practice7"
	yandexCAPath      = "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
)

func main() {

	topic := "practice7"
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          "go-hw6-consumer",
		"auto.offset.reset": "earliest",
		"security.protocol": "SASL_SSL",
//		"sasl.mechanism":    "SCRAM-SHA-512",
		"sasl.mechanism":    "PLAIN",
		"sasl.username":     username,
		"sasl.password":     password,
		"ssl.ca.location":   yandexCAPath,
	})
	if err != nil {
		log.Fatalf("consumer: %s", err)
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		log.Fatalf("subscribe: %s", err)
	}

	httpClient, err := httpClientWithYandexCA()
	if err != nil {
		log.Fatalf("tls: %s", err)
	}
	sr := srclient.NewSchemaRegistryClient(schemaRegistryURL, srclient.WithClient(httpClient))
	sr.SetCredentials(username, password)
	sr.CodecCreationEnabled(true)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("waiting for messages (Ctrl+C to stop)...")

run:
	for {
		select {
		case <-sigchan:
			break run
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if ke, ok := err.(kafka.Error); ok && ke.Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("read: %s", err)
				continue
			}

			value, err := decodeAvro(sr, msg.Value)
			if err != nil {
				log.Printf("decode: %s", err)
				continue
			}

			fmt.Printf("topic=%s partition=%d offset=%d value=%s\n",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, value)
		}
	}
}

func decodeAvro(sr *srclient.SchemaRegistryClient, payload []byte) (string, error) {
	if len(payload) < 5 || payload[0] != 0 {
		return "", fmt.Errorf("not confluent avro wire format")
	}
	schemaID := binary.BigEndian.Uint32(payload[1:5])
	schema, err := sr.GetSchema(int(schemaID))
	if err != nil {
		return "", err
	}
	native, _, err := schema.Codec().NativeFromBinary(payload[5:])
	if err != nil {
		return "", err
	}
	text, err := schema.Codec().TextualFromNative(nil, native)
	if err != nil {
		return "", err
	}
	return string(text), nil
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
