package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// User is a simple record example
type User struct {
	Name           string `json:"name"`
	FavoriteNumber int    `json:"favorite_number"`
	FavoriteColor  string `json:"favorite_color"`
}

func main() {

	if len(os.Args) != 3 {
		// kafka-1:1092
		log.Fatalf("Пример использования: %s <bootstrap-servers> <topic>\n", os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	topic := os.Args[2]

	// SSL and SASL configuration
	sslConfig := &kafka.ConfigMap{
		"bootstrap.servers":                   bootstrapServers,
		"security.protocol":                   "SASL_SSL",
		"sasl.mechanism":                      "PLAIN",
		"sasl.username":                       "producer",
		"sasl.password":                       "producer-secret",
		"enable.ssl.certificate.verification": false,
	}

	// Create a new producer
	p, err := kafka.NewProducer(sslConfig)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer p.Close()

	fmt.Printf("Created Producer %v\n", p)

	// Прослушиваем все события на канале событий по умолчанию.
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				// The message delivery report, indicating success or
				// permanent failure after retries have been exhausted.
				// Application level retries won't help since the client
				// is already configured to do that.
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Ошибка доставки сообщения: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Сообщение отправлено в топик %s [%d] оффсет %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			case kafka.Error:
				// Generic client instance-level errors, such as
				// broker connection failures, authentication issues, etc.
				//
				// These errors should generally be considered informational
				// as the underlying client will automatically try to
				// recover from any errors encountered, the application
				// does not need to take action on them.
				fmt.Printf("Ошибка: %v\n", ev)
			default:
				fmt.Printf("Игнорируем событие: %s\n", ev)
			}
		}
	}()

	totalMsgcnt := 10
	msgcnt := 0
	for msgcnt < totalMsgcnt {
		// value := fmt.Sprintf("Producer example, message #%d", msgcnt)

		value := User{
			Name:           "First user",
			FavoriteNumber: rand.IntN(1000),
			FavoriteColor:  "blue",
		}

		payload, err := json.Marshal(value)
		if err != nil {
			log.Fatalf("Невозможно сериализовать сообщение: %s\n", err)
			continue
		}

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          payload,
			Headers:        []kafka.Header{{Key: "Msg_id", Value: []byte(fmt.Sprintf("msg_%d", msgcnt))}},
		}, nil)

		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrQueueFull {
				// Producer queue is full, wait 1s for messages
				// to be delivered then try again.
				time.Sleep(time.Second)
				continue
			}
			fmt.Printf("Ошибка доставки сообщения: %v\n", err)
		}
		msgcnt++
	}

	// Flush and close the producer and the events channel
	for p.Flush(10000) > 0 {
		fmt.Print("Ожидаем завершения обработки незаверщшенных сообщений\n")
	}
}
