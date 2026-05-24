package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const PARTITION_AMOUNT = 3

type Row struct {
	RowID  string  `json:"row_id"`
	Value1 int     `json:"value1"`
	Value2 float64 `json:"value2"`
}

type Message struct {
	MessageID string `json:"message_id"`
	UserID    string `json:"user_id"`
	Rows      []Row  `json:"rows"`
}

func main() {
	if len(os.Args) != 4 {
		log.Fatalf("Пример использования: %s <bootstrap-servers> <topic> <message amount>\n", os.Args[0])
		os.Exit(1)
	}

	bootstrapServers := os.Args[1]
	topic := os.Args[2]

	totalMsgcnt, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalf("Не верный формат передаваемого параметра %s\n", os.Args[3])
		os.Exit(1)
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"acks":               "all", // Guarantee message is written to all replicas
		"enable.idempotence": true,  // Recommended: prevent duplicates during retries
		"retries":            100,   // Allow many retries for transient issues

	})

	if err != nil {
		log.Fatalf("Невозможно создать продюсера: %s\n", err)
		os.Exit(1)
	}

	defer p.Close()

	log.Printf("Продюсер создан %v\n", p)

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
					fmt.Printf("Сообщение отправлено в топик %s [%d] оффсет %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
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

	msgcnt := 0
	for msgcnt < totalMsgcnt {

		userID := rand.IntN(12)
		value := &Message{
			MessageID: fmt.Sprintf("msg_%d", msgcnt),
			UserID:    fmt.Sprintf("User_%d", userID),
			Rows: []Row{
				{RowID: "test_535", Value1: 777, Value2: 1.234},
				{RowID: "test_987", Value1: 23, Value2: 0.567},
			},
		}

		payload, err := json.Marshal(value)
		if err != nil {
			log.Fatalf("Невозможно сериализовать сообщение: %s\n", err)
			continue
		}

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: (int32)(userID % PARTITION_AMOUNT)},
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
