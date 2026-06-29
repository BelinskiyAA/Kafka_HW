package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type User struct {
	Name           string `json:"name"`
	FavoriteNumber int    `json:"favorite_number"`
	FavoriteColor  string `json:"favorite_color"`
}

func main() {

	if len(os.Args) < 4 {
		log.Fatalf("Пример использования: %s <bootstrap-servers> <batchSize> <topics..>\n", os.Args[0])
	}
	// Парсим параметры и получаем адрес брокера, группу и имя топиков
	bootstrapServers := os.Args[1]
	batchSize, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Fprintf(os.Stderr, "<batchSize> must be integer: %s\n", err)
		os.Exit(1)
	}
	topics := os.Args[3:]

	// Перехватываем сигналы syscall.SIGINT и syscall.SIGTERM для graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Создаём консьюмера
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":                   bootstrapServers,
		"group.id":                            "group",                      // уникальный идентификатор группы
		"session.timeout.ms":                  6000,                         // время, в течение которого Kafka ожидает активности от консьюмера до того, как объявит его «мёртвым» и начнёт ребалансировку
		"enable.auto.commit":                  false,                        // отключаем автоматическое сохраненте смещения (offsets) после получения сообщений
		"queued.min.messages":                 min(batchSize*100, 10000000), //librdkafka пытается поддерживать минимальное количество сообщений на topic+partition в локальной очереди консьюмера.
		"isolation.level":                     "read_committed",
		"auto.offset.reset":                   "earliest",
		"security.protocol":                   "SASL_SSL",
		"sasl.mechanism":                      "PLAIN",
		"sasl.username":                       "consumer",
		"sasl.password":                       "consumer-secret",
		"enable.ssl.certificate.verification": false,
	}) // начинать чтение с самого старого доступного сообщения.

	if err != nil {
		log.Fatalf("Невозможно создать консьюмера: %s\n", err)
		os.Exit(1)
	}

	// Закрываем потребителя
	defer c.Close()

	fmt.Printf("Консьюмер создан %v\n", c)

	err = c.SubscribeTopics(topics, nil)

	if err != nil {
		log.Fatalf("Невозможно подписаться на топик: %s\n", err)
		os.Exit(1)
	}

	run := true
	var controller int
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Передан сигнал %v: приложение останавливается\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				if controller != 0 {
					doCommit(c)
					controller = 0
				}
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				controller++
				if controller == batchSize {
					doCommit(c)
					controller = 0
				}
				value := User{}
				err := json.Unmarshal(e.Value, &value)
				if err != nil {
					fmt.Printf("Ошибка десериализации: %s\n", err)
					log.Fatalf("Ошибка десериализации: %s\n", err)
				} else {
					fmt.Printf("%% Получено сообщение в топик %s:\n%+v\n", e.TopicPartition, value)
				}
				if e.Headers != nil {
					fmt.Printf("%% Заголовки: %v\n", e.Headers)
				}
			case kafka.Error:
				// Ошибки обычно следует считать информационными, клиент попытается автоматически их восстановить
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				log.Fatalf("%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Другие события %v\n", e)
			}
		}
	}
}

func doCommit(consumer *kafka.Consumer) {
	info, err := consumer.Commit()
	fmt.Printf("Commited Topic %s offset %s \n", *info[len(info)-1].Topic, info[len(info)-1].Offset.String())
	if err != nil {
		fmt.Printf("Error %s", err)
		log.Fatalf("Commite topic error %s", err)
		panic("Ошибка подтверждения получения сообщения.")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
