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

type Committer interface {
	Commit() ([]kafka.TopicPartition, error)
}

func main() {

	if len(os.Args) < 5 {
		log.Fatalf("Пример использования: %s <bootstrap-servers> <group> <batchSize> <topics..>\n", os.Args[0])
	}
	// Парсим параметры и получаем адрес брокера, группу и имя топиков
	bootstrapServers := os.Args[1]
	group := os.Args[2]
	batchSize, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Fprintf(os.Stderr, "<batchSize> must be integer: %s\n", err)
		os.Exit(1)
	}
	if batchSize <= 0 {
		fmt.Fprint(os.Stderr, "<batchSize> must be positive\n")
		os.Exit(1)
	}
	topics := os.Args[4:]

	// Перехватываем сигналы syscall.SIGINT и syscall.SIGTERM для graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Создаём консьюмера
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":   bootstrapServers,
		"group.id":            group,                        // уникальный идентификатор группы
		"session.timeout.ms":  6000,                         // время, в течение которого Kafka ожидает активности от консьюмера до того, как объявит его «мёртвым» и начнёт ребалансировку
		"enable.auto.commit":  false,                        // отключаем автоматическое сохраненте смещения (offsets) после получения сообщений
		"queued.min.messages": min(batchSize*100, 10000000), //librdkafka пытается поддерживать минимальное количество сообщений на topic+partition в локальной очереди консьюмера.
		"isolation.level":     "read_committed",
		"auto.offset.reset":   "earliest"}) // начинать чтение с самого старого доступного сообщения.

	if err != nil {
		log.Fatalf("Невозможно создать консьюмера: %s\n", err)
	}

	// Закрываем потребителя
	defer c.Close()

	fmt.Printf("Консьюмер создан %v\n", c)

	err = c.SubscribeTopics(topics, nil)

	if err != nil {
		log.Fatalf("Невозможно подписаться на топик: %s\n", err)
	}

	run := true
	var batchCounter int
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Передан сигнал %v: приложение останавливается\n", sig)
			if batchCounter > 0 {
				doCommit(c)
			}
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				value := Message{}
				err := json.Unmarshal(e.Value, &value)
				if err != nil {
					fmt.Printf("Ошибка десериализации: %s\n", err)
					log.Printf("Ошибка десериализации: %s\n", err)
				} else {
					fmt.Printf("%% Получено сообщение в топик %s:\n%+v\n", e.TopicPartition, value)
				}

				batchCounter++

				if e.Headers != nil {
					fmt.Printf("%% Заголовки: %v\n", e.Headers)
				}
				if batchCounter == batchSize {
					doCommit(c)
					batchCounter = 0
				}
			case kafka.Error:
				// Ошибки обычно следует считать информационными, клиент попытается автоматически их восстановить
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				log.Printf("%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					if batchCounter > 0 {
						doCommit(c)
					}
					run = false
				}
			default:
				fmt.Printf("Другие события %v\n", e)
			}
		}
	}
}

func doCommit(consumer Committer) {
	info, err := consumer.Commit()
	if err != nil {
		fmt.Printf("Commit error: %s\n", err)
		log.Printf("Commit topic error: %s", err)
		return
	}
	if len(info) > 0 {
		last := info[len(info)-1]
		fmt.Printf("Commited Topic %s offset %s \n", *last.Topic, last.Offset.String())
	}

}
