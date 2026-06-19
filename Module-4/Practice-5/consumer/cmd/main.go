package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type User struct {
	UserID   int64  `json:"id"`
	Name     string `json:"name"`
	Email    string `json:"email"`
	CreateAt int64  `json:"created_at"`

	/*
		CREATE TABLE orders (
		    id SERIAL PRIMARY KEY,
		    user_id INT REFERENCES users(id),
		    product_name VARCHAR(100),
		    quantity INT,
		    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

	*/
}

// Таймаут для запроса
const timeoutMs = 100

func main() {

	if len(os.Args) < 2 {
		log.Fatalf("Пример использования: %s <bootstrap-servers>\n",
			os.Args[0])
	}
	// Парсим параметры и получаем адрес брокера, группу и имя топиков
	bootstrapServers := os.Args[1]
	group := "test_group"
	topic := []string{"customers.public.users"}

	// Перехватываем сигналы syscall.SIGINT и syscall.SIGTERM для graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Создаём консьюмера
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           group,
		"session.timeout.ms": 6000,        // время, в течение которого Kafka ожидает активности от консьюмера до того, как объявит его «мёртвым» и начнёт ребалансировку
		"enable.auto.commit": true,        // автоматически сохранять смещения (offsets) после получения сообщений
		"auto.offset.reset":  "earliest"}) // начинать чтение с самого старого доступного сообщения.

	if err != nil {
		log.Fatalf("Невозможно создать консьюмера: %s\nOoopps!\n", err)
		os.Exit(1)
	}

	// Закрываем потребителя
	defer c.Close()

	fmt.Printf("Консьюмер создан %v\n", c)

	// Подписываемся на топик
	err = c.SubscribeTopics(topic, nil)

	if err != nil {
		log.Fatalf("Невозможно подписаться на топик: %s\n", err)
	}

	run := true
	// Запускаем бесконечный цикл
	for run {
		select {
		// Для выхода нажмите ctrl+C
		case sig := <-sigchan:
			fmt.Printf("Передан сигнал %v: приложение останавливается\n", sig)
			run = false
		default:

			// Делаем запрос на считывание сообщения из брокера
			ev := c.Poll(timeoutMs)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				value := User{}
				err := json.Unmarshal(e.Value, &value)
				if err != nil {
					fmt.Printf("Ошибка десериализации: %s\n", err)
				} else {
					fmt.Printf("%% Получено сообщение в топик %s:\n%+v\n", e.TopicPartition, value)
				}
				if e.Headers != nil {
					fmt.Printf("%% Заголовки: %v\n", e.Headers)
				}
			case kafka.Error:
				// Ошибки обычно следует считать информационными, клиент попытается автоматически их восстановить
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
			default:
				fmt.Printf("Другие события %v\n", e)
			}
		}
	}
}
