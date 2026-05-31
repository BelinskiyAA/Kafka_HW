package main

import (
	"context"
	"fmt"
	"log"

	"github.com/BelinskiyAA/kafka/Module-1/Practice-3/internal/models"
	"github.com/lovoo/goka"
)

var (
	brokers = []string{"kafka:9094"}

	topicFilterMessages goka.Stream = "filtered_messages"
	groupFilterMessages goka.Group  = "filtered_messages-group"
)

func main() {
	go readMessagesProcessor()

	select {} // Блокируем main, чтобы горутины работали
}

func readMessagesProcessor() {
	fmt.Println("Start readMessage processor")
	processFunc := func(ctx goka.Context, msg interface{}) {
		var (
			message *models.Message
			ok      bool
		)

		if message, ok = msg.(*models.Message); !ok {
			log.Printf("illegal type: %T", msg)
			return
		}

		log.Printf("Get message from: %d to: %d message: %s\n", message.UserID, message.RecipientID, message.Message)
	}

	g := goka.DefineGroup(groupFilterMessages,
		goka.Input(topicFilterMessages, new(models.MessageCodec), processFunc),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Stop()

	if err = p.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
