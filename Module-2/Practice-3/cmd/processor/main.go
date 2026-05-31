package main

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"

	"github.com/BelinskiyAA/kafka/Module-1/Practice-3/internal/models"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers = []string{"kafka:9094"}

	topicMessages       goka.Stream = "messages"
	topicFilterMessages goka.Stream = "filtered_messages"

	groupFilterMessages goka.Group = "filtered_messages-group"

	topicBlock      goka.Stream = "blocked_users"
	groupBlock      goka.Group  = "blocked_users-group"
	groupBlockTable goka.Table  = goka.GroupTable(groupBlock)

	blockVal   int64 = 1
	unBlockVal int64 = 0

	topicWord      goka.Stream = "blocked-words"
	groupWord      goka.Group  = "blocked-words-group"
	groupWordTable goka.Table  = goka.GroupTable(groupWord)
)

func main() {
	go messagesProcessor()
	go blockProcessor()
	go censorWordProcessor()

	select {} // Блокируем main, чтобы горутины работали
}

func messagesProcessor() {
	fmt.Println("Start message processor")
	processFunc := func(ctx goka.Context, msg interface{}) {
		var (
			message *models.Message
			ok      bool
		)

		if message, ok = msg.(*models.Message); !ok {
			log.Printf("illegal type: %T", msg)
			return
		}

		// Маскируем слова
		var words []string

		if v := ctx.Lookup(groupWordTable, "censor"); v != nil {
			words = v.([]string)
		}

		// Используем самый простой "алгоритм" для того чтобы показать обработку в рамках задания
		log.Print(words)
		mes := message.Message

		for _, val := range words {
			mes = strings.Replace(mes, val, "***", -1)
		}
		log.Printf("Clear message\nBefore %s\nAfter  %s", message.Message, mes)
		message.Message = mes

		// Проверяем не заблокирован ли пользователь
		key := fmt.Sprintf("%d_%d", message.RecipientID, message.UserID)

		blocked := ctx.Lookup(groupBlockTable, key)
		if blocked == nil {
			blocked = 0
		}
		log.Printf("Blocked %d %T  %s", blocked, blocked, key)
		if blocked != blockVal {

			log.Printf("Transfer message From %d to %d", message.UserID, message.RecipientID)
			ctx.Emit(topicFilterMessages, ctx.Key(), *message)
		} else {
			log.Printf("Blocked message From %d to %d", message.UserID, message.RecipientID)
		}
	}

	g := goka.DefineGroup(groupFilterMessages,
		goka.Input(topicMessages, new(models.MessageCodec), processFunc),
		goka.Lookup(groupBlockTable, new(codec.Int64)),
		goka.Lookup(groupWordTable, new(models.WordsListCodec)),
		goka.Output(topicFilterMessages, new(models.MessageCodec)),
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

func blockProcessor() {
	fmt.Println("Start block processor")
	processFunc := func(ctx goka.Context, msg interface{}) {
		var (
			block *models.Block
			ok    bool
		)

		if block, ok = msg.(*models.Block); !ok {
			log.Printf("illegal type: %T", msg)
			return // Не останавливаем процессор
		}

		currentStatus := ctx.Value()
		switch block.Cmd {
		case "block":
			if currentStatus != blockVal {
				ctx.SetValue(blockVal)
				fmt.Println("Set blocked")
			}
		case "unblock":
			if currentStatus != unBlockVal {
				ctx.SetValue(unBlockVal)
				fmt.Println("Set unblocked")
			}
		}
	}

	g := goka.DefineGroup(groupBlock,
		goka.Input(topicBlock, new(models.BlockCodec), processFunc),
		goka.Persist(new(codec.Int64)),
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

func censorWordProcessor() {
	fmt.Println("Start censor word processor")
	processFunc := func(ctx goka.Context, msg interface{}) {
		var (
			word   *models.Word
			ok     bool
			addCmd string = "add"
			rmCmd  string = "rm"
		)

		if word, ok = msg.(*models.Word); !ok {
			log.Printf("illegal type: %T", msg)
			return
		}

		var words []string
		if v := ctx.Value(); v != nil {
			words = v.([]string)
		}

		index := slices.Index(words, word.Word)
		isChange := false

		switch word.Cmd {
		case addCmd:
			if index == -1 {
				words = append(words, word.Word)
				isChange = true
				log.Printf("Add word '%s' to the censorship list.\n", word.Word)
			}
		case rmCmd:
			if index != -1 {
				words[index] = words[len(words)-1]
				words[len(words)-1] = ""
				words = words[:len(words)-1]
				isChange = true
				log.Printf("Remove word '%s' to the censorship list.\n", word.Word)
			}
		}

		if isChange {
			ctx.SetValue(words)
		}
	}

	g := goka.DefineGroup(groupWord,
		goka.Input(topicWord, new(models.WordCodec), processFunc),
		goka.Persist(new(models.WordsListCodec)),
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
