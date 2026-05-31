package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/BelinskiyAA/kafka/Module-1/Practice-3/internal/models"
	"github.com/lovoo/goka"
)

var (
	brokers                = []string{"kafka:9094"}
	topicWords goka.Stream = "blocked-words"

	addCmd string = "add"
	rmCmd  string = "rm"
)

func main() {

	cmd := flag.String("cmd", "none", "add or rm word from censorship")
	word := flag.String("word", "", "word")

	flag.Parse()

	trimWord := strings.TrimSpace(*word)
	if trimWord == "" {
		fmt.Println("Empty 'word' parameter value")
		return
	}

	var censorWord models.Word
	censorWord.Word = trimWord

	switch *cmd {
	case addCmd, rmCmd:
		censorWord.Cmd = *cmd
	default:
		fmt.Println("Wrong 'cmd' parameter value")
		return
	}

	emitter, err := goka.NewEmitter(brokers, topicWords, new(models.WordCodec))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	key := "censor"
	err = emitter.EmitSync(key, censorWord)
	if err != nil {
		fmt.Printf("error: %v", err)
		return
	}
	fmt.Println("OK")
}
