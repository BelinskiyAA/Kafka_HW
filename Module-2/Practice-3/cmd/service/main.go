package main

import (
	"fmt"
	"net/http"

	"github.com/BelinskiyAA/kafka/Module-1/Practice-3/internal/handlers"
	"github.com/BelinskiyAA/kafka/Module-1/Practice-3/internal/models"
	"github.com/lovoo/goka"
)

var (
	brokers             = []string{"kafka:9094"} // Адрес брокера
	input   goka.Stream = "messages"             // Топик с исходными данными
	block   goka.Stream = "blocked_users"        // Топик с заблокированными пользователями
)

func main() {

	mux := http.NewServeMux()

	emitter, err := goka.NewEmitter(brokers, input, new(models.MessageCodec))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	emitterBlock, err := goka.NewEmitter(brokers, block, new(models.BlockCodec))
	if err != nil {
		panic(err)
	}
	defer emitterBlock.Finish()

	server := handlers.NewServer(emitter, emitterBlock)

	mux.HandleFunc("POST /{user_id}/send", server.Send)
	mux.HandleFunc("POST /{user_id}/block", server.Block)
	mux.HandleFunc("POST /{user_id}/unblock", server.UnBlock)

	fmt.Println("Starting server at port 8000")
	err = http.ListenAndServe(":8000", mux)
	if err != nil {
		fmt.Println("Error starting the server:", err)
	}

}
