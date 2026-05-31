package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/BelinskiyAA/kafka/Module-1/Practice-3/internal/models"
	"github.com/lovoo/goka"
)

// optional code omitted

type Server struct {
	Emitter       *goka.Emitter
	EmmitterBlock *goka.Emitter
}

func NewServer(emitter *goka.Emitter, emitterBlock *goka.Emitter) Server {
	return Server{Emitter: emitter, EmmitterBlock: emitterBlock}
}

func (s Server) Send(w http.ResponseWriter, r *http.Request) {

	defer r.Body.Close()

	id, err := strconv.ParseInt(r.PathValue("user_id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid user_id", http.StatusBadRequest)
		return
	}

	var recieveMes sendMessage
	err = json.NewDecoder(r.Body).Decode(&recieveMes)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	var m models.Message
	m.UserID = id
	m.RecipientID = recieveMes.RecipientID
	m.Message = recieveMes.Message
	m.Timestamp = time.Now().Unix()

	err = s.Emitter.EmitSync(strconv.FormatInt(m.RecipientID, 10), m)
	if err != nil {
		fmt.Fprintf(w, "error: %v", err)
		return
	}
	log.Printf("Sent message:\n %v\n", m)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode("Send OK!")
}

func (s Server) Block(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	id, err := strconv.ParseInt(r.PathValue("user_id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid user_id", http.StatusBadRequest)
		return
	}

	var block models.Block
	err = json.NewDecoder(r.Body).Decode(&block)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	block.UserID = id
	block.Timestamp = time.Now().Unix()
	block.Cmd = "block"

	key := fmt.Sprintf("%d_%d", id, block.BlockedUserID)
	err = s.EmmitterBlock.EmitSync(key, block)
	if err != nil {
		fmt.Fprintf(w, "error: %v", err)
		return
	}
	log.Printf("Block message:\n %v\n", block)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode("Block OK")
}

func (s Server) UnBlock(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	id, err := strconv.ParseInt(r.PathValue("user_id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid user_id", http.StatusBadRequest)
		return
	}

	var block models.Block
	err = json.NewDecoder(r.Body).Decode(&block)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	block.UserID = id
	block.Timestamp = time.Now().Unix()
	block.Cmd = "unblock"

	key := fmt.Sprintf("%d_%d", id, block.BlockedUserID)
	err = s.EmmitterBlock.EmitSync(key, block)
	if err != nil {
		fmt.Fprintf(w, "error: %v", err)
		return
	}
	log.Printf("Un Block message:\n %v\n", block)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode("Unblock OK")
}

type sendMessage struct {
	RecipientID int64  `json:"to"`
	Message     string `json:"message"`
}
