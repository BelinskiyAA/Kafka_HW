package models

import (
	"encoding/json"
	"fmt"
)

type Message struct {
	UserID      int64  `json:"user_id"`
	RecipientID int64  `json:"recipient_id"`
	Message     string `json:"message"`
	Timestamp   int64  `json:"timestamp"`
}

type MessageCodec struct{}

func (c *MessageCodec) Encode(value interface{}) ([]byte, error) {
	if message, ok := value.(Message); ok {
		return json.Marshal(message)
	}
	return nil, fmt.Errorf("illegal type: %T", value)
}

func (c *MessageCodec) Decode(data []byte) (interface{}, error) {
	var m Message
	return &m, json.Unmarshal(data, &m)
}
