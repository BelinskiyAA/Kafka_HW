package models

import (
	"encoding/json"
	"fmt"
)

type Block struct {
	UserID        int64  `json:"user_id"`
	BlockedUserID int64  `json:"blocked_user_id"`
	Cmd           string `json:"cmd"`
	Timestamp     int64  `json:"timestamp"`
}

type BlockCodec struct{}

func (c *BlockCodec) Encode(value interface{}) ([]byte, error) {
	if block, ok := value.(Block); ok {
		return json.Marshal(block)
	}
	return nil, fmt.Errorf("illegal type: %T", value)
}

func (c *BlockCodec) Decode(data []byte) (interface{}, error) {
	var m Block
	return &m, json.Unmarshal(data, &m)
}
