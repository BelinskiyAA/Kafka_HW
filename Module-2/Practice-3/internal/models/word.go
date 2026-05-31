package models

import (
	"encoding/json"
	"fmt"
)

type Word struct {
	Word string `json:"word"`
	Cmd  string `json:"cmd"`
}

type WordCodec struct{}

func (c *WordCodec) Encode(value interface{}) ([]byte, error) {
	if word, ok := value.(Word); ok {
		return json.Marshal(word)
	}
	return nil, fmt.Errorf("illegal type: %T", value)
}

func (c *WordCodec) Decode(data []byte) (interface{}, error) {
	var m Word
	return &m, json.Unmarshal(data, &m)
}

type WordsListCodec struct{}

func (c *WordsListCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *WordsListCodec) Decode(data []byte) (interface{}, error) {
	var m []string
	err := json.Unmarshal(data, &m)
	return m, err
}
