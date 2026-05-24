package main

import (
	"errors"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type mockCommitter struct {
	commitCalled int
	commitResult []kafka.TopicPartition
	commitErr    error
}

func (m *mockCommitter) Commit() ([]kafka.TopicPartition, error) {
	m.commitCalled++
	return m.commitResult, m.commitErr
}

func TestDoCommit_Success(t *testing.T) {

	topic := "test-topic"
	mock := &mockCommitter{
		commitResult: []kafka.TopicPartition{
			{Topic: &topic, Offset: 42},
		},
	}

	doCommit(mock)
	if mock.commitCalled != 1 {
		t.Errorf("expected 1 commit call, got %d", mock.commitCalled)
	}
}

func TestDoCommit_Error(t *testing.T) {
	mock := &mockCommitter{
		commitErr: errors.New("commit failed"),
	}

	doCommit(mock)

	if mock.commitCalled != 1 {
		t.Errorf("expected 1 commit call, got %d", mock.commitCalled)
	}
}

func TestDoCommit_EmptyPartitions(t *testing.T) {
	mock := &mockCommitter{
		commitResult: []kafka.TopicPartition{},
	}

	doCommit(mock)

	if mock.commitCalled != 1 {
		t.Errorf("expected 1 commit call, got %d", mock.commitCalled)
	}
}

func TestDoCommit_NilPartitions(t *testing.T) {
	mock := &mockCommitter{
		commitResult: nil,
	}

	doCommit(mock)

	if mock.commitCalled != 1 {
		t.Errorf("expected 1 commit call, got %d", mock.commitCalled)
	}
}
