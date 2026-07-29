package consumer

import (
	"errors"
	"fmt"
)

var ErrConsumerOffsetNotFound = errors.New("consumer: offset not found for id/topic")

// ErrOffsetNotFoundForID returns an error when no stored offset exists for the given consumer id and topic.
func ErrOffsetNotFoundForID(id, topic string) error {
	return fmt.Errorf("offset not found for id: %s and topic: %s: %w", id, topic, ErrConsumerOffsetNotFound)
}
