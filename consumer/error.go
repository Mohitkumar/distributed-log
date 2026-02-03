package consumer

import (
	"fmt"
)

// ErrOffsetNotFoundForID returns error when offset not found for consumer id and topic.
func ErrOffsetNotFoundForID(id, topic string) error {
	return fmt.Errorf("offset not found for id: %s and topic: %s", id, topic)
}
