package consumer

import (
	"fmt"
)

func ErrOffsetNotFoundForID(id, topic string) error {
	return fmt.Errorf("offset not found for id: %s and topic: %s", id, topic)
}
