package cluster

import (
	"errors"
	"fmt"
)

var ErrNotEnoughNodes = errors.New("not enough nodes")

func ErrNotEnoughNodesf(need, have int) error {
	return fmt.Errorf("not enough nodes: need %d, have %d: %w", need, have, ErrNotEnoughNodes)
}
