package cluster

import "sync"

type ClusterMetadataStore struct {
	mu sync.RWMutex
}
