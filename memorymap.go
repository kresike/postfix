package postfix

import (
	"fmt"
	"sync"
)

// MemoryMap is a lock protected map storing key value pairs
type MemoryMap struct {
	mu sync.Mutex
	v  map[string]string
}

// NewMemoryMap creates a new MemoryMap structure
func NewMemoryMap() *MemoryMap {
	var m MemoryMap
	m.v = make(map[string]string)
	return &m
}

// Add adds a new key/value pair to the map
func (m *MemoryMap) Add(k, v string) {
	m.mu.Lock()
	m.v[k] = v
	m.mu.Unlock()
}

// Get returns the value stored under key in the map or error if not found
func (m *MemoryMap) Get(k string) (value string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.v[k]
	if !ok {
		return "", fmt.Errorf("Key not found")
	}
	return value, nil
}

// Remove removes a key from the map
func (m *MemoryMap) Remove(k string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.v, k)
}

// Clear clears the entire map
func (m *MemoryMap) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.v = make(map[string]string)
}
