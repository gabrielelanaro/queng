package distributed

import (
	"fmt"
	"iter"
	"sync"

	"github.com/gabrielelanaro/queng/datasource"
	"github.com/gabrielelanaro/queng/execution"
)

// MemoryExchange stores stage output in memory for local execution.
type MemoryExchange struct {
	mu      sync.Mutex
	batches map[execution.ExchangeKey][]datasource.RecordBatch
	closed  bool
}

func NewMemoryExchange() *MemoryExchange {
	return &MemoryExchange{batches: make(map[execution.ExchangeKey][]datasource.RecordBatch)}
}

func (m *MemoryExchange) Write(key execution.ExchangeKey, batch datasource.RecordBatch) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("exchange is closed")
	}
	batch.Retain()
	m.batches[key] = append(m.batches[key], batch)
	return nil
}

func (m *MemoryExchange) Read(key execution.ExchangeKey) iter.Seq2[datasource.RecordBatch, error] {
	return func(yield func(datasource.RecordBatch, error) bool) {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			yield(datasource.RecordBatch{}, fmt.Errorf("exchange is closed"))
			return
		}
		batches := make([]datasource.RecordBatch, 0, len(m.batches[key]))
		for _, batch := range m.batches[key] {
			batch.Retain()
			batches = append(batches, batch)
		}
		m.mu.Unlock()

		for i, batch := range batches {
			if !yield(batch, nil) {
				for _, remaining := range batches[i+1:] {
					remaining.Release()
				}
				return
			}
		}
	}
}

func (m *MemoryExchange) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	for _, batches := range m.batches {
		for _, batch := range batches {
			batch.Release()
		}
	}
	m.batches = nil
	m.closed = true
	return nil
}
