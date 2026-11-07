package distributed

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"

	"github.com/gabrielelanaro/queng/datasource"
)

// Partitioning determines where exchange rows go and how many consumer tasks run.
type Partitioning interface {
	PartitionCount(shufflePartitions int) int
	partitionForRow(batch datasource.RecordBatch, row, partitionCount int) (int, error)
}

// HashPartitioning sends equal keys to the same partition.
type HashPartitioning struct {
	Keys []int
}

func (HashPartitioning) PartitionCount(shufflePartitions int) int { return shufflePartitions }

func (p HashPartitioning) partitionForRow(batch datasource.RecordBatch, row, partitionCount int) (int, error) {
	var hash uint64
	for _, index := range p.Keys {
		if index < 0 || index >= batch.ColumnCount() {
			return 0, fmt.Errorf("exchange key index %d out of range", index)
		}
		keyHash, err := hashKey(batch.Column(index).Value(row))
		if err != nil {
			return 0, err
		}
		hash = 31*hash + keyHash
	}
	return int(hash % uint64(partitionCount)), nil
}

// SinglePartition gathers all rows into partition zero.
type SinglePartition struct{}

func (SinglePartition) PartitionCount(int) int { return 1 }

func (SinglePartition) partitionForRow(datasource.RecordBatch, int, int) (int, error) {
	return 0, nil
}

// hashKey uses a stable hash so equal keys route identically on every worker.
func hashKey(value any) (uint64, error) {
	h := fnv.New64a()
	var bits [8]byte
	switch v := value.(type) {
	case nil:
		h.Write([]byte{0})
	case int64:
		h.Write([]byte{1})
		binary.LittleEndian.PutUint64(bits[:], uint64(v))
		h.Write(bits[:])
	case float64:
		h.Write([]byte{2})
		if v == 0 {
			v = 0 // Both signs of zero compare equal.
		}
		binary.LittleEndian.PutUint64(bits[:], math.Float64bits(v))
		h.Write(bits[:])
	case string:
		h.Write([]byte{3})
		h.Write([]byte(v))
	case bool:
		h.Write([]byte{4})
		if v {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	default:
		return 0, fmt.Errorf("unsupported exchange key type: %T", value)
	}
	return h.Sum64(), nil
}
