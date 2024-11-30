// pkg/management/key.go
package management

import (
	"crypto/sha256"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/mdastpak/redis-management/config"
)

type KeyManager struct {
	cfg     *config.Config
	dbRange DBRange
	prefix  string
}

type DBRange struct {
	Start int
	End   int
}

func NewKeyManager(cfg *config.Config) (*KeyManager, error) {
	dbRange, err := parseDBRange(cfg.Redis.DB)
	if err != nil {
		return nil, err
	}

	return &KeyManager{
		cfg:     cfg,
		dbRange: dbRange,
		prefix:  strings.TrimRight(cfg.Redis.KeyPrefix, ":"),
	}, nil
}

func (km *KeyManager) GetKey(key string) string {
	finalKey := key
	if km.cfg.Redis.HashKeys {
		hash := sha256.Sum256([]byte(key))
		finalKey = fmt.Sprintf("%x", hash)
	}

	if km.prefix != "" {
		finalKey = fmt.Sprintf("%s:%s", km.prefix, finalKey)
	}

	return finalKey
}

func (km *KeyManager) GetShardIndex(key string) (int, error) {
	if km.dbRange.Start == km.dbRange.End {
		return km.dbRange.Start, nil
	}

	dbCount := km.dbRange.End - km.dbRange.Start + 1
	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()

	return int(hash%uint32(dbCount)) + km.dbRange.Start, nil
}

// ParseDBRange converts a string DB range (e.g., "0-5" or "3") to a DBRange struct
func parseDBRange(dbStr string) (DBRange, error) {
	var start, end int

	// Check if range format (e.g., "0-5")
	if strings.Contains(dbStr, "-") {
		parts := strings.Split(dbStr, "-")
		if len(parts) != 2 {
			return DBRange{}, fmt.Errorf("invalid DB range format: %s", dbStr)
		}

		var err error
		start, err = strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return DBRange{}, fmt.Errorf("invalid start DB number: %v", err)
		}

		end, err = strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return DBRange{}, fmt.Errorf("invalid end DB number: %v", err)
		}

		if start > end {
			return DBRange{}, fmt.Errorf("start DB (%d) cannot be greater than end DB (%d)", start, end)
		}
	} else {
		// Single DB number
		var err error
		start, err = strconv.Atoi(strings.TrimSpace(dbStr))
		if err != nil {
			return DBRange{}, fmt.Errorf("invalid DB number: %v", err)
		}
		end = start
	}

	if start < 0 || end < 0 {
		return DBRange{}, fmt.Errorf("DB numbers cannot be negative")
	}

	return DBRange{
		Start: start,
		End:   end,
	}, nil
}
