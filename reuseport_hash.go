package main

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

const maxReusePortHashOffsets = 8

var reusePortHashPrimes = [...]uint32{
	0x9e3779b1,
	0x85ebca6b,
	0xc2b2ae35,
	0x27d4eb2f,
	0x165667b1,
	0xd3a2646c,
	0xfd7046c5,
	0xb55a4f09,
}

func parseReusePortHashOffsets(value string) ([]int, error) {
	parts := strings.Split(value, ",")
	if len(parts) == 0 || len(parts) > maxReusePortHashOffsets {
		return nil, fmt.Errorf("expected between 1 and %d payload offsets", maxReusePortHashOffsets)
	}

	offsets := make([]int, 0, len(parts))
	seen := make(map[int]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("payload offsets cannot contain empty values")
		}

		offset, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid payload offset %q: %w", part, err)
		}
		if offset < 0 || offset > maxUDPPacketSize-4 {
			return nil, fmt.Errorf("payload offset %d must be between 0 and %d", offset, maxUDPPacketSize-4)
		}
		if _, exists := seen[offset]; exists {
			return nil, fmt.Errorf("payload offset %d is duplicated", offset)
		}

		seen[offset] = struct{}{}
		offsets = append(offsets, offset)
	}

	return offsets, nil
}

func formatReusePortHashOffsets(offsets []int) string {
	values := make([]string, len(offsets))
	for index, offset := range offsets {
		values[index] = strconv.Itoa(offset)
	}
	return strings.Join(values, ",")
}

// reusePortPayloadHash mirrors the CBPF selector and is used by tests to
// validate that representative Huawei payloads spread across all sockets.
func reusePortPayloadHash(payload []byte, offsets []int, socketCount int) (int, error) {
	if socketCount <= 0 {
		return 0, fmt.Errorf("socket count must be greater than zero")
	}
	if len(offsets) == 0 {
		return 0, fmt.Errorf("at least one payload offset is required")
	}

	var hash uint32
	for index, offset := range offsets {
		if offset < 0 || offset+4 > len(payload) {
			return 0, fmt.Errorf("payload offset %d exceeds packet size %d", offset, len(payload))
		}
		word := binary.BigEndian.Uint32(payload[offset : offset+4])
		mixed := word * reusePortHashPrimes[index%len(reusePortHashPrimes)]
		if index == 0 {
			hash = mixed
		} else {
			hash ^= mixed
		}
	}

	hash ^= hash >> 16
	return int(hash % uint32(socketCount)), nil
}
