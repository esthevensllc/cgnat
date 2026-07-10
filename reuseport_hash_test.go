package main

import (
	"encoding/binary"
	"testing"
)

func TestParseReusePortHashOffsets(t *testing.T) {
	offsets, err := parseReusePortHashOffsets("20, 36")
	if err != nil {
		t.Fatalf("parseReusePortHashOffsets returned error: %v", err)
	}
	if len(offsets) != 2 || offsets[0] != 20 || offsets[1] != 36 {
		t.Fatalf("unexpected offsets: %v", offsets)
	}
	if formatted := formatReusePortHashOffsets(offsets); formatted != "20,36" {
		t.Fatalf("unexpected formatted offsets: %s", formatted)
	}
}

func TestParseReusePortHashOffsetsRejectsInvalidValues(t *testing.T) {
	for _, value := range []string{"", "20,", "20,20", "-1", "65532", "word"} {
		if _, err := parseReusePortHashOffsets(value); err == nil {
			t.Fatalf("expected %q to be rejected", value)
		}
	}
}

func TestReusePortPayloadHashDistributesSimulatorPackets(t *testing.T) {
	const socketCount = 16
	counts := make([]int, socketCount)
	payload := make([]byte, 528)

	for sequence := uint32(1); sequence <= 100000; sequence++ {
		binary.BigEndian.PutUint32(payload[20:24], 0x0a000000|(sequence&0x00ffffff))
		privatePort := uint32(1024 + sequence%50000)
		publicPort := uint32(20000 + sequence%40000)
		binary.BigEndian.PutUint32(payload[36:40], privatePort<<16|publicPort)

		socketIndex, err := reusePortPayloadHash(payload, []int{20, 36}, socketCount)
		if err != nil {
			t.Fatalf("reusePortPayloadHash returned error: %v", err)
		}
		counts[socketIndex]++
	}

	minCount := counts[0]
	maxCount := counts[0]
	for index, count := range counts {
		if count == 0 {
			t.Fatalf("socket %d received no simulated packets; distribution=%v", index+1, counts)
		}
		if count < minCount {
			minCount = count
		}
		if count > maxCount {
			maxCount = count
		}
	}

	average := 100000 / socketCount
	if maxCount-minCount > average/10 {
		t.Fatalf("distribution is too uneven: %v", counts)
	}
}
