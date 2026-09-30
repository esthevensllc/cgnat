package main

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseOnlyProcessesPacketsWithoutInsertOrSpool(t *testing.T) {
	previousParseOnly := parseOnly
	previousFlushMS := insertFlushMS
	previousSpoolBase := failedSpoolBase
	previousParsed := atomic.LoadUint64(&totalParsed)
	previousProcessed := atomic.LoadUint64(&totalPacketProcessed)
	previousParseErrors := atomic.LoadUint64(&totalParseErrors)
	previousAlertCohorts := alertCohorts
	t.Cleanup(func() {
		parseOnly = previousParseOnly
		insertFlushMS = previousFlushMS
		failedSpoolBase = previousSpoolBase
		atomic.StoreUint64(&totalParsed, previousParsed)
		atomic.StoreUint64(&totalPacketProcessed, previousProcessed)
		atomic.StoreUint64(&totalParseErrors, previousParseErrors)
		alertCohorts = previousAlertCohorts
	})

	parseOnly = true
	insertFlushMS = 10
	failedSpoolBase = t.TempDir()
	alertCohorts = nil

	valid := acquirePacketBuffer(42)
	copy(valid[:4], []byte{0xaa, 0xbb, 0xcc, 0xdd})
	invalid := acquirePacketBuffer(3)
	packets := make(chan UDPPacket, 2)
	packets <- UDPPacket{Buffer: valid, Length: len(valid), ReceivedUnixNano: time.Now().UnixNano()}
	packets <- UDPPacket{Buffer: invalid, Length: len(invalid), ReceivedUnixNano: time.Now().UnixNano()}
	close(packets)
	batches := make(chan InsertBatch, 2)

	var wg sync.WaitGroup
	wg.Add(1)
	packetWorker(1, packets, batches, &wg)
	wg.Wait()

	if got := atomic.LoadUint64(&totalParsed) - previousParsed; got != 1 {
		t.Fatalf("parsed rows = %d, want 1", got)
	}
	if got := atomic.LoadUint64(&totalPacketProcessed) - previousProcessed; got != 2 {
		t.Fatalf("processed packets = %d, want 2", got)
	}
	if got := atomic.LoadUint64(&totalParseErrors) - previousParseErrors; got != 1 {
		t.Fatalf("parse errors = %d, want 1", got)
	}
	if len(batches) != 0 {
		t.Fatalf("queued insert batches = %d, want 0", len(batches))
	}
	entries, err := os.ReadDir(failedSpoolBase)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed spool entries = %v, error = %v", entries, err)
	}
}
