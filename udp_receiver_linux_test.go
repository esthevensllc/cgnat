//go:build linux && (amd64 || arm64)

package main

import (
	"net"
	"testing"
	"time"
	"unsafe"
)

func TestRecvmmsgLayout(t *testing.T) {
	if size := unsafe.Sizeof(mmsghdr{}); size != 64 {
		t.Fatalf("mmsghdr size = %d, want 64", size)
	}
}

func TestUDPReceiverReadsPacket(t *testing.T) {
	receiver, err := openUDPReceiver("127.0.0.1:0", false, 1<<20, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if err := receiver.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}

	sender, err := net.DialUDP("udp4", nil, receiver.conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if _, err := sender.Write([]byte("arm64-udp-test")); err != nil {
		t.Fatal(err)
	}

	results := make([]UDPReadResult, 4)
	count, err := receiver.ReadBatch(results)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("packet count = %d, want 1", count)
	}
	if got := string(results[0].Data); got != "arm64-udp-test" {
		t.Fatalf("payload = %q", got)
	}
	if results[0].RouterIP != "127.0.0.1" || results[0].RouterPort != uint16(sender.LocalAddr().(*net.UDPAddr).Port) {
		t.Fatalf("sender = %s:%d", results[0].RouterIP, results[0].RouterPort)
	}
}

func TestAttachReusePortPayloadSelector(t *testing.T) {
	first, err := openUDPReceiver("127.0.0.1:0", true, 1<<20, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := openUDPReceiver(first.conn.LocalAddr().String(), true, 1<<20, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := attachReusePortPayloadSelector(first, 2, []int{20, 36}); err != nil {
		t.Fatal(err)
	}
}

func TestObserveSocketDropsHandlesDuplicatesAndStaleMessages(t *testing.T) {
	tracker := &udpSocketDropTracker{}
	receiver := &udpReceiver{dropTracker: tracker}

	if delta := receiver.ObserveSocketDrops(10); delta != 10 {
		t.Fatalf("initial delta = %d, want 10", delta)
	}
	if delta := receiver.ObserveSocketDrops(10); delta != 0 {
		t.Fatalf("duplicate delta = %d, want 0", delta)
	}
	if delta := receiver.ObserveSocketDrops(15); delta != 5 {
		t.Fatalf("next delta = %d, want 5", delta)
	}
	if delta := receiver.ObserveSocketDrops(12); delta != 0 {
		t.Fatalf("stale delta = %d, want 0", delta)
	}
}

func TestObserveSocketDropsHandlesUint32Wrap(t *testing.T) {
	tracker := &udpSocketDropTracker{state: uint64(1)<<32 | uint64(^uint32(0)-2)}
	receiver := &udpReceiver{dropTracker: tracker}

	if delta := receiver.ObserveSocketDrops(2); delta != 5 {
		t.Fatalf("wrapped delta = %d, want 5", delta)
	}
}
