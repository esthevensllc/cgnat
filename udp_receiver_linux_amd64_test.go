//go:build linux && amd64

package main

import "testing"

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
