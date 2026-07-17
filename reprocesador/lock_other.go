//go:build !linux

package main

func acquireProcessLock(_ string) (func(), error) {
	return func() {}, nil
}
