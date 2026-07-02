//go:build linux && amd64

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"syscall"
	"unsafe"
)

const (
	sysRecvmmsg = 299
	soReusePort = 15
)

type rawSockaddrInet4 struct {
	Family uint16
	Port   [2]byte
	Addr   [4]byte
	Zero   [8]byte
}

type mmsghdr struct {
	Hdr syscall.Msghdr
	Len uint32
	Pad [4]byte
}

type udpReceiver struct {
	conn     *net.UDPConn
	raw      syscall.RawConn
	buffers  [][]byte
	names    []rawSockaddrInet4
	iovecs   []syscall.Iovec
	messages []mmsghdr
}

func openUDPReceiver(listenAddress string, reusePort bool, readBufferBytes int, batchSize int) (*udpReceiver, error) {
	if batchSize < 1 {
		batchSize = 1
	}

	listenConfig := net.ListenConfig{
		Control: func(network string, address string, raw syscall.RawConn) error {
			var controlErr error
			if err := raw.Control(func(fd uintptr) {
				if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
					controlErr = fmt.Errorf("setsockopt_so_reuseaddr_error: %w", err)
					return
				}
				if reusePort {
					if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, soReusePort, 1); err != nil {
						controlErr = fmt.Errorf("setsockopt_so_reuseport_error: %w", err)
					}
				}
			}); err != nil {
				return err
			}
			return controlErr
		},
	}

	packetConn, err := listenConfig.ListenPacket(context.Background(), "udp4", listenAddress)
	if err != nil {
		return nil, err
	}

	conn, ok := packetConn.(*net.UDPConn)
	if !ok {
		_ = packetConn.Close()
		return nil, fmt.Errorf("listener_is_not_udp_conn")
	}

	if err := conn.SetReadBuffer(readBufferBytes); err != nil {
		log.Printf("udp_set_read_buffer_warning requested_bytes=%d error=%v", readBufferBytes, err)
	}

	rawConn, err := conn.SyscallConn()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	receiver := &udpReceiver{
		conn:     conn,
		raw:      rawConn,
		buffers:  make([][]byte, batchSize),
		names:    make([]rawSockaddrInet4, batchSize),
		iovecs:   make([]syscall.Iovec, batchSize),
		messages: make([]mmsghdr, batchSize),
	}

	for index := 0; index < batchSize; index++ {
		receiver.buffers[index] = make([]byte, maxUDPPacketSize)
		receiver.iovecs[index] = syscall.Iovec{
			Base: &receiver.buffers[index][0],
			Len:  uint64(len(receiver.buffers[index])),
		}
		receiver.messages[index].Hdr.Name = (*byte)(unsafe.Pointer(&receiver.names[index]))
		receiver.messages[index].Hdr.Namelen = uint32(unsafe.Sizeof(receiver.names[index]))
		receiver.messages[index].Hdr.Iov = &receiver.iovecs[index]
		receiver.messages[index].Hdr.Iovlen = 1
	}

	return receiver, nil
}

func (receiver *udpReceiver) Close() error {
	return receiver.conn.Close()
}

func (receiver *udpReceiver) ReadBatch(results []UDPReadResult) (int, error) {
	maxBatch := len(receiver.messages)
	if len(results) < maxBatch {
		maxBatch = len(results)
	}
	if maxBatch == 0 {
		return 0, fmt.Errorf("udp_batch_results_empty")
	}

	var count int
	var syscallErr error

	err := receiver.raw.Read(func(fd uintptr) bool {
		count, syscallErr = receiver.recvmmsg(fd, results[:maxBatch])
		if syscallErr == syscall.EAGAIN || syscallErr == syscall.EWOULDBLOCK || syscallErr == syscall.EINTR {
			return false
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if syscallErr != nil {
		return 0, syscallErr
	}

	return count, nil
}

func (receiver *udpReceiver) recvmmsg(fd uintptr, results []UDPReadResult) (int, error) {
	for index := range results {
		receiver.names[index] = rawSockaddrInet4{}
		receiver.messages[index].Len = 0
		receiver.messages[index].Hdr.Namelen = uint32(unsafe.Sizeof(receiver.names[index]))
		receiver.messages[index].Hdr.Flags = 0
	}

	n, _, errno := syscall.Syscall6(
		sysRecvmmsg,
		fd,
		uintptr(unsafe.Pointer(&receiver.messages[0])),
		uintptr(len(results)),
		0,
		0,
		0,
	)
	if errno != 0 {
		return int(n), errno
	}

	count := int(n)
	for index := 0; index < count; index++ {
		name := receiver.names[index]
		port := uint16(name.Port[0])<<8 | uint16(name.Port[1])
		ipNum := binary.BigEndian.Uint32(name.Addr[:])
		results[index] = UDPReadResult{
			Data:        receiver.buffers[index][:receiver.messages[index].Len],
			RouterIP:    net.IPv4(name.Addr[0], name.Addr[1], name.Addr[2], name.Addr[3]).String(),
			RouterIPNum: ipNum,
			RouterPort:  port,
		}
	}

	return count, nil
}
