//go:build linux && amd64

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"runtime"
	"syscall"
	"unsafe"
)

const (
	sysRecvmmsg           = 299
	soReusePort           = 15
	soAttachReusePortCBPF = 51

	bpfLoadWordAbsolute = 0x20
	bpfStoreA           = 0x02
	bpfLoadXMemory      = 0x61
	bpfMultiplyK        = 0x24
	bpfRightShiftK      = 0x74
	bpfXorX             = 0xac
	bpfModuloK          = 0x94
	bpfReturnA          = 0x16
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
	ownsConn bool
	buffers  [][]byte
	names    []rawSockaddrInet4
	iovecs   []syscall.Iovec
	messages []mmsghdr
}

func newUDPReceiver(conn *net.UDPConn, rawConn syscall.RawConn, batchSize int, ownsConn bool) *udpReceiver {
	if batchSize < 1 {
		batchSize = 1
	}

	receiver := &udpReceiver{
		conn:     conn,
		raw:      rawConn,
		ownsConn: ownsConn,
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

	return receiver
}

func openUDPReceiver(listenAddress string, reusePort bool, readBufferBytes int, batchSize int) (*udpReceiver, error) {
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

	return newUDPReceiver(conn, rawConn, batchSize, true), nil
}

func cloneUDPReceiver(receiver *udpReceiver, batchSize int) *udpReceiver {
	return newUDPReceiver(receiver.conn, receiver.raw, batchSize, false)
}

func buildReusePortCBPF(socketCount int, hashOffsets []int) ([]syscall.SockFilter, error) {
	if socketCount <= 1 {
		return nil, fmt.Errorf("reuseport_bpf requires at least two sockets")
	}
	if len(hashOffsets) == 0 {
		return nil, fmt.Errorf("reuseport_bpf requires at least one payload hash offset")
	}

	// Reuseport CBPF sees byte zero as the first byte of the UDP payload.
	filters := make([]syscall.SockFilter, 0, len(hashOffsets)*5+5)
	for index, offset := range hashOffsets {
		if offset < 0 || offset > maxUDPPacketSize-4 {
			return nil, fmt.Errorf("reuseport_bpf payload offset %d is outside the UDP payload", offset)
		}

		filters = append(filters,
			syscall.SockFilter{Code: bpfLoadWordAbsolute, K: uint32(offset)},
			syscall.SockFilter{Code: bpfMultiplyK, K: reusePortHashPrimes[index%len(reusePortHashPrimes)]},
		)
		if index > 0 {
			filters = append(filters,
				syscall.SockFilter{Code: bpfLoadXMemory, K: 0},
				syscall.SockFilter{Code: bpfXorX},
			)
		}
		filters = append(filters, syscall.SockFilter{Code: bpfStoreA, K: 0})
	}

	filters = append(filters,
		syscall.SockFilter{Code: bpfRightShiftK, K: 16},
		syscall.SockFilter{Code: bpfLoadXMemory, K: 0},
		syscall.SockFilter{Code: bpfXorX},
		syscall.SockFilter{Code: bpfModuloK, K: uint32(socketCount)},
		syscall.SockFilter{Code: bpfReturnA},
	)
	return filters, nil
}

func attachReusePortPayloadSelector(receiver *udpReceiver, socketCount int, hashOffsets []int) error {
	filters, err := buildReusePortCBPF(socketCount, hashOffsets)
	if err != nil {
		return err
	}

	program := syscall.SockFprog{
		Len:    uint16(len(filters)),
		Filter: &filters[0],
	}
	var syscallErr error
	if err := receiver.raw.Control(func(fd uintptr) {
		_, _, errno := syscall.Syscall6(
			syscall.SYS_SETSOCKOPT,
			fd,
			uintptr(syscall.SOL_SOCKET),
			uintptr(soAttachReusePortCBPF),
			uintptr(unsafe.Pointer(&program)),
			unsafe.Sizeof(program),
			0,
		)
		if errno != 0 {
			syscallErr = errno
		}
	}); err != nil {
		return fmt.Errorf("reuseport_bpf_control_error: %w", err)
	}
	runtime.KeepAlive(filters)
	runtime.KeepAlive(program)
	if syscallErr != nil {
		return fmt.Errorf("reuseport_bpf_attach_error: %w", syscallErr)
	}
	return nil
}

func (receiver *udpReceiver) Close() error {
	if !receiver.ownsConn {
		return nil
	}
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
