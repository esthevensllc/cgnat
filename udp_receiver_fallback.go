//go:build !linux || !amd64

package main

import (
	"fmt"
	"log"
	"net"
)

type udpReceiver struct {
	conn     *net.UDPConn
	buffer   []byte
	ownsConn bool
}

func openUDPReceiver(listenAddress string, reusePort bool, readBufferBytes int, batchSize int) (*udpReceiver, error) {
	udpAddress, err := net.ResolveUDPAddr("udp", listenAddress)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		return nil, err
	}

	if err := conn.SetReadBuffer(readBufferBytes); err != nil {
		log.Printf("udp_set_read_buffer_warning requested_bytes=%d error=%v", readBufferBytes, err)
	}

	return &udpReceiver{
		conn:     conn,
		buffer:   make([]byte, maxUDPPacketSize),
		ownsConn: true,
	}, nil
}

func cloneUDPReceiver(receiver *udpReceiver, batchSize int) *udpReceiver {
	return &udpReceiver{
		conn:     receiver.conn,
		buffer:   make([]byte, maxUDPPacketSize),
		ownsConn: false,
	}
}

func attachReusePortPayloadSelector(receiver *udpReceiver, socketCount int, hashOffsets []int) error {
	return fmt.Errorf("reuseport_bpf is only supported on linux/amd64")
}

func (receiver *udpReceiver) Close() error {
	if !receiver.ownsConn {
		return nil
	}
	return receiver.conn.Close()
}

func (receiver *udpReceiver) ReadBatch(results []UDPReadResult) (int, error) {
	if len(results) == 0 {
		return 0, fmt.Errorf("udp_batch_results_empty")
	}

	bytesRead, remoteAddress, err := receiver.conn.ReadFromUDP(receiver.buffer)
	if err != nil {
		return 0, err
	}

	results[0] = UDPReadResult{
		Data:        receiver.buffer[:bytesRead],
		RouterIP:    remoteAddress.IP.String(),
		RouterIPNum: ipv4StringToUInt32(remoteAddress.IP.String()),
		RouterPort:  uint16(remoteAddress.Port),
	}
	return 1, nil
}
