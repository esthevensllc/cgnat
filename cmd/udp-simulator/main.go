package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	target      string
	mode        string
	header      []byte
	records     int
	protocol    byte
	duration    time.Duration
	pps         int64
	startPPS    int64
	maxPPS      int64
	stepPPS     int64
	stepEvery   time.Duration
	workers     int
	reportEvery time.Duration
}

type counters struct {
	sequence atomic.Uint64
	attempts atomic.Uint64
	sent     atomic.Uint64
	errors   atomic.Uint64
}

func main() {
	cfg := parseFlags()

	targetAddress, err := net.ResolveUDPAddr("udp", cfg.target)
	if err != nil {
		log.Fatalf("resolve_target_error target=%s error=%v", cfg.target, err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if cfg.duration > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(ctx, cfg.duration)
		defer timeoutCancel()
	}

	var currentPPS atomic.Int64
	initialPPS := cfg.pps
	if cfg.startPPS > 0 {
		initialPPS = cfg.startPPS
	}
	currentPPS.Store(initialPPS)

	counts := &counters{}
	var wg sync.WaitGroup

	log.Printf(
		"simulator_started target=%s mode=%s records_per_packet=%d workers=%d initial_pps=%d max_pps=%d step_pps=%d step_every=%s duration=%s",
		cfg.target,
		cfg.mode,
		recordsPerPacket(cfg),
		cfg.workers,
		initialPPS,
		cfg.maxPPS,
		cfg.stepPPS,
		cfg.stepEvery,
		cfg.duration,
	)

	if cfg.stepPPS > 0 && cfg.maxPPS > initialPPS {
		wg.Add(1)
		go rampPPS(ctx, cfg, &currentPPS, &wg)
	}

	wg.Add(1)
	go reportLoop(ctx, cfg, &currentPPS, counts, &wg)

	for workerID := 1; workerID <= cfg.workers; workerID++ {
		wg.Add(1)
		go sender(ctx, workerID, cfg, targetAddress, &currentPPS, counts, &wg)
	}

	wg.Wait()

	log.Printf(
		"simulator_stopped attempts=%d sent=%d errors=%d records_sent_estimated=%d",
		counts.attempts.Load(),
		counts.sent.Load(),
		counts.errors.Load(),
		counts.sent.Load()*uint64(recordsPerPacket(cfg)),
	)
}

func parseFlags() config {
	var cfg config
	headerHex := ""

	flag.StringVar(&cfg.target, "target", "127.0.0.1:9088", "collector UDP address, for example 10.96.167.132:9088")
	flag.StringVar(&cfg.mode, "mode", "legacy", "packet mode: legacy or multi")
	flag.StringVar(&headerHex, "header", "aabbccdd", "4-byte header in hex")
	flag.IntVar(&cfg.records, "records", 8, "records per UDP packet when mode=multi")
	flag.Int64Var(&cfg.pps, "pps", 1000, "fixed packets per second when start-pps is not set")
	flag.Int64Var(&cfg.startPPS, "start-pps", 0, "initial packets per second for ramp test")
	flag.Int64Var(&cfg.maxPPS, "max-pps", 0, "maximum packets per second for ramp test")
	flag.Int64Var(&cfg.stepPPS, "step-pps", 0, "increase packets per second by this amount every step-every")
	flag.DurationVar(&cfg.stepEvery, "step-every", 30*time.Second, "ramp interval")
	flag.DurationVar(&cfg.duration, "duration", 60*time.Second, "test duration, 0 means until Ctrl+C")
	flag.IntVar(&cfg.workers, "workers", runtime.NumCPU(), "parallel UDP sender workers; each worker uses a different UDP socket")
	flag.DurationVar(&cfg.reportEvery, "report-every", time.Second, "progress report interval")
	protocol := flag.String("protocol", "tcp", "legacy protocol: tcp, udp, icmp, or protocol number")

	flag.Parse()

	cfg.mode = strings.ToLower(strings.TrimSpace(cfg.mode))
	switch cfg.mode {
	case "legacy", "multi":
	default:
		log.Fatalf("invalid_mode mode=%s allowed=legacy,multi", cfg.mode)
	}

	header, err := hex.DecodeString(strings.TrimSpace(headerHex))
	if err != nil || len(header) != 4 {
		log.Fatalf("invalid_header header=%s expected_4_hex_bytes=true", headerHex)
	}
	cfg.header = header

	cfg.protocol = parseProtocol(*protocol)

	if cfg.records <= 0 {
		log.Fatal("records must be greater than zero")
	}
	if cfg.pps <= 0 && cfg.startPPS <= 0 {
		log.Fatal("pps or start-pps must be greater than zero")
	}
	if cfg.startPPS > 0 && cfg.maxPPS <= 0 {
		log.Fatal("max-pps must be greater than zero when start-pps is used")
	}
	if cfg.stepPPS < 0 {
		log.Fatal("step-pps cannot be negative")
	}
	if cfg.stepPPS > 0 && cfg.stepEvery <= 0 {
		log.Fatal("step-every must be greater than zero")
	}
	if cfg.workers <= 0 {
		log.Fatal("workers must be greater than zero")
	}
	if cfg.reportEvery <= 0 {
		log.Fatal("report-every must be greater than zero")
	}

	return cfg
}

func parseProtocol(value string) byte {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "icmp":
		return 1
	case "tcp":
		return 6
	case "udp":
		return 17
	default:
		var number int
		if _, err := fmt.Sscanf(value, "%d", &number); err != nil || number < 0 || number > 255 {
			log.Fatalf("invalid_protocol protocol=%s", value)
		}
		return byte(number)
	}
}

func recordsPerPacket(cfg config) int {
	if cfg.mode == "multi" {
		return cfg.records
	}
	return 1
}

func packetSize(cfg config) int {
	if cfg.mode == "multi" {
		return 16 + cfg.records*64
	}
	return 42
}

func rampPPS(ctx context.Context, cfg config, currentPPS *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(cfg.stepEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			next := currentPPS.Load() + cfg.stepPPS
			if next > cfg.maxPPS {
				next = cfg.maxPPS
			}
			currentPPS.Store(next)
			log.Printf("ramp_update target_pps=%d", next)
		case <-ctx.Done():
			return
		}
	}
}

func reportLoop(ctx context.Context, cfg config, currentPPS *atomic.Int64, counts *counters, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(cfg.reportEvery)
	defer ticker.Stop()

	startedAt := time.Now()
	previousSent := uint64(0)
	previousErrors := uint64(0)

	for {
		select {
		case <-ticker.C:
			sent := counts.sent.Load()
			errors := counts.errors.Load()
			sentDelta := sent - previousSent
			errorDelta := errors - previousErrors
			seconds := cfg.reportEvery.Seconds()

			log.Printf(
				"metrics elapsed=%s target_pps=%d sent=%d send_rate_pps=%.0f errors=%d error_rate=%.0f records_sent_estimated=%d",
				time.Since(startedAt).Truncate(time.Second),
				currentPPS.Load(),
				sent,
				float64(sentDelta)/seconds,
				errors,
				float64(errorDelta)/seconds,
				sent*uint64(recordsPerPacket(cfg)),
			)

			previousSent = sent
			previousErrors = errors
		case <-ctx.Done():
			return
		}
	}
}

func sender(
	ctx context.Context,
	workerID int,
	cfg config,
	targetAddress *net.UDPAddr,
	currentPPS *atomic.Int64,
	counts *counters,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	conn, err := net.DialUDP("udp", nil, targetAddress)
	if err != nil {
		log.Printf("worker_dial_error worker=%d error=%v", workerID, err)
		counts.errors.Add(1)
		return
	}
	defer conn.Close()

	tickInterval := 10 * time.Millisecond
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	packet := make([]byte, packetSize(cfg))
	accumulator := 0.0

	for {
		select {
		case <-ticker.C:
			workerPPS := float64(currentPPS.Load()) / float64(cfg.workers)
			accumulator += workerPPS * tickInterval.Seconds()
			sendCount := int(accumulator)
			if sendCount <= 0 {
				continue
			}
			accumulator -= float64(sendCount)

			now := uint32(time.Now().Unix())
			for index := 0; index < sendCount; index++ {
				sequence := counts.sequence.Add(1)
				buildPacket(packet, cfg, sequence, now)
				counts.attempts.Add(1)
				if _, err := conn.Write(packet); err != nil {
					counts.errors.Add(1)
					continue
				}
				counts.sent.Add(1)
			}

		case <-ctx.Done():
			return
		}
	}
}

func buildPacket(packet []byte, cfg config, sequence uint64, now uint32) {
	copy(packet[0:4], cfg.header)
	binary.BigEndian.PutUint32(packet[4:8], now)

	if cfg.mode == "multi" {
		for recordIndex := 0; recordIndex < cfg.records; recordIndex++ {
			recordStart := 16 + recordIndex*64
			fillNATRecord(packet[recordStart:recordStart+64], sequence+uint64(recordIndex), now)
		}
		return
	}

	for index := range packet[8:] {
		packet[8+index] = 0
	}
	packet[11] = cfg.protocol
	writeIPv4(packet[20:24], 10, byte(sequence>>16), byte(sequence>>8), byte(sequence))
	writeIPv4(packet[24:28], 200, 1, byte(sequence>>8), byte(sequence))
	writeIPv4(packet[28:32], 8, 8, 8, 8)
	binary.BigEndian.PutUint16(packet[36:38], uint16(1024+(sequence%50000)))
	binary.BigEndian.PutUint16(packet[38:40], uint16(20000+(sequence%40000)))
	binary.BigEndian.PutUint16(packet[40:42], 443)
}

func fillNATRecord(record []byte, sequence uint64, now uint32) {
	for index := range record {
		record[index] = 0
	}
	writeIPv4(record[4:8], 10, byte(sequence>>16), byte(sequence>>8), byte(sequence))
	writeIPv4(record[8:12], 200, 1, byte(sequence>>8), byte(sequence))
	writeIPv4(record[12:16], 8, 8, 8, 8)
	binary.BigEndian.PutUint16(record[20:22], uint16(1024+(sequence%50000)))
	binary.BigEndian.PutUint16(record[22:24], uint16(20000+(sequence%40000)))
	binary.BigEndian.PutUint16(record[24:26], 443)
	binary.BigEndian.PutUint32(record[28:32], now)
	binary.BigEndian.PutUint32(record[32:36], now)
}

func writeIPv4(target []byte, a byte, b byte, c byte, d byte) {
	target[0] = a
	target[1] = b
	target[2] = c
	target[3] = d
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetOutput(os.Stdout)
}
