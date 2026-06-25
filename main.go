package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	generalHeaderSize = 16
	recordSize        = 64
	maxUDPPacketSize  = 65535
)

var peruTZ = time.FixedZone("PET", -5*60*60)

// RawPacketLog conserva el formato RAW actual para que siga siendo compatible
// con reprocess_raw.go. No se almacenan los JSON parseados de NAT en disco.
type RawPacketLog struct {
	ReceivedTime string `json:"received_time"`
	RouterIP     string `json:"router_ip"`
	RouterPort   uint16 `json:"router_port"`
	PacketSize   uint16 `json:"packet_size"`
	RawHex       string `json:"raw_hex"`
}

type NATLogEntry struct {
	EventTime       string `json:"event_time"`
	StartTime       string `json:"start_time"`
	EndTime         string `json:"end_time"`
	EventID         string `json:"event_id"`
	EventType       string `json:"event_type"`
	Header          string `json:"header"`
	RouterIP        string `json:"router_ip"`
	RouterPort      uint16 `json:"router_port"`
	ProtocolID      uint8  `json:"protocol_id"`
	Protocol        string `json:"protocol"`
	PrivateIP       string `json:"private_ip"`
	PrivatePort     uint16 `json:"private_port"`
	PublicIP        string `json:"public_ip"`
	PublicPort      uint16 `json:"public_port"`
	DestinationIP   string `json:"destination_ip"`
	DestinationPort uint16 `json:"destination_port"`
	PacketSize      uint16 `json:"packet_size"`
}

type UDPPacket struct {
	Buffer           []byte
	Length           int
	RouterIP         string
	RouterPort       uint16
	ReceivedUnixNano int64
}

type InsertBatch struct {
	Body []byte
	Rows int
}

var (
	listenAddr      string
	clickhouseURL   string
	clickhouseUser  string
	clickhousePass  string
	clickhouseTable string
	insertURL       string

	rawSpoolBase string

	maxPacketsPerFile   int
	minPacketsPerFile   int
	rotateSeconds       int
	forceRotateSeconds  int
	syncEveryPackets    int
	rawWriterBufferMB   int
	rawWriters          int
	rawChannelSize      int
	packetWorkers       int
	packetChannelSize   int
	eventChannelSize    int
	insertWorkers       int
	batchBuilders       int
	insertBatchRows     int
	insertBatchBytes    int
	insertFlushMS       int
	insertBatchChanSize int
	insertMaxRetries    int
	insertRetryMS       int
	insertHTTPTimeoutS  int
	udpReadBufferMB     int

	totalReceived          uint64
	totalPacketProcessed   uint64
	totalPacketQueueDrops  uint64
	totalRawSpooled        uint64
	totalParsed            uint64
	totalInserted          uint64
	totalParseErrors       uint64
	totalRawMarshalErrors  uint64
	totalEventMarshalError uint64
	totalInsertErrors      uint64
	totalInsertDroppedRows uint64
	totalBatchesInserted   uint64
)

var packetBuffer512Pool = sync.Pool{
	New: func() any {
		return make([]byte, 512)
	},
}

var packetBuffer2048Pool = sync.Pool{
	New: func() any {
		return make([]byte, 2048)
	},
}

func acquirePacketBuffer(size int) []byte {
	switch {
	case size <= 512:
		return packetBuffer512Pool.Get().([]byte)[:size]
	case size <= 2048:
		return packetBuffer2048Pool.Get().([]byte)[:size]
	default:
		return make([]byte, size)
	}
}

func releasePacketBuffer(buffer []byte) {
	switch cap(buffer) {
	case 512:
		packetBuffer512Pool.Put(buffer[:512])
	case 2048:
		packetBuffer2048Pool.Put(buffer[:2048])
	}
}

func getEnv(key, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	return value
}

func getEnvInt(key string, defaultValue int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}

	number, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("invalid_env_integer key=%s value=%q default=%d", key, value, defaultValue)
		return defaultValue
	}

	return number
}

func protocolName(protocolID uint8) string {
	switch protocolID {
	case 1:
		return "ICMP"
	case 6:
		return "TCP"
	case 17:
		return "UDP"
	default:
		return "UNKNOWN"
	}
}

func peruTimeFromUnix(timestamp uint32) string {
	return time.Unix(int64(timestamp), 0).
		In(peruTZ).
		Format("2006-01-02 15:04:05")
}

func peruTimeFromUnixNano(timestamp int64) string {
	return time.Unix(0, timestamp).
		In(peruTZ).
		Format("2006-01-02 15:04:05")
}

func timestampWithFallback(primary uint32, packetTimestamp uint32, receivedUnixNano int64) uint32 {
	if primary != 0 {
		return primary
	}
	if packetTimestamp != 0 {
		return packetTimestamp
	}
	if receivedUnixNano > 0 {
		return uint32(time.Unix(0, receivedUnixNano).Unix())
	}
	return uint32(time.Now().Unix())
}

func makeEventID(entry NATLogEntry) string {
	value := fmt.Sprintf(
		"%s|%s|%s|%s|%d|%s|%d|%s|%d|%s|%d|%s|%d",
		entry.EventTime,
		entry.StartTime,
		entry.EndTime,
		entry.RouterIP,
		entry.ProtocolID,
		entry.PrivateIP,
		entry.PrivatePort,
		entry.PublicIP,
		entry.PublicPort,
		entry.DestinationIP,
		entry.DestinationPort,
		entry.Header,
		entry.PacketSize,
	)

	sum := sha1.Sum([]byte(value))
	return hex.EncodeToString(sum[:])
}

func parseMultiRecordPacket(
	data []byte,
	header string,
	routerIP string,
	routerPort uint16,
	receivedUnixNano int64,
) ([]NATLogEntry, error) {
	packetSize := len(data)
	recordsBytes := packetSize - generalHeaderSize

	if recordsBytes <= 0 || recordsBytes%recordSize != 0 {
		return nil, fmt.Errorf(
			"invalid_multi_packet_size packet_size=%d records_bytes=%d",
			packetSize,
			recordsBytes,
		)
	}

	packetTimestamp := uint32(0)
	if len(data) >= 8 {
		packetTimestamp = binary.BigEndian.Uint32(data[4:8])
	}

	totalRecords := recordsBytes / recordSize
	entries := make([]NATLogEntry, 0, totalRecords)

	for recordIndex := 0; recordIndex < totalRecords; recordIndex++ {
		recordStart := generalHeaderSize + recordIndex*recordSize
		recordEnd := recordStart + recordSize

		if recordEnd > packetSize {
			return nil, fmt.Errorf(
				"record_out_of_bounds index=%d start=%d end=%d packet_size=%d",
				recordIndex,
				recordStart,
				recordEnd,
				packetSize,
			)
		}

		record := data[recordStart:recordEnd]

		privateIP := net.IP(record[4:8]).String()
		publicIP := net.IP(record[8:12]).String()
		destinationIP := net.IP(record[12:16]).String()

		privatePort := binary.BigEndian.Uint16(record[20:22])
		publicPort := binary.BigEndian.Uint16(record[22:24])
		destinationPort := binary.BigEndian.Uint16(record[24:26])

		startTimestamp := timestampWithFallback(
			binary.BigEndian.Uint32(record[28:32]),
			packetTimestamp,
			receivedUnixNano,
		)
		endTimestamp := timestampWithFallback(
			binary.BigEndian.Uint32(record[32:36]),
			packetTimestamp,
			receivedUnixNano,
		)

		startTime := peruTimeFromUnix(startTimestamp)
		endTime := peruTimeFromUnix(endTimestamp)

		// El offset exacto del protocolo dentro del bloque de 64 bytes aún no
		// está formalmente confirmado; se conserva 0/UNKNOWN para no inventarlo.
		protocolID := uint8(0)

		entry := NATLogEntry{
			EventTime:       endTime,
			StartTime:       startTime,
			EndTime:         endTime,
			EventType:       "huawei_cgn_nat",
			Header:          header,
			RouterIP:        routerIP,
			RouterPort:      routerPort,
			ProtocolID:      protocolID,
			Protocol:        protocolName(protocolID),
			PrivateIP:       privateIP,
			PrivatePort:     privatePort,
			PublicIP:        publicIP,
			PublicPort:      publicPort,
			DestinationIP:   destinationIP,
			DestinationPort: destinationPort,
			PacketSize:      uint16(packetSize),
		}

		entry.EventID = makeEventID(entry)
		entries = append(entries, entry)
	}

	return entries, nil
}

func parseLegacyPacket(
	data []byte,
	header string,
	routerIP string,
	routerPort uint16,
	receivedUnixNano int64,
) ([]NATLogEntry, error) {
	if len(data) < 42 {
		return nil, fmt.Errorf("legacy_packet_too_short packet_size=%d minimum=42", len(data))
	}

	eventTimestamp := timestampWithFallback(
		binary.BigEndian.Uint32(data[4:8]),
		0,
		receivedUnixNano,
	)
	eventTime := peruTimeFromUnix(eventTimestamp)

	protocolID := data[11]

	entry := NATLogEntry{
		EventTime:       eventTime,
		StartTime:       eventTime,
		EndTime:         eventTime,
		EventType:       "huawei_cgn_nat",
		Header:          header,
		RouterIP:        routerIP,
		RouterPort:      routerPort,
		ProtocolID:      protocolID,
		Protocol:        protocolName(protocolID),
		PrivateIP:       net.IP(data[20:24]).String(),
		PrivatePort:     binary.BigEndian.Uint16(data[36:38]),
		PublicIP:        net.IP(data[24:28]).String(),
		PublicPort:      binary.BigEndian.Uint16(data[38:40]),
		DestinationIP:   net.IP(data[28:32]).String(),
		DestinationPort: binary.BigEndian.Uint16(data[40:42]),
		PacketSize:      uint16(len(data)),
	}

	entry.EventID = makeEventID(entry)
	return []NATLogEntry{entry}, nil
}

func parsePacket(
	data []byte,
	routerIP string,
	routerPort uint16,
	receivedUnixNano int64,
) ([]NATLogEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("packet_too_short_for_header packet_size=%d", len(data))
	}

	// Se acepta cualquier valor de header; se conserva para trazabilidad.
	header := hex.EncodeToString(data[0:4])
	packetSize := len(data)

	if packetSize >= generalHeaderSize+recordSize &&
		(packetSize-generalHeaderSize)%recordSize == 0 {
		return parseMultiRecordPacket(
			data,
			header,
			routerIP,
			routerPort,
			receivedUnixNano,
		)
	}

	return parseLegacyPacket(
		data,
		header,
		routerIP,
		routerPort,
		receivedUnixNano,
	)
}

func ensureRawSpoolDirs(base string) error {
	for _, directory := range []string{"open", "done"} {
		path := filepath.Join(base, directory)
		if err := os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("create_directory path=%s error=%w", path, err)
		}
	}
	return nil
}

func recoverRawSpool() {
	files, err := filepath.Glob(filepath.Join(rawSpoolBase, "open", "*.open"))
	if err != nil {
		log.Printf("recover_raw_glob_error error=%v", err)
		return
	}

	for _, sourcePath := range files {
		targetName := strings.TrimSuffix(filepath.Base(sourcePath), ".open") + ".done"
		targetPath := filepath.Join(rawSpoolBase, "done", targetName)

		if err := os.Rename(sourcePath, targetPath); err != nil {
			log.Printf(
				"recover_raw_move_error source=%s target=%s error=%v",
				sourcePath,
				targetPath,
				err,
			)
			continue
		}

		log.Printf("recover_raw_move_ok source=%s target=%s", sourcePath, targetPath)
	}
}

func newRawOpenFile(writerID int) (*os.File, *bufio.Writer, string, error) {
	fileName := fmt.Sprintf(
		"raw_w%02d_%s_%d.open",
		writerID,
		time.Now().Format("20060102_150405"),
		time.Now().UnixNano(),
	)
	filePath := filepath.Join(rawSpoolBase, "open", fileName)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, "", err
	}

	writer := bufio.NewWriterSize(file, rawWriterBufferMB*1024*1024)
	return file, writer, filePath, nil
}

func rotateRawFile(
	writerID int,
	file *os.File,
	writer *bufio.Writer,
	openPath string,
	packetCount int,
) error {
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("raw_flush_error file=%s error=%w", openPath, err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("raw_sync_error file=%s error=%w", openPath, err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("raw_close_error file=%s error=%w", openPath, err)
	}

	targetName := strings.TrimSuffix(filepath.Base(openPath), ".open") + ".done"
	targetPath := filepath.Join(rawSpoolBase, "done", targetName)

	if err := os.Rename(openPath, targetPath); err != nil {
		return fmt.Errorf(
			"raw_rename_error source=%s target=%s error=%w",
			openPath,
			targetPath,
			err,
		)
	}

	log.Printf(
		"raw_rotate_ok writer=%d packets=%d file=%s",
		writerID,
		packetCount,
		targetPath,
	)
	return nil
}

func reportFatal(fatalErrors chan<- error, err error) {
	select {
	case fatalErrors <- err:
	default:
	}
}

func closeEmptyRawFile(writerID int, file *os.File, writer *bufio.Writer, openPath string) {
	if err := writer.Flush(); err != nil {
		log.Printf("raw_empty_flush_error writer=%d file=%s error=%v", writerID, openPath, err)
	}
	if err := file.Close(); err != nil {
		log.Printf("raw_empty_close_error writer=%d file=%s error=%v", writerID, openPath, err)
	}
	if err := os.Remove(openPath); err != nil && !os.IsNotExist(err) {
		log.Printf("raw_empty_remove_error writer=%d file=%s error=%v", writerID, openPath, err)
	}
}

func rawSpoolWriter(
	writerID int,
	packets <-chan UDPPacket,
	parsedPackets chan<- UDPPacket,
	fatalErrors chan<- error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	file, writer, openPath, err := newRawOpenFile(writerID)
	if err != nil {
		reportFatal(fatalErrors, fmt.Errorf("raw_open_error writer=%d error=%w", writerID, err))
		return
	}

	packetCount := 0
	createdAt := time.Now()

	for packet := range packets {
		data := packet.Buffer[:packet.Length]
		rawPacket := RawPacketLog{
			ReceivedTime: peruTimeFromUnixNano(packet.ReceivedUnixNano),
			RouterIP:     packet.RouterIP,
			RouterPort:   packet.RouterPort,
			PacketSize:   uint16(packet.Length),
			RawHex:       hex.EncodeToString(data),
		}

		rawJSON, err := json.Marshal(rawPacket)
		if err != nil {
			atomic.AddUint64(&totalRawMarshalErrors, 1)
			log.Printf(
				"raw_marshal_error writer=%d router_ip=%s packet_size=%d error=%v",
				writerID,
				packet.RouterIP,
				packet.Length,
				err,
			)
			releasePacketBuffer(packet.Buffer)
			continue
		}
		rawJSON = append(rawJSON, '\n')

		if _, err := writer.Write(rawJSON); err != nil {
			releasePacketBuffer(packet.Buffer)
			reportFatal(
				fatalErrors,
				fmt.Errorf("raw_write_error writer=%d file=%s error=%w", writerID, openPath, err),
			)
			return
		}

		packetCount++
		atomic.AddUint64(&totalRawSpooled, 1)

		if syncEveryPackets > 0 && packetCount%syncEveryPackets == 0 {
			if err := writer.Flush(); err != nil {
				releasePacketBuffer(packet.Buffer)
				reportFatal(
					fatalErrors,
					fmt.Errorf("raw_flush_error writer=%d file=%s error=%w", writerID, openPath, err),
				)
				return
			}
			if err := file.Sync(); err != nil {
				releasePacketBuffer(packet.Buffer)
				reportFatal(
					fatalErrors,
					fmt.Errorf("raw_sync_error writer=%d file=%s error=%w", writerID, openPath, err),
				)
				return
			}
		}

		fileAge := time.Since(createdAt)
		shouldRotate := packetCount >= maxPacketsPerFile ||
			(packetCount >= minPacketsPerFile && fileAge >= time.Duration(rotateSeconds)*time.Second) ||
			fileAge >= time.Duration(forceRotateSeconds)*time.Second

		if shouldRotate {
			if err := rotateRawFile(writerID, file, writer, openPath, packetCount); err != nil {
				releasePacketBuffer(packet.Buffer)
				reportFatal(
					fatalErrors,
					fmt.Errorf("raw_rotate_error writer=%d file=%s error=%w", writerID, openPath, err),
				)
				return
			}

			file, writer, openPath, err = newRawOpenFile(writerID)
			if err != nil {
				releasePacketBuffer(packet.Buffer)
				reportFatal(fatalErrors, fmt.Errorf("raw_reopen_error writer=%d error=%w", writerID, err))
				return
			}

			packetCount = 0
			createdAt = time.Now()
		}

		parsedPackets <- packet
	}

	if packetCount > 0 {
		if err := rotateRawFile(writerID, file, writer, openPath, packetCount); err != nil {
			reportFatal(
				fatalErrors,
				fmt.Errorf("raw_shutdown_rotate_error writer=%d file=%s error=%w", writerID, openPath, err),
			)
		}
		return
	}

	closeEmptyRawFile(writerID, file, writer, openPath)
}

func packetWorker(
	workerID int,
	packets <-chan UDPPacket,
	eventLines chan<- []byte,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for packet := range packets {
		data := packet.Buffer[:packet.Length]

		entries, err := parsePacket(
			data,
			packet.RouterIP,
			packet.RouterPort,
			packet.ReceivedUnixNano,
		)
		if err != nil {
			atomic.AddUint64(&totalParseErrors, 1)
			log.Printf(
				"parse_error worker=%d router_ip=%s packet_size=%d error=%v",
				workerID,
				packet.RouterIP,
				packet.Length,
				err,
			)
		} else {
			for _, entry := range entries {
				entryJSON, err := json.Marshal(entry)
				if err != nil {
					atomic.AddUint64(&totalEventMarshalError, 1)
					log.Printf(
						"event_marshal_error worker=%d event_id=%s error=%v",
						workerID,
						entry.EventID,
						err,
					)
					continue
				}

				entryJSON = append(entryJSON, '\n')
				eventLines <- entryJSON
				atomic.AddUint64(&totalParsed, 1)
			}
		}

		atomic.AddUint64(&totalPacketProcessed, 1)
		releasePacketBuffer(packet.Buffer)
	}
}

func batchBuilder(eventLines <-chan []byte, batches chan<- InsertBatch, wg *sync.WaitGroup) {
	defer wg.Done()

	flushTicker := time.NewTicker(time.Duration(insertFlushMS) * time.Millisecond)
	defer flushTicker.Stop()

	body := make([]byte, 0, insertBatchBytes)
	rows := 0

	flush := func() {
		if rows == 0 {
			return
		}

		batches <- InsertBatch{
			Body: body,
			Rows: rows,
		}

		body = make([]byte, 0, insertBatchBytes)
		rows = 0
	}

	for {
		select {
		case line, ok := <-eventLines:
			if !ok {
				flush()
				return
			}

			if rows > 0 &&
				(rows >= insertBatchRows || len(body)+len(line) > insertBatchBytes) {
				flush()
			}

			body = append(body, line...)
			rows++

			if rows >= insertBatchRows || len(body) >= insertBatchBytes {
				flush()
			}

		case <-flushTicker.C:
			flush()
		}
	}
}

func newHTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          insertWorkers * 4,
		MaxIdleConnsPerHost:   insertWorkers * 2,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: time.Duration(insertHTTPTimeoutS) * time.Second,
		DisableCompression:    true,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   time.Duration(insertHTTPTimeoutS) * time.Second,
	}
}

func insertBatchToClickHouse(client *http.Client, batch InsertBatch) error {
	request, err := http.NewRequest(
		http.MethodPost,
		insertURL,
		bytes.NewReader(batch.Body),
	)
	if err != nil {
		return err
	}

	request.Header.Set("Content-Type", "application/x-ndjson")
	request.Header.Set("Connection", "keep-alive")

	if clickhouseUser != "" {
		request.SetBasicAuth(clickhouseUser, clickhousePass)
	}

	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
		return fmt.Errorf(
			"clickhouse_status=%d body=%s",
			response.StatusCode,
			strings.TrimSpace(string(responseBody)),
		)
	}

	_, _ = io.Copy(io.Discard, response.Body)
	return nil
}

func insertWorker(workerID int, batches <-chan InsertBatch, wg *sync.WaitGroup) {
	defer wg.Done()

	client := newHTTPClient()

	for batch := range batches {
		var lastErr error

		for attempt := 1; attempt <= insertMaxRetries; attempt++ {
			lastErr = insertBatchToClickHouse(client, batch)
			if lastErr == nil {
				atomic.AddUint64(&totalInserted, uint64(batch.Rows))
				atomic.AddUint64(&totalBatchesInserted, 1)
				break
			}

			log.Printf(
				"clickhouse_insert_retry worker=%d attempt=%d/%d rows=%d bytes=%d error=%v",
				workerID,
				attempt,
				insertMaxRetries,
				batch.Rows,
				len(batch.Body),
				lastErr,
			)

			if attempt < insertMaxRetries {
				time.Sleep(time.Duration(insertRetryMS*attempt) * time.Millisecond)
			}
		}

		if lastErr != nil {
			atomic.AddUint64(&totalInsertErrors, 1)
			atomic.AddUint64(&totalInsertDroppedRows, uint64(batch.Rows))
			log.Printf(
				"clickhouse_insert_failed worker=%d rows=%d bytes=%d error=%v raw_reprocess_required=true",
				workerID,
				batch.Rows,
				len(batch.Body),
				lastErr,
			)
		}
	}
}

func metricsLogger(
	stop <-chan struct{},
	spoolPackets <-chan UDPPacket,
	packetChannel <-chan UDPPacket,
	eventLines <-chan []byte,
	batches <-chan InsertBatch,
) {
	var previousReceived uint64
	var previousPacketProcessed uint64
	var previousRawSpooled uint64
	var previousParsed uint64
	var previousInserted uint64

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			received := atomic.LoadUint64(&totalReceived)
			processed := atomic.LoadUint64(&totalPacketProcessed)
			rawSpooled := atomic.LoadUint64(&totalRawSpooled)
			parsed := atomic.LoadUint64(&totalParsed)
			inserted := atomic.LoadUint64(&totalInserted)

			log.Printf(
				"metrics total_received=%d total_packet_processed=%d total_raw_spooled=%d total_parsed=%d total_inserted=%d total_packet_queue_drops=%d total_parse_errors=%d total_raw_marshal_errors=%d total_event_marshal_errors=%d total_insert_errors=%d total_insert_dropped_rows=%d total_batches_inserted=%d rate_received_10s=%d rate_packet_processed_10s=%d rate_raw_spooled_10s=%d rate_parsed_10s=%d rate_inserted_10s=%d queue_raw_input=%d/%d queue_packet=%d/%d queue_event=%d/%d queue_batch=%d/%d",
				received,
				processed,
				rawSpooled,
				parsed,
				inserted,
				atomic.LoadUint64(&totalPacketQueueDrops),
				atomic.LoadUint64(&totalParseErrors),
				atomic.LoadUint64(&totalRawMarshalErrors),
				atomic.LoadUint64(&totalEventMarshalError),
				atomic.LoadUint64(&totalInsertErrors),
				atomic.LoadUint64(&totalInsertDroppedRows),
				atomic.LoadUint64(&totalBatchesInserted),
				received-previousReceived,
				processed-previousPacketProcessed,
				rawSpooled-previousRawSpooled,
				parsed-previousParsed,
				inserted-previousInserted,
				len(spoolPackets),
				cap(spoolPackets),
				len(packetChannel),
				cap(packetChannel),
				len(eventLines),
				cap(eventLines),
				len(batches),
				cap(batches),
			)

			previousReceived = received
			previousPacketProcessed = processed
			previousRawSpooled = rawSpooled
			previousParsed = parsed
			previousInserted = inserted
		case <-stop:
			return
		}
	}
}

func loadConfig() {
	listenAddr = getEnv("LISTEN_ADDR", "0.0.0.0:9088")

	clickhouseURL = getEnv("CLICKHOUSE_URL", "http://127.0.0.1:8123")
	clickhouseUser = getEnv("CLICKHOUSE_USER", "default")
	clickhousePass = getEnv("CLICKHOUSE_PASS", "")
	clickhouseTable = getEnv("CLICKHOUSE_TABLE", "cgnat.huawei_cgn_nat")

	rawSpoolBase = getEnv("RAW_SPOOL_BASE", "/index2/huawei-cgn-go/raw")

	maxPacketsPerFile = getEnvInt("MAX_EVENTS_PER_FILE", 250000)
	minPacketsPerFile = getEnvInt("MIN_EVENTS_PER_FILE", 100000)
	rotateSeconds = getEnvInt("ROTATE_SECONDS", 10)
	forceRotateSeconds = getEnvInt("FORCE_ROTATE_SECONDS", 30)
	syncEveryPackets = getEnvInt("SYNC_EVERY_EVENTS", 250000)
	rawWriterBufferMB = getEnvInt("RAW_WRITER_BUFFER_MB", 8)

	defaultPacketWorkers := runtime.NumCPU() - 2
	if defaultPacketWorkers < 2 {
		defaultPacketWorkers = 2
	}

	packetWorkers = getEnvInt("PACKET_WORKERS", defaultPacketWorkers)
	packetChannelSize = getEnvInt("PACKET_CHANNEL_SIZE", 500000)
	rawWriters = getEnvInt("RAW_WRITERS", 2)
	rawChannelSize = getEnvInt("RAW_CHANNEL_SIZE", 500000)
	eventChannelSize = getEnvInt("EVENT_CHANNEL_SIZE", 1000000)

	insertWorkers = getEnvInt("INSERT_WORKERS", 6)
	batchBuilders = getEnvInt("BATCH_BUILDERS", 2)
	insertBatchRows = getEnvInt("INSERT_BATCH_ROWS", 100000)
	insertBatchBytes = getEnvInt("INSERT_BATCH_BYTES", 16*1024*1024)
	insertFlushMS = getEnvInt("INSERT_FLUSH_MS", 500)
	insertBatchChanSize = getEnvInt("INSERT_BATCH_CHANNEL_SIZE", 16)
	insertMaxRetries = getEnvInt("INSERT_MAX_RETRIES", 3)
	insertRetryMS = getEnvInt("INSERT_RETRY_MS", 500)
	insertHTTPTimeoutS = getEnvInt("INSERT_HTTP_TIMEOUT_SECONDS", 180)

	udpReadBufferMB = getEnvInt("UDP_READ_BUFFER_MB", 512)

	query := fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", clickhouseTable)
	insertURL = strings.TrimRight(clickhouseURL, "/") + "/?query=" + url.QueryEscape(query)
}

func validateConfig() {
	positiveValues := map[string]int{
		"MAX_EVENTS_PER_FILE":         maxPacketsPerFile,
		"MIN_EVENTS_PER_FILE":         minPacketsPerFile,
		"ROTATE_SECONDS":              rotateSeconds,
		"FORCE_ROTATE_SECONDS":        forceRotateSeconds,
		"RAW_WRITER_BUFFER_MB":        rawWriterBufferMB,
		"PACKET_WORKERS":              packetWorkers,
		"PACKET_CHANNEL_SIZE":         packetChannelSize,
		"RAW_WRITERS":                 rawWriters,
		"RAW_CHANNEL_SIZE":            rawChannelSize,
		"EVENT_CHANNEL_SIZE":          eventChannelSize,
		"INSERT_WORKERS":              insertWorkers,
		"BATCH_BUILDERS":              batchBuilders,
		"INSERT_BATCH_ROWS":           insertBatchRows,
		"INSERT_BATCH_BYTES":          insertBatchBytes,
		"INSERT_FLUSH_MS":             insertFlushMS,
		"INSERT_BATCH_CHANNEL_SIZE":   insertBatchChanSize,
		"INSERT_MAX_RETRIES":          insertMaxRetries,
		"INSERT_RETRY_MS":             insertRetryMS,
		"INSERT_HTTP_TIMEOUT_SECONDS": insertHTTPTimeoutS,
		"UDP_READ_BUFFER_MB":          udpReadBufferMB,
	}

	for key, value := range positiveValues {
		if value <= 0 {
			log.Fatalf("%s must be greater than zero", key)
		}
	}

	if minPacketsPerFile > maxPacketsPerFile {
		log.Fatal("MIN_EVENTS_PER_FILE cannot exceed MAX_EVENTS_PER_FILE")
	}

	if syncEveryPackets < 0 {
		log.Fatal("SYNC_EVERY_EVENTS cannot be negative")
	}
}

func main() {
	loadConfig()
	validateConfig()

	if err := ensureRawSpoolDirs(rawSpoolBase); err != nil {
		log.Fatalf("raw_spool_directory_error error=%v", err)
	}
	recoverRawSpool()

	udpAddress, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		log.Fatalf("resolve_udp_error address=%s error=%v", listenAddr, err)
	}

	connection, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		log.Fatalf("listen_udp_error address=%s error=%v", listenAddr, err)
	}
	defer connection.Close()

	readBufferBytes := udpReadBufferMB * 1024 * 1024
	if err := connection.SetReadBuffer(readBufferBytes); err != nil {
		log.Printf(
			"udp_set_read_buffer_warning requested_bytes=%d error=%v",
			readBufferBytes,
			err,
		)
	}

	spoolPackets := make(chan UDPPacket, rawChannelSize)
	packetChannel := make(chan UDPPacket, packetChannelSize)
	eventLines := make(chan []byte, eventChannelSize)
	insertBatches := make(chan InsertBatch, insertBatchChanSize)
	fatalErrors := make(chan error, 1)
	stop := make(chan struct{})

	var stopOnce sync.Once
	requestStop := func(reason string) {
		stopOnce.Do(func() {
			log.Printf("collector_shutdown_requested reason=%s", reason)
			close(stop)
			if err := connection.Close(); err != nil {
				log.Printf("udp_close_error error=%v", err)
			}
		})
	}

	signalChannel := make(chan os.Signal, 2)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signalChannel)

	go func() {
		select {
		case sig := <-signalChannel:
			requestStop(sig.String())
		case err := <-fatalErrors:
			log.Printf("collector_fatal_error error=%v", err)
			requestStop("fatal_error")
		case <-stop:
		}
	}()

	var rawWG sync.WaitGroup
	var packetWG sync.WaitGroup
	var batchWG sync.WaitGroup
	var insertWG sync.WaitGroup

	for writerID := 1; writerID <= rawWriters; writerID++ {
		rawWG.Add(1)
		go rawSpoolWriter(writerID, spoolPackets, packetChannel, fatalErrors, &rawWG)
	}

	for workerID := 1; workerID <= packetWorkers; workerID++ {
		packetWG.Add(1)
		go packetWorker(workerID, packetChannel, eventLines, &packetWG)
	}

	for builderID := 1; builderID <= batchBuilders; builderID++ {
		batchWG.Add(1)
		go batchBuilder(eventLines, insertBatches, &batchWG)
	}

	for workerID := 1; workerID <= insertWorkers; workerID++ {
		insertWG.Add(1)
		go insertWorker(workerID, insertBatches, &insertWG)
	}

	go metricsLogger(stop, spoolPackets, packetChannel, eventLines, insertBatches)

	log.Printf(
		"collector_started listen_addr=%s clickhouse_url=%s clickhouse_table=%s raw_spool=%s packet_workers=%d packet_channel_size=%d raw_writers=%d raw_channel_size=%d event_channel_size=%d batch_builders=%d insert_workers=%d insert_batch_rows=%d insert_batch_bytes=%d udp_read_buffer_mb=%d parsed_json_spool=false accept_any_header=true dynamic_multi_record=true start_end_time=true raw_first_pipeline=true graceful_shutdown=true",
		listenAddr,
		clickhouseURL,
		clickhouseTable,
		rawSpoolBase,
		packetWorkers,
		packetChannelSize,
		rawWriters,
		rawChannelSize,
		eventChannelSize,
		batchBuilders,
		insertWorkers,
		insertBatchRows,
		insertBatchBytes,
		udpReadBufferMB,
	)

	readBuffer := make([]byte, maxUDPPacketSize)

readLoop:
	for {
		bytesRead, remoteAddress, err := connection.ReadFromUDP(readBuffer)
		if err != nil {
			select {
			case <-stop:
				break readLoop
			default:
			}
			log.Printf("udp_read_error error=%v", err)
			continue
		}

		atomic.AddUint64(&totalReceived, 1)

		packetBuffer := acquirePacketBuffer(bytesRead)
		copy(packetBuffer, readBuffer[:bytesRead])

		packet := UDPPacket{
			Buffer:           packetBuffer,
			Length:           bytesRead,
			RouterIP:         remoteAddress.IP.String(),
			RouterPort:       uint16(remoteAddress.Port),
			ReceivedUnixNano: time.Now().UnixNano(),
		}

		select {
		case spoolPackets <- packet:
		case <-stop:
			releasePacketBuffer(packetBuffer)
			break readLoop
		}
	}

	log.Printf("collector_draining_start")
	close(spoolPackets)
	rawWG.Wait()
	close(packetChannel)
	packetWG.Wait()
	close(eventLines)
	batchWG.Wait()
	close(insertBatches)
	insertWG.Wait()
	log.Printf("collector_stopped")
}
