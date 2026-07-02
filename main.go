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
	rawFormatBinary   = "binary"
	rawFormatJSONL    = "jsonl"
	rawBinaryMagic    = "HCGNRAW2\n"
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
	EventTime       uint32
	StartTime       uint32
	EndTime         uint32
	EventID         string
	EventType       string
	Header          string
	RouterIP        uint32
	RouterPort      uint16
	ProtocolID      uint8
	Protocol        string
	PrivateIP       uint32
	PrivatePort     uint16
	PublicIP        uint32
	PublicPort      uint16
	DestinationIP   uint32
	DestinationPort uint16
	PacketSize      uint16
}

type UDPPacket struct {
	Buffer           []byte
	Length           int
	RouterIP         string
	RouterIPNum      uint32
	RouterPort       uint16
	ReceivedUnixNano int64
}

type UDPReadResult struct {
	Data        []byte
	RouterIP    string
	RouterIPNum uint32
	RouterPort  uint16
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
	rawFormat    string

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
	udpReceivers        int
	udpBatchSize        int
	udpReusePort        bool

	autoTuneEnabled              bool
	autoTuneIntervalSeconds      int
	autoTuneHighWatermarkPct     int
	autoTuneCriticalWatermarkPct int
	autoTuneMaxRawWriters        int
	autoTuneMaxPacketWorkers     int
	autoTuneMaxBatchBuilders     int
	autoTuneMaxInsertWorkers     int

	liveInsertOverloadPolicy        string
	liveInsertQueueHighWatermarkPct int
	liveInsertSendTimeoutMS         int

	totalReceived          uint64
	totalPacketProcessed   uint64
	totalPacketQueueDrops  uint64
	totalLiveInsertSkipped uint64
	totalRawSpooled        uint64
	totalParsed            uint64
	totalInserted          uint64
	totalParseErrors       uint64
	totalRawMarshalErrors  uint64
	totalEventMarshalError uint64
	totalInsertErrors      uint64
	totalInsertDroppedRows uint64
	totalLiveSkippedRows   uint64
	totalBatchesInserted   uint64

	currentRawWriters    int64
	currentPacketWorkers int64
	currentBatchBuilders int64
	currentInsertWorkers int64
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

func getEnvBool(key string, defaultValue bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return defaultValue
	}

	switch value {
	case "1", "true", "yes", "y", "on", "si", "s":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		log.Printf("invalid_env_bool key=%s value=%q default=%t", key, value, defaultValue)
		return defaultValue
	}
}

func queueUsagePct(length int, capacity int) int {
	if capacity <= 0 {
		return 0
	}
	return (length * 100) / capacity
}

func channelUsagePct[T any](channel <-chan T) int {
	return queueUsagePct(len(channel), cap(channel))
}

func channelsLenCap[T any](channels []chan T) (int, int) {
	totalLen := 0
	totalCap := 0
	for _, channel := range channels {
		totalLen += len(channel)
		totalCap += cap(channel)
	}
	return totalLen, totalCap
}

func channelsMaxUsagePct[T any](channels []chan T) int {
	maxUsage := 0
	for _, channel := range channels {
		usage := channelUsagePct(channel)
		if usage > maxUsage {
			maxUsage = usage
		}
	}
	return maxUsage
}

func scaleStep(current int, max int, critical bool) int {
	if current >= max {
		return 0
	}

	step := current / 2
	if critical {
		step = current
	}
	if step < 1 {
		step = 1
	}
	remaining := max - current
	if step > remaining {
		step = remaining
	}
	return step
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

func ipv4BytesToUInt32(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

func ipv4StringToUInt32(value string) uint32 {
	ip := net.ParseIP(value).To4()
	if ip == nil {
		return 0
	}
	return binary.BigEndian.Uint32(ip)
}

func appendUInt8(buffer []byte, value uint8) []byte {
	return append(buffer, value)
}

func appendUInt16(buffer []byte, value uint16) []byte {
	var raw [2]byte
	binary.LittleEndian.PutUint16(raw[:], value)
	return append(buffer, raw[:]...)
}

func appendUInt32(buffer []byte, value uint32) []byte {
	var raw [4]byte
	binary.LittleEndian.PutUint32(raw[:], value)
	return append(buffer, raw[:]...)
}

func appendULEB128(buffer []byte, value uint64) []byte {
	for value >= 0x80 {
		buffer = append(buffer, byte(value)|0x80)
		value >>= 7
	}
	return append(buffer, byte(value))
}

func appendRowBinaryString(buffer []byte, value string) []byte {
	buffer = appendULEB128(buffer, uint64(len(value)))
	return append(buffer, value...)
}

func appendRowBinaryFixedString(buffer []byte, value string, size int) ([]byte, error) {
	if len(value) != size {
		return buffer, fmt.Errorf("fixed_string_size_mismatch size=%d actual=%d value=%q", size, len(value), value)
	}
	return append(buffer, value...), nil
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
	buffer := make([]byte, 0, 64)
	buffer = appendUInt32(buffer, entry.EventTime)
	buffer = appendUInt32(buffer, entry.StartTime)
	buffer = appendUInt32(buffer, entry.EndTime)
	buffer = appendUInt32(buffer, entry.RouterIP)
	buffer = appendUInt8(buffer, entry.ProtocolID)
	buffer = appendUInt32(buffer, entry.PrivateIP)
	buffer = appendUInt16(buffer, entry.PrivatePort)
	buffer = appendUInt32(buffer, entry.PublicIP)
	buffer = appendUInt16(buffer, entry.PublicPort)
	buffer = appendUInt32(buffer, entry.DestinationIP)
	buffer = appendUInt16(buffer, entry.DestinationPort)
	buffer = append(buffer, entry.Header...)
	buffer = appendUInt16(buffer, entry.PacketSize)

	sum := sha1.Sum(buffer)
	return hex.EncodeToString(sum[:])
}

func parseMultiRecordPacket(
	data []byte,
	header string,
	routerIP uint32,
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

		privateIP := ipv4BytesToUInt32(record[4:8])
		publicIP := ipv4BytesToUInt32(record[8:12])
		destinationIP := ipv4BytesToUInt32(record[12:16])

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

		// El offset exacto del protocolo dentro del bloque de 64 bytes aún no
		// está formalmente confirmado; se conserva 0/UNKNOWN para no inventarlo.
		protocolID := uint8(0)

		entry := NATLogEntry{
			EventTime:       endTimestamp,
			StartTime:       startTimestamp,
			EndTime:         endTimestamp,
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
	routerIP uint32,
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

	protocolID := data[11]

	entry := NATLogEntry{
		EventTime:       eventTimestamp,
		StartTime:       eventTimestamp,
		EndTime:         eventTimestamp,
		EventType:       "huawei_cgn_nat",
		Header:          header,
		RouterIP:        routerIP,
		RouterPort:      routerPort,
		ProtocolID:      protocolID,
		Protocol:        protocolName(protocolID),
		PrivateIP:       ipv4BytesToUInt32(data[20:24]),
		PrivatePort:     binary.BigEndian.Uint16(data[36:38]),
		PublicIP:        ipv4BytesToUInt32(data[24:28]),
		PublicPort:      binary.BigEndian.Uint16(data[38:40]),
		DestinationIP:   ipv4BytesToUInt32(data[28:32]),
		DestinationPort: binary.BigEndian.Uint16(data[40:42]),
		PacketSize:      uint16(len(data)),
	}

	entry.EventID = makeEventID(entry)
	return []NATLogEntry{entry}, nil
}

func parsePacket(
	data []byte,
	routerIP uint32,
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
		"raw_%s_w%02d_%s_%d.open",
		rawFormat,
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
	if rawFormat == rawFormatBinary {
		if _, err := writer.WriteString(rawBinaryMagic); err != nil {
			_ = file.Close()
			_ = os.Remove(filePath)
			return nil, nil, "", err
		}
	}

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

func writeRawJSONLPacket(writer *bufio.Writer, packet UDPPacket) error {
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
		return err
	}

	rawJSON = append(rawJSON, '\n')
	_, err = writer.Write(rawJSON)
	return err
}

func writeRawBinaryPacket(writer *bufio.Writer, packet UDPPacket) error {
	routerIP := []byte(packet.RouterIP)
	if len(routerIP) > 65535 {
		return fmt.Errorf("raw_binary_router_ip_too_long length=%d", len(routerIP))
	}

	data := packet.Buffer[:packet.Length]
	if len(data) > 65535 {
		return fmt.Errorf("raw_binary_packet_too_large length=%d", len(data))
	}

	recordLen := 8 + 2 + 2 + 2 + len(routerIP) + len(data)
	var header [18]byte
	binary.BigEndian.PutUint32(header[0:4], uint32(recordLen))
	binary.BigEndian.PutUint64(header[4:12], uint64(packet.ReceivedUnixNano))
	binary.BigEndian.PutUint16(header[12:14], packet.RouterPort)
	binary.BigEndian.PutUint16(header[14:16], uint16(len(routerIP)))
	binary.BigEndian.PutUint16(header[16:18], uint16(len(data)))

	if _, err := writer.Write(header[:]); err != nil {
		return err
	}
	if _, err := writer.Write(routerIP); err != nil {
		return err
	}
	_, err := writer.Write(data)
	return err
}

func writeRawPacket(writer *bufio.Writer, packet UDPPacket) error {
	switch rawFormat {
	case rawFormatBinary:
		return writeRawBinaryPacket(writer, packet)
	case rawFormatJSONL:
		return writeRawJSONLPacket(writer, packet)
	default:
		return fmt.Errorf("unsupported_raw_format format=%s", rawFormat)
	}
}

func skipLiveInsert() {
	atomic.AddUint64(&totalPacketQueueDrops, 1)
	atomic.AddUint64(&totalLiveInsertSkipped, 1)
}

func enqueueForLiveInsert(packet UDPPacket, parsedPackets chan UDPPacket) bool {
	if liveInsertOverloadPolicy != "raw_only" {
		parsedPackets <- packet
		return true
	}

	if channelUsagePct(parsedPackets) >= liveInsertQueueHighWatermarkPct {
		skipLiveInsert()
		return false
	}

	if liveInsertSendTimeoutMS <= 0 {
		select {
		case parsedPackets <- packet:
			return true
		default:
			skipLiveInsert()
			return false
		}
	}

	timer := time.NewTimer(time.Duration(liveInsertSendTimeoutMS) * time.Millisecond)
	defer timer.Stop()

	select {
	case parsedPackets <- packet:
		return true
	case <-timer.C:
		skipLiveInsert()
		return false
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
	parsedPackets chan UDPPacket,
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
		if err := writeRawPacket(writer, packet); err != nil {
			atomic.AddUint64(&totalRawMarshalErrors, 1)
			log.Printf(
				"raw_encode_error writer=%d router_ip=%s packet_size=%d error=%v",
				writerID,
				packet.RouterIP,
				packet.Length,
				err,
			)
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

		if !enqueueForLiveInsert(packet, parsedPackets) {
			releasePacketBuffer(packet.Buffer)
		}
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
	batches chan InsertBatch,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	flushTicker := time.NewTicker(time.Duration(insertFlushMS) * time.Millisecond)
	defer flushTicker.Stop()

	initialBatchCapacity := insertBatchBytes
	if initialBatchCapacity > 1024*1024 {
		initialBatchCapacity = 1024 * 1024
	}

	body := make([]byte, 0, initialBatchCapacity)
	rows := 0

	flush := func() {
		if rows == 0 {
			return
		}

		if sendInsertBatch(InsertBatch{Body: body, Rows: rows}, batches) {
			body = make([]byte, 0, initialBatchCapacity)
			rows = 0
			return
		}

		atomic.AddUint64(&totalLiveSkippedRows, uint64(rows))
		body = make([]byte, 0, initialBatchCapacity)
		rows = 0
	}

	for {
		select {
		case packet, ok := <-packets:
			if !ok {
				flush()
				return
			}

			data := packet.Buffer[:packet.Length]

			entries, err := parsePacket(
				data,
				packet.RouterIPNum,
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
					beforeLen := len(body)
					var err error
					body, err = appendRowBinaryEvent(body, entry)
					if err != nil {
						body = body[:beforeLen]
						atomic.AddUint64(&totalEventMarshalError, 1)
						log.Printf("event_rowbinary_error worker=%d event_id=%s error=%v", workerID, entry.EventID, err)
						continue
					}

					rows++
					atomic.AddUint64(&totalParsed, 1)

					if rows >= insertBatchRows || len(body) >= insertBatchBytes {
						flush()
					}
				}
			}

			atomic.AddUint64(&totalPacketProcessed, 1)
			releasePacketBuffer(packet.Buffer)

		case <-flushTicker.C:
			flush()
		}
	}
}

func appendRowBinaryEvent(buffer []byte, entry NATLogEntry) ([]byte, error) {
	buffer = appendUInt32(buffer, entry.EventTime)
	buffer = appendUInt32(buffer, entry.StartTime)
	buffer = appendUInt32(buffer, entry.EndTime)

	var err error
	buffer, err = appendRowBinaryFixedString(buffer, entry.EventID, 40)
	if err != nil {
		return buffer, err
	}

	buffer = appendRowBinaryString(buffer, entry.EventType)
	buffer = appendRowBinaryString(buffer, entry.Header)
	buffer = appendUInt32(buffer, entry.RouterIP)
	buffer = appendUInt16(buffer, entry.RouterPort)
	buffer = appendUInt8(buffer, entry.ProtocolID)
	buffer = appendRowBinaryString(buffer, entry.Protocol)
	buffer = appendUInt32(buffer, entry.PrivateIP)
	buffer = appendUInt16(buffer, entry.PrivatePort)
	buffer = appendUInt32(buffer, entry.PublicIP)
	buffer = appendUInt16(buffer, entry.PublicPort)
	buffer = appendUInt32(buffer, entry.DestinationIP)
	buffer = appendUInt16(buffer, entry.DestinationPort)
	buffer = appendUInt16(buffer, entry.PacketSize)
	return buffer, nil
}

func sendInsertBatch(batch InsertBatch, batches chan InsertBatch) bool {
	if batch.Rows == 0 {
		return true
	}

	if liveInsertOverloadPolicy != "raw_only" {
		batches <- batch
		return true
	}

	if channelUsagePct(batches) >= liveInsertQueueHighWatermarkPct {
		return false
	}

	if liveInsertSendTimeoutMS <= 0 {
		select {
		case batches <- batch:
			return true
		default:
			return false
		}
	}

	timer := time.NewTimer(time.Duration(liveInsertSendTimeoutMS) * time.Millisecond)
	defer timer.Stop()

	select {
	case batches <- batch:
		return true
	case <-timer.C:
		return false
	}
}

func newHTTPClient() *http.Client {
	maxWorkers := autoTuneMaxInsertWorkers
	if !autoTuneEnabled {
		maxWorkers = insertWorkers
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          maxWorkers * 4,
		MaxIdleConnsPerHost:   maxWorkers * 2,
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

	request.Header.Set("Content-Type", "application/octet-stream")
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
	spoolQueues []chan UDPPacket,
	packetChannel <-chan UDPPacket,
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
			spoolLen, spoolCap := channelsLenCap(spoolQueues)

			log.Printf(
				"metrics live_batch_mode=packet_worker_direct total_received=%d total_packet_processed=%d total_raw_spooled=%d total_parsed=%d total_inserted=%d total_packet_queue_drops=%d total_live_insert_skipped=%d total_live_insert_skipped_rows=%d total_parse_errors=%d total_raw_marshal_errors=%d total_event_marshal_errors=%d total_insert_errors=%d total_insert_dropped_rows=%d total_batches_inserted=%d rate_received_10s=%d rate_packet_processed_10s=%d rate_raw_spooled_10s=%d rate_parsed_10s=%d rate_inserted_10s=%d queue_raw_input=%d/%d queue_packet=%d/%d queue_batch=%d/%d workers_raw=%d workers_packet=%d workers_batch=%d workers_insert=%d",
				received,
				processed,
				rawSpooled,
				parsed,
				inserted,
				atomic.LoadUint64(&totalPacketQueueDrops),
				atomic.LoadUint64(&totalLiveInsertSkipped),
				atomic.LoadUint64(&totalLiveSkippedRows),
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
				spoolLen,
				spoolCap,
				len(packetChannel),
				cap(packetChannel),
				len(batches),
				cap(batches),
				atomic.LoadInt64(&currentRawWriters),
				atomic.LoadInt64(&currentPacketWorkers),
				atomic.LoadInt64(&currentBatchBuilders),
				atomic.LoadInt64(&currentInsertWorkers),
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

func autoTuneSupervisor(
	stop <-chan struct{},
	spoolQueues []chan UDPPacket,
	packetChannel <-chan UDPPacket,
	insertBatches <-chan InsertBatch,
	spawnRawWriters func(int),
	spawnPacketWorkers func(int),
	spawnInsertWorkers func(int),
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	if !autoTuneEnabled {
		return
	}

	ticker := time.NewTicker(time.Duration(autoTuneIntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rawUsage := channelsMaxUsagePct(spoolQueues)
			packetUsage := channelUsagePct(packetChannel)
			batchUsage := channelUsagePct(insertBatches)

			if rawUsage >= autoTuneHighWatermarkPct {
				current := int(atomic.LoadInt64(&currentRawWriters))
				step := scaleStep(current, autoTuneMaxRawWriters, rawUsage >= autoTuneCriticalWatermarkPct)
				if step > 0 {
					spawnRawWriters(step)
					log.Printf(
						"auto_tune_scale component=raw_writers from=%d to=%d queue_raw_input_pct=%d",
						current,
						current+step,
						rawUsage,
					)
				}
			}

			if packetUsage >= autoTuneHighWatermarkPct && batchUsage < autoTuneCriticalWatermarkPct {
				current := int(atomic.LoadInt64(&currentPacketWorkers))
				step := scaleStep(current, autoTuneMaxPacketWorkers, packetUsage >= autoTuneCriticalWatermarkPct)
				if step > 0 {
					spawnPacketWorkers(step)
					log.Printf(
						"auto_tune_scale component=packet_workers from=%d to=%d queue_packet_pct=%d queue_batch_pct=%d",
						current,
						current+step,
						packetUsage,
						batchUsage,
					)
				}
			}

			if batchUsage >= autoTuneHighWatermarkPct {
				current := int(atomic.LoadInt64(&currentInsertWorkers))
				step := scaleStep(current, autoTuneMaxInsertWorkers, batchUsage >= autoTuneCriticalWatermarkPct)
				if step > 0 {
					spawnInsertWorkers(step)
					log.Printf(
						"auto_tune_scale component=insert_workers from=%d to=%d queue_batch_pct=%d",
						current,
						current+step,
						batchUsage,
					)
				}
			}

		case <-stop:
			return
		}
	}
}

func loadConfig() {
	listenAddr = getEnv("LISTEN_ADDR", "0.0.0.0:9088")

	clickhouseURL = getEnv("CLICKHOUSE_URL", "http://127.0.0.1:8123")
	clickhouseUser = getEnv("CLICKHOUSE_USER", "admin")
	clickhousePass = getEnv("CLICKHOUSE_PASS", "")
	clickhouseTable = getEnv("CLICKHOUSE_TABLE", "cgnat.huawei_cgn_nat_v2")

	rawSpoolBase = getEnv("RAW_SPOOL_BASE", "/index2/huawei-cgn-go/raw")
	rawFormat = strings.ToLower(getEnv("RAW_SPOOL_FORMAT", rawFormatBinary))

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
	udpReceivers = getEnvInt("UDP_RECEIVERS", 4)
	udpBatchSize = getEnvInt("UDP_BATCH_SIZE", 64)
	udpReusePort = getEnvBool("UDP_REUSEPORT", true)
	if strings.TrimSpace(os.Getenv("RAW_WRITERS")) == "" && rawWriters < udpReceivers {
		rawWriters = udpReceivers
	}

	autoTuneEnabled = getEnvBool("AUTO_TUNE", true)
	autoTuneIntervalSeconds = getEnvInt("AUTO_TUNE_INTERVAL_SECONDS", 5)
	autoTuneHighWatermarkPct = getEnvInt("AUTO_TUNE_HIGH_WATERMARK_PCT", 70)
	autoTuneCriticalWatermarkPct = getEnvInt("AUTO_TUNE_CRITICAL_WATERMARK_PCT", 90)

	defaultMaxRawWriters := rawWriters * 4
	if defaultMaxRawWriters < rawWriters {
		defaultMaxRawWriters = rawWriters
	}
	defaultMaxPacketWorkers := runtime.NumCPU() * 2
	if defaultMaxPacketWorkers < packetWorkers {
		defaultMaxPacketWorkers = packetWorkers
	}
	defaultMaxBatchBuilders := batchBuilders * 4
	if defaultMaxBatchBuilders < batchBuilders {
		defaultMaxBatchBuilders = batchBuilders
	}
	defaultMaxInsertWorkers := insertWorkers * 4
	if defaultMaxInsertWorkers < insertWorkers {
		defaultMaxInsertWorkers = insertWorkers
	}

	autoTuneMaxRawWriters = getEnvInt("AUTO_TUNE_MAX_RAW_WRITERS", defaultMaxRawWriters)
	autoTuneMaxPacketWorkers = getEnvInt("AUTO_TUNE_MAX_PACKET_WORKERS", defaultMaxPacketWorkers)
	autoTuneMaxBatchBuilders = getEnvInt("AUTO_TUNE_MAX_BATCH_BUILDERS", defaultMaxBatchBuilders)
	autoTuneMaxInsertWorkers = getEnvInt("AUTO_TUNE_MAX_INSERT_WORKERS", defaultMaxInsertWorkers)

	liveInsertOverloadPolicy = strings.ToLower(getEnv("LIVE_INSERT_OVERLOAD_POLICY", "raw_only"))
	liveInsertQueueHighWatermarkPct = getEnvInt("LIVE_INSERT_QUEUE_HIGH_WATERMARK_PCT", 95)
	liveInsertSendTimeoutMS = getEnvInt("LIVE_INSERT_SEND_TIMEOUT_MS", 1)

	query := fmt.Sprintf("INSERT INTO %s FORMAT RowBinary", clickhouseTable)
	insertURL = strings.TrimRight(clickhouseURL, "/") + "/?query=" + url.QueryEscape(query)
}

func validateConfig() {
	positiveValues := map[string]int{
		"MAX_EVENTS_PER_FILE":          maxPacketsPerFile,
		"MIN_EVENTS_PER_FILE":          minPacketsPerFile,
		"ROTATE_SECONDS":               rotateSeconds,
		"FORCE_ROTATE_SECONDS":         forceRotateSeconds,
		"RAW_WRITER_BUFFER_MB":         rawWriterBufferMB,
		"PACKET_WORKERS":               packetWorkers,
		"PACKET_CHANNEL_SIZE":          packetChannelSize,
		"RAW_WRITERS":                  rawWriters,
		"RAW_CHANNEL_SIZE":             rawChannelSize,
		"INSERT_WORKERS":               insertWorkers,
		"INSERT_BATCH_ROWS":            insertBatchRows,
		"INSERT_BATCH_BYTES":           insertBatchBytes,
		"INSERT_FLUSH_MS":              insertFlushMS,
		"INSERT_BATCH_CHANNEL_SIZE":    insertBatchChanSize,
		"INSERT_MAX_RETRIES":           insertMaxRetries,
		"INSERT_RETRY_MS":              insertRetryMS,
		"INSERT_HTTP_TIMEOUT_SECONDS":  insertHTTPTimeoutS,
		"UDP_READ_BUFFER_MB":           udpReadBufferMB,
		"UDP_RECEIVERS":                udpReceivers,
		"UDP_BATCH_SIZE":               udpBatchSize,
		"AUTO_TUNE_INTERVAL_SECONDS":   autoTuneIntervalSeconds,
		"AUTO_TUNE_MAX_RAW_WRITERS":    autoTuneMaxRawWriters,
		"AUTO_TUNE_MAX_PACKET_WORKERS": autoTuneMaxPacketWorkers,
		"AUTO_TUNE_MAX_INSERT_WORKERS": autoTuneMaxInsertWorkers,
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

	switch rawFormat {
	case rawFormatBinary, rawFormatJSONL:
	default:
		log.Fatal("RAW_SPOOL_FORMAT must be binary or jsonl")
	}

	if autoTuneHighWatermarkPct <= 0 || autoTuneHighWatermarkPct > 100 {
		log.Fatal("AUTO_TUNE_HIGH_WATERMARK_PCT must be between 1 and 100")
	}
	if autoTuneCriticalWatermarkPct <= 0 || autoTuneCriticalWatermarkPct > 100 {
		log.Fatal("AUTO_TUNE_CRITICAL_WATERMARK_PCT must be between 1 and 100")
	}
	if autoTuneCriticalWatermarkPct < autoTuneHighWatermarkPct {
		log.Fatal("AUTO_TUNE_CRITICAL_WATERMARK_PCT cannot be lower than AUTO_TUNE_HIGH_WATERMARK_PCT")
	}

	if autoTuneMaxRawWriters < rawWriters {
		log.Fatal("AUTO_TUNE_MAX_RAW_WRITERS cannot be lower than RAW_WRITERS")
	}
	if autoTuneMaxPacketWorkers < packetWorkers {
		log.Fatal("AUTO_TUNE_MAX_PACKET_WORKERS cannot be lower than PACKET_WORKERS")
	}
	if autoTuneMaxInsertWorkers < insertWorkers {
		log.Fatal("AUTO_TUNE_MAX_INSERT_WORKERS cannot be lower than INSERT_WORKERS")
	}

	switch liveInsertOverloadPolicy {
	case "block", "raw_only":
	default:
		log.Fatal("LIVE_INSERT_OVERLOAD_POLICY must be block or raw_only")
	}
	if liveInsertQueueHighWatermarkPct <= 0 || liveInsertQueueHighWatermarkPct > 100 {
		log.Fatal("LIVE_INSERT_QUEUE_HIGH_WATERMARK_PCT must be between 1 and 100")
	}
	if liveInsertSendTimeoutMS < 0 {
		log.Fatal("LIVE_INSERT_SEND_TIMEOUT_MS cannot be negative")
	}
}

func makeSpoolQueues(count int, queueSize int) []chan UDPPacket {
	queues := make([]chan UDPPacket, count)
	for index := range queues {
		queues[index] = make(chan UDPPacket, queueSize)
	}
	return queues
}

func closeUDPReceivers(receivers []*udpReceiver) {
	for _, receiver := range receivers {
		if receiver != nil {
			_ = receiver.Close()
		}
	}
}

func udpReadLoop(
	receiverID int,
	receiver *udpReceiver,
	spoolQueue chan<- UDPPacket,
	stop <-chan struct{},
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	results := make([]UDPReadResult, udpBatchSize)

	for {
		count, err := receiver.ReadBatch(results)
		if err != nil {
			select {
			case <-stop:
				return
			default:
			}
			log.Printf("udp_read_error receiver=%d error=%v", receiverID, err)
			continue
		}

		receivedAt := time.Now().UnixNano()
		for index := 0; index < count; index++ {
			result := results[index]
			bytesRead := len(result.Data)
			if bytesRead == 0 {
				continue
			}

			atomic.AddUint64(&totalReceived, 1)

			packetBuffer := acquirePacketBuffer(bytesRead)
			copy(packetBuffer, result.Data)

			packet := UDPPacket{
				Buffer:           packetBuffer,
				Length:           bytesRead,
				RouterIP:         result.RouterIP,
				RouterIPNum:      result.RouterIPNum,
				RouterPort:       result.RouterPort,
				ReceivedUnixNano: receivedAt,
			}

			select {
			case spoolQueue <- packet:
			case <-stop:
				releasePacketBuffer(packetBuffer)
				return
			}
		}
	}
}

func main() {
	loadConfig()
	validateConfig()

	if err := ensureRawSpoolDirs(rawSpoolBase); err != nil {
		log.Fatalf("raw_spool_directory_error error=%v", err)
	}
	recoverRawSpool()

	readBufferBytes := udpReadBufferMB * 1024 * 1024
	receivers := make([]*udpReceiver, 0, udpReceivers)
	for receiverID := 1; receiverID <= udpReceivers; receiverID++ {
		receiver, err := openUDPReceiver(listenAddr, udpReusePort, readBufferBytes, udpBatchSize)
		if err != nil {
			closeUDPReceivers(receivers)
			log.Fatalf(
				"listen_udp_error receiver=%d address=%s reuse_port=%t error=%v",
				receiverID,
				listenAddr,
				udpReusePort,
				err,
			)
		}
		receivers = append(receivers, receiver)
	}
	defer closeUDPReceivers(receivers)

	spoolQueues := makeSpoolQueues(udpReceivers, rawChannelSize)
	packetChannel := make(chan UDPPacket, packetChannelSize)
	insertBatches := make(chan InsertBatch, insertBatchChanSize)
	fatalErrors := make(chan error, 1)
	stop := make(chan struct{})

	var stopOnce sync.Once
	requestStop := func(reason string) {
		stopOnce.Do(func() {
			log.Printf("collector_shutdown_requested reason=%s", reason)
			close(stop)
			closeUDPReceivers(receivers)
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

	var readWG sync.WaitGroup
	var rawWG sync.WaitGroup
	var packetWG sync.WaitGroup
	var insertWG sync.WaitGroup
	var autoTuneWG sync.WaitGroup
	var nextRawQueue uint64

	spawnRawWriters := func(count int) {
		for range count {
			writerID := int(atomic.AddInt64(&currentRawWriters, 1))
			queueIndex := int(atomic.AddUint64(&nextRawQueue, 1)-1) % len(spoolQueues)
			rawWG.Add(1)
			go rawSpoolWriter(writerID, spoolQueues[queueIndex], packetChannel, fatalErrors, &rawWG)
		}
	}

	spawnPacketWorkers := func(count int) {
		for range count {
			workerID := int(atomic.AddInt64(&currentPacketWorkers, 1))
			atomic.AddInt64(&currentBatchBuilders, 1)
			packetWG.Add(1)
			go packetWorker(workerID, packetChannel, insertBatches, &packetWG)
		}
	}

	spawnInsertWorkers := func(count int) {
		for range count {
			workerID := int(atomic.AddInt64(&currentInsertWorkers, 1))
			insertWG.Add(1)
			go insertWorker(workerID, insertBatches, &insertWG)
		}
	}

	spawnRawWriters(rawWriters)
	spawnPacketWorkers(packetWorkers)
	spawnInsertWorkers(insertWorkers)

	for receiverIndex, receiver := range receivers {
		readWG.Add(1)
		go udpReadLoop(receiverIndex+1, receiver, spoolQueues[receiverIndex], stop, &readWG)
	}

	go metricsLogger(stop, spoolQueues, packetChannel, insertBatches)

	autoTuneWG.Add(1)
	go autoTuneSupervisor(
		stop,
		spoolQueues,
		packetChannel,
		insertBatches,
		spawnRawWriters,
		spawnPacketWorkers,
		spawnInsertWorkers,
		&autoTuneWG,
	)

	log.Printf(
		"collector_started listen_addr=%s clickhouse_url=%s clickhouse_table=%s clickhouse_insert_format=RowBinary live_batch_mode=packet_worker_direct raw_spool=%s raw_format=%s udp_receivers=%d udp_reuse_port=%t udp_batch_size=%d packet_workers=%d packet_channel_size=%d raw_writers=%d raw_channel_size_per_receiver=%d batch_builders_ignored=%d insert_workers=%d insert_batch_rows=%d insert_batch_bytes=%d insert_flush_ms=%d udp_read_buffer_mb=%d auto_tune=%t auto_tune_max_raw_writers=%d auto_tune_max_packet_workers=%d auto_tune_max_batch_builders_ignored=%d auto_tune_max_insert_workers=%d live_insert_overload_policy=%s live_insert_queue_high_watermark_pct=%d parsed_json_spool=false accept_any_header=true dynamic_multi_record=true start_end_time=true raw_first_pipeline=true direct_live_batching=true graceful_shutdown=true",
		listenAddr,
		clickhouseURL,
		clickhouseTable,
		rawSpoolBase,
		rawFormat,
		udpReceivers,
		udpReusePort,
		udpBatchSize,
		packetWorkers,
		packetChannelSize,
		rawWriters,
		rawChannelSize,
		batchBuilders,
		insertWorkers,
		insertBatchRows,
		insertBatchBytes,
		insertFlushMS,
		udpReadBufferMB,
		autoTuneEnabled,
		autoTuneMaxRawWriters,
		autoTuneMaxPacketWorkers,
		autoTuneMaxBatchBuilders,
		autoTuneMaxInsertWorkers,
		liveInsertOverloadPolicy,
		liveInsertQueueHighWatermarkPct,
	)

	readWG.Wait()
	log.Printf("collector_draining_start")
	autoTuneWG.Wait()
	for _, queue := range spoolQueues {
		close(queue)
	}
	rawWG.Wait()
	close(packetChannel)
	packetWG.Wait()
	close(insertBatches)
	insertWG.Wait()
	log.Printf("collector_stopped")
}
