package main

import (
	"bufio"
	"bytes"
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
	generalHeaderSize   = 16
	recordSize          = 64
	maxUDPPacketSize    = 65535
	rawFormatBinary     = "binary"
	rawFormatJSONL      = "jsonl"
	rawBinaryMagic      = "HCGNRAW2\n"
	udpModeReusePort    = "reuseport"
	udpModeReusePortBPF = "reuseport_bpf"
	udpModeShared       = "shared_socket"
	rowBinarySchemaV2   = 2
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
	RouterIP        uint32
	RouterPort      uint16
	ProtocolID      uint8
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
	Data           []byte
	RouterIP       string
	RouterIPNum    uint32
	RouterPort     uint16
	SocketDrops    uint32
	HasSocketDrops bool
}

type UDPReceiverStats struct {
	Batches     uint64
	Packets     uint64
	FullBatches uint64
	Errors      uint64
	KernelDrops uint64
}

type InsertBatch struct {
	TableName   string
	Body        []byte
	Rows        int
	Cohorts     []insertCohort
	CohortShard int
}

type FailedInsertBatchMeta struct {
	SchemaVersion int    `json:"schema_version"`
	CreatedTime   string `json:"created_time"`
	TableName     string `json:"table_name"`
	Rows          int    `json:"rows"`
	Bytes         int    `json:"bytes"`
	Format        string `json:"format"`
	Error         string `json:"error"`
}

var (
	listenAddr      string
	clickhouseURL   string
	clickhouseUser  string
	clickhousePass  string
	clickhouseTable string
	dailyTables     bool

	rawSpoolBase    string
	failedSpoolBase string
	rawFormat       string

	maxPacketsPerFile            int
	minPacketsPerFile            int
	rotateSeconds                int
	forceRotateSeconds           int
	syncEveryPackets             int
	rawWriterBufferMB            int
	rawWriters                   int
	rawChannelSize               int
	packetWorkers                int
	packetChannelSize            int
	eventChannelSize             int
	insertWorkers                int
	batchBuilders                int
	insertBatchRows              int
	insertBatchBytes             int
	insertFlushMS                int
	insertBatchChanSize          int
	insertMaxRetries             int
	insertRetryMS                int
	insertHTTPTimeoutS           int
	udpReadBufferMB              int
	udpReceivers                 int
	udpBatchSize                 int
	udpReusePort                 bool
	udpReceiveMode               string
	udpReusePortHashOffsetsValue string
	udpReusePortHashOffsets      []int

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

	totalReceived           uint64
	totalPacketProcessed    uint64
	totalPacketQueueDrops   uint64
	totalLiveInsertSkipped  uint64
	totalRawSpooled         uint64
	totalParsed             uint64
	totalInserted           uint64
	totalParseErrors        uint64
	totalRawMarshalErrors   uint64
	totalEventMarshalError  uint64
	totalInsertErrors       uint64
	totalInsertDroppedRows  uint64
	totalLiveSkippedRows    uint64
	totalBatchesInserted    uint64
	totalFailedBatchSpooled uint64
	totalFailedRowsSpooled  uint64
	totalFailedSpoolErrors  uint64
	totalTablesCreated      uint64
	totalTableCreateErrors  uint64

	currentRawWriters    int64
	currentPacketWorkers int64
	currentBatchBuilders int64
	currentInsertWorkers int64
)

var (
	createdTablesMu sync.Mutex
	createdTables   = make(map[string]struct{})
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

func isValidClickHouseIdentifier(value string) bool {
	if value == "" {
		return false
	}

	for index, char := range value {
		isLetter := (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z')
		isDigit := char >= '0' && char <= '9'
		if index == 0 {
			if !isLetter && char != '_' {
				return false
			}
			continue
		}

		if !isLetter && !isDigit && char != '_' {
			return false
		}
	}

	return true
}

func validateClickHouseTableName(tableName string) error {
	parts := strings.Split(tableName, ".")
	if len(parts) == 0 || len(parts) > 2 {
		return fmt.Errorf("invalid_clickhouse_table_name table=%s", tableName)
	}

	for _, part := range parts {
		if !isValidClickHouseIdentifier(part) {
			return fmt.Errorf("invalid_clickhouse_identifier part=%s table=%s", part, tableName)
		}
	}

	return nil
}

func dailyTableName(baseTable string, eventUnix uint32) string {
	if !dailyTables {
		return baseTable
	}

	suffix := time.Unix(int64(eventUnix), 0).In(peruTZ).Format("2006_01_02")
	parts := strings.Split(baseTable, ".")
	if len(parts) == 2 {
		return parts[0] + "." + parts[1] + "_" + suffix
	}
	return baseTable + "_" + suffix
}

func clickHouseInsertURL(tableName string) string {
	query := fmt.Sprintf(
		"INSERT INTO %s (start_time, end_time, router_ip, router_port, protocol_id, private_ip, private_port, public_ip, public_port, destination_ip, destination_port, packet_size) FORMAT RowBinary",
		tableName,
	)
	return strings.TrimRight(clickhouseURL, "/") + "/?query=" + url.QueryEscape(query)
}

func clickHouseCreateTableDDL(tableName string) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s
(
    start_time DateTime CODEC(ZSTD(3)),
    end_time DateTime,
    router_ip IPv4,
    router_port UInt16,
    protocol_id UInt8,
    private_ip IPv4,
    private_port UInt16,
    public_ip IPv4,
    public_port UInt16,
    destination_ip IPv4,
    destination_port UInt16,
    packet_size UInt16
)
ENGINE = MergeTree
ORDER BY (end_time, router_ip, private_ip, public_ip, destination_ip, private_port, public_port)
SETTINGS index_granularity = 8192`, tableName)
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

func parseMultiRecordPacket(
	data []byte,
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
			RouterIP:        routerIP,
			RouterPort:      routerPort,
			ProtocolID:      protocolID,
			PrivateIP:       privateIP,
			PrivatePort:     privatePort,
			PublicIP:        publicIP,
			PublicPort:      publicPort,
			DestinationIP:   destinationIP,
			DestinationPort: destinationPort,
			PacketSize:      uint16(packetSize),
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func parseLegacyPacket(
	data []byte,
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
		RouterIP:        routerIP,
		RouterPort:      routerPort,
		ProtocolID:      protocolID,
		PrivateIP:       ipv4BytesToUInt32(data[20:24]),
		PrivatePort:     binary.BigEndian.Uint16(data[36:38]),
		PublicIP:        ipv4BytesToUInt32(data[24:28]),
		PublicPort:      binary.BigEndian.Uint16(data[38:40]),
		DestinationIP:   ipv4BytesToUInt32(data[28:32]),
		DestinationPort: binary.BigEndian.Uint16(data[40:42]),
		PacketSize:      uint16(len(data)),
	}

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

	packetSize := len(data)

	if packetSize >= generalHeaderSize+recordSize &&
		(packetSize-generalHeaderSize)%recordSize == 0 {
		return parseMultiRecordPacket(
			data,
			routerIP,
			routerPort,
			receivedUnixNano,
		)
	}

	return parseLegacyPacket(
		data,
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

func ensureFailedSpoolDirs(base string) error {
	for _, dir := range []string{"open", "done"} {
		if err := os.MkdirAll(filepath.Join(base, dir), 0750); err != nil {
			return err
		}
	}
	return nil
}

func safeFileToken(value string) string {
	replacer := strings.NewReplacer(".", "_", "/", "_", "\\", "_", ":", "_", " ", "_")
	return replacer.Replace(value)
}

func spoolFailedInsertBatch(batch InsertBatch, insertErr error) error {
	if batch.Rows == 0 || len(batch.Body) == 0 {
		return nil
	}

	now := time.Now().In(peruTZ)
	token := safeFileToken(batch.TableName)
	baseName := fmt.Sprintf(
		"failed_%s_%s_%d_rows%d.rowbinary",
		token,
		now.Format("20060102_150405"),
		time.Now().UnixNano(),
		batch.Rows,
	)

	openPath := filepath.Join(failedSpoolBase, "open", baseName+".open")
	donePath := filepath.Join(failedSpoolBase, "done", baseName+".done")
	metaPath := donePath + ".json"

	file, err := os.OpenFile(openPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0640)
	if err != nil {
		return err
	}

	if _, err := file.Write(batch.Body); err != nil {
		_ = file.Close()
		_ = os.Remove(openPath)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(openPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(openPath)
		return err
	}

	if err := os.Rename(openPath, donePath); err != nil {
		_ = os.Remove(openPath)
		return err
	}

	meta := FailedInsertBatchMeta{
		SchemaVersion: rowBinarySchemaV2,
		CreatedTime:   now.Format("2006-01-02 15:04:05"),
		TableName:     batch.TableName,
		Rows:          batch.Rows,
		Bytes:         len(batch.Body),
		Format:        "RowBinary",
		Error:         insertErr.Error(),
	}

	metaJSON, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	metaJSON = append(metaJSON, '\n')
	if err := os.WriteFile(metaPath, metaJSON, 0640); err != nil {
		return err
	}

	atomic.AddUint64(&totalFailedBatchSpooled, 1)
	atomic.AddUint64(&totalFailedRowsSpooled, uint64(batch.Rows))
	log.Printf("failed_insert_spooled table=%s rows=%d bytes=%d file=%s", batch.TableName, batch.Rows, len(batch.Body), donePath)
	return nil
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

	type pendingBatch struct {
		body    []byte
		rows    int
		cohorts []insertCohort
	}

	pending := make(map[string]*pendingBatch, 2)

	newPendingBatch := func() *pendingBatch {
		return &pendingBatch{
			body: make([]byte, 0, initialBatchCapacity),
		}
	}

	flushTable := func(tableName string, batch *pendingBatch) {
		if batch == nil || batch.rows == 0 {
			return
		}

		insertBatch := InsertBatch{
			TableName:   tableName,
			Body:        batch.body,
			Rows:        batch.rows,
			Cohorts:     batch.cohorts,
			CohortShard: udpReceivers + workerID,
		}
		if sendInsertBatch(insertBatch, batches) {
			pending[tableName] = newPendingBatch()
			return
		}

		atomic.AddUint64(&totalLiveSkippedRows, uint64(batch.rows))
		atomic.AddUint64(&totalLiveInsertSkipped, 1)
		overloadErr := fmt.Errorf("live_insert_queue_overload queue_batch=%d/%d", len(batches), cap(batches))
		if err := spoolFailedInsertBatch(insertBatch, overloadErr); err != nil {
			atomic.AddUint64(&totalFailedSpoolErrors, 1)
			if alertCohorts != nil {
				alertCohorts.recordSpoolFailure(insertBatch.Cohorts, insertBatch.CohortShard, time.Now().Unix())
			}
			log.Printf(
				"failed_insert_spool_error worker=%d table=%s rows=%d bytes=%d error=%v original_insert_error=%v",
				workerID,
				tableName,
				batch.rows,
				len(batch.body),
				err,
				overloadErr,
			)
		} else if alertCohorts != nil {
			alertCohorts.recordSpooled(insertBatch.Cohorts, insertBatch.CohortShard, time.Now().Unix())
		}
		pending[tableName] = newPendingBatch()
	}

	flushAll := func() {
		for tableName, batch := range pending {
			flushTable(tableName, batch)
		}
	}

	for {
		select {
		case packet, ok := <-packets:
			if !ok {
				flushAll()
				return
			}

			data := packet.Buffer[:packet.Length]

			entries, err := parsePacket(
				data,
				packet.RouterIPNum,
				packet.RouterPort,
				packet.ReceivedUnixNano,
			)
			cohortSecond := packet.ReceivedUnixNano / int64(time.Second)
			if err != nil {
				atomic.AddUint64(&totalParseErrors, 1)
				if alertCohorts != nil {
					alertCohorts.recordParseResult(cohortSecond, udpReceivers+workerID, false, 0)
				}
				log.Printf(
					"parse_error worker=%d router_ip=%s packet_size=%d error=%v",
					workerID,
					packet.RouterIP,
					packet.Length,
					err,
				)
			} else {
				if alertCohorts != nil {
					alertCohorts.recordParseResult(cohortSecond, udpReceivers+workerID, true, uint64(len(entries)))
				}
				for _, entry := range entries {
					tableName := dailyTableName(clickhouseTable, entry.EventTime)
					batch := pending[tableName]
					if batch == nil {
						batch = newPendingBatch()
						pending[tableName] = batch
					}

					beforeLen := len(batch.body)
					var err error
					batch.body, err = appendRowBinaryEvent(batch.body, entry)
					if err != nil {
						batch.body = batch.body[:beforeLen]
						atomic.AddUint64(&totalEventMarshalError, 1)
						log.Printf("event_rowbinary_error worker=%d router_ip=%d end_time=%d error=%v", workerID, entry.RouterIP, entry.EndTime, err)
						continue
					}

					batch.rows++
					if alertCohorts != nil {
						batch.cohorts = addInsertCohort(batch.cohorts, cohortSecond, 1)
					}
					atomic.AddUint64(&totalParsed, 1)

					if batch.rows >= insertBatchRows || len(batch.body) >= insertBatchBytes {
						flushTable(tableName, batch)
					}
				}
			}

			atomic.AddUint64(&totalPacketProcessed, 1)
			releasePacketBuffer(packet.Buffer)

		case <-flushTicker.C:
			flushAll()
		}
	}
}

func appendRowBinaryEvent(buffer []byte, entry NATLogEntry) ([]byte, error) {
	buffer = appendUInt32(buffer, entry.StartTime)
	buffer = appendUInt32(buffer, entry.EndTime)
	buffer = appendUInt32(buffer, entry.RouterIP)
	buffer = appendUInt16(buffer, entry.RouterPort)
	buffer = appendUInt8(buffer, entry.ProtocolID)
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

func executeClickHouseQuery(client *http.Client, query string) error {
	request, err := http.NewRequest(
		http.MethodPost,
		strings.TrimRight(clickhouseURL, "/")+"/?query="+url.QueryEscape(query),
		nil,
	)
	if err != nil {
		return err
	}

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

func ensureClickHouseTable(client *http.Client, tableName string) error {
	createdTablesMu.Lock()
	if _, ok := createdTables[tableName]; ok {
		createdTablesMu.Unlock()
		return nil
	}
	defer createdTablesMu.Unlock()

	if err := executeClickHouseQuery(client, clickHouseCreateTableDDL(tableName)); err != nil {
		atomic.AddUint64(&totalTableCreateErrors, 1)
		return err
	}

	createdTables[tableName] = struct{}{}
	atomic.AddUint64(&totalTablesCreated, 1)
	log.Printf("clickhouse_table_ready table=%s", tableName)
	return nil
}

func insertBatchToClickHouse(client *http.Client, batch InsertBatch) error {
	if err := ensureClickHouseTable(client, batch.TableName); err != nil {
		return fmt.Errorf("clickhouse_create_table table=%s error=%w", batch.TableName, err)
	}

	request, err := http.NewRequest(
		http.MethodPost,
		clickHouseInsertURL(batch.TableName),
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
				if alertCohorts != nil {
					alertCohorts.recordInserted(batch.Cohorts, batch.CohortShard, time.Now().Unix())
				}
				break
			}

			log.Printf(
				"clickhouse_insert_retry worker=%d table=%s attempt=%d/%d rows=%d bytes=%d error=%v",
				workerID,
				batch.TableName,
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
			if err := spoolFailedInsertBatch(batch, lastErr); err != nil {
				atomic.AddUint64(&totalFailedSpoolErrors, 1)
				if alertCohorts != nil {
					alertCohorts.recordSpoolFailure(batch.Cohorts, batch.CohortShard, time.Now().Unix())
				}
				log.Printf(
					"failed_insert_spool_error worker=%d table=%s rows=%d bytes=%d error=%v original_insert_error=%v",
					workerID,
					batch.TableName,
					batch.Rows,
					len(batch.Body),
					err,
					lastErr,
				)
			} else if alertCohorts != nil {
				alertCohorts.recordSpooled(batch.Cohorts, batch.CohortShard, time.Now().Unix())
			}
			log.Printf(
				"clickhouse_insert_failed worker=%d table=%s rows=%d bytes=%d error=%v failed_batch_reprocess_required=true",
				workerID,
				batch.TableName,
				batch.Rows,
				len(batch.Body),
				lastErr,
			)
		}
	}
}

func metricsLogger(
	stop <-chan struct{},
	packetChannel <-chan UDPPacket,
	batches <-chan InsertBatch,
	receiverStats []UDPReceiverStats,
) {
	var previousReceived uint64
	var previousPacketProcessed uint64
	var previousParsed uint64
	var previousInserted uint64
	previousReceiverPackets := make([]uint64, len(receiverStats))
	previousReceiverBatches := make([]uint64, len(receiverStats))
	previousReceiverFullBatches := make([]uint64, len(receiverStats))
	previousReceiverErrors := make([]uint64, len(receiverStats))
	previousReceiverKernelDrops := make([]uint64, len(receiverStats))

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			received := atomic.LoadUint64(&totalReceived)
			processed := atomic.LoadUint64(&totalPacketProcessed)
			parsed := atomic.LoadUint64(&totalParsed)
			inserted := atomic.LoadUint64(&totalInserted)

			receiverDistribution := make([]string, len(receiverStats))
			var receiverPacketsMin uint64
			var receiverPacketsMax uint64
			var receiverBatchesDelta uint64
			var receiverPacketsDelta uint64
			var receiverFullBatchesDelta uint64
			var receiverErrorsDelta uint64
			var receiverKernelDropsDelta uint64
			for index := range receiverStats {
				packets := atomic.LoadUint64(&receiverStats[index].Packets)
				readBatches := atomic.LoadUint64(&receiverStats[index].Batches)
				fullBatches := atomic.LoadUint64(&receiverStats[index].FullBatches)
				readErrors := atomic.LoadUint64(&receiverStats[index].Errors)
				kernelDrops := atomic.LoadUint64(&receiverStats[index].KernelDrops)

				packetsDelta := packets - previousReceiverPackets[index]
				batchesDelta := readBatches - previousReceiverBatches[index]
				fullBatchesDelta := fullBatches - previousReceiverFullBatches[index]
				errorsDelta := readErrors - previousReceiverErrors[index]
				kernelDropsDelta := kernelDrops - previousReceiverKernelDrops[index]

				receiverDistribution[index] = strconv.Itoa(index+1) + ":" + strconv.FormatUint(packetsDelta, 10)
				if index == 0 || packetsDelta < receiverPacketsMin {
					receiverPacketsMin = packetsDelta
				}
				if packetsDelta > receiverPacketsMax {
					receiverPacketsMax = packetsDelta
				}
				receiverPacketsDelta += packetsDelta
				receiverBatchesDelta += batchesDelta
				receiverFullBatchesDelta += fullBatchesDelta
				receiverErrorsDelta += errorsDelta
				receiverKernelDropsDelta += kernelDropsDelta

				previousReceiverPackets[index] = packets
				previousReceiverBatches[index] = readBatches
				previousReceiverFullBatches[index] = fullBatches
				previousReceiverErrors[index] = readErrors
				previousReceiverKernelDrops[index] = kernelDrops
			}

			averageBatch := 0.0
			fullBatchPct := 0.0
			if receiverBatchesDelta > 0 {
				averageBatch = float64(receiverPacketsDelta) / float64(receiverBatchesDelta)
				fullBatchPct = float64(receiverFullBatchesDelta) * 100 / float64(receiverBatchesDelta)
			}

			log.Printf(
				"metrics live_batch_mode=packet_worker_direct raw_spool_mode=failed_inserts_only total_received=%d total_udp_kernel_drops=%d total_packet_processed=%d total_parsed=%d total_inserted=%d total_packet_queue_drops=%d total_live_insert_skipped=%d total_live_insert_skipped_rows=%d total_parse_errors=%d total_event_marshal_errors=%d total_insert_errors=%d total_insert_dropped_rows=%d total_batches_inserted=%d total_failed_batch_spooled=%d total_failed_rows_spooled=%d total_failed_spool_errors=%d total_tables_created=%d total_table_create_errors=%d alerts_enabled=%t alerts_mode=%s alerts_active=%d total_alerts_opened=%d total_alerts_cleared=%d total_alerts_delivered=%d total_alert_delivery_errors=%d alert_outbox_pending=%d rate_received_10s=%d rate_packet_processed_10s=%d rate_parsed_10s=%d rate_inserted_10s=%d pps_received_10s=%d pps_packet_processed_10s=%d rps_parsed_10s=%d rps_inserted_10s=%d queue_packet=%d/%d queue_batch=%d/%d workers_packet=%d workers_batch=%d workers_insert=%d udp_read_batches_10s=%d udp_average_batch_10s=%.2f udp_full_batch_pct_10s=%.2f udp_read_errors_10s=%d udp_kernel_drops_10s=%d udp_receiver_min_10s=%d udp_receiver_max_10s=%d udp_receiver_packets_10s=%s",
				received,
				atomic.LoadUint64(&totalUDPKernelDrops),
				processed,
				parsed,
				inserted,
				atomic.LoadUint64(&totalPacketQueueDrops),
				atomic.LoadUint64(&totalLiveInsertSkipped),
				atomic.LoadUint64(&totalLiveSkippedRows),
				atomic.LoadUint64(&totalParseErrors),
				atomic.LoadUint64(&totalEventMarshalError),
				atomic.LoadUint64(&totalInsertErrors),
				atomic.LoadUint64(&totalInsertDroppedRows),
				atomic.LoadUint64(&totalBatchesInserted),
				atomic.LoadUint64(&totalFailedBatchSpooled),
				atomic.LoadUint64(&totalFailedRowsSpooled),
				atomic.LoadUint64(&totalFailedSpoolErrors),
				atomic.LoadUint64(&totalTablesCreated),
				atomic.LoadUint64(&totalTableCreateErrors),
				alertConfig.Enabled,
				alertConfig.Mode,
				atomic.LoadInt64(&activeAlerts),
				atomic.LoadUint64(&totalAlertsOpened),
				atomic.LoadUint64(&totalAlertsCleared),
				atomic.LoadUint64(&totalAlertsDelivered),
				atomic.LoadUint64(&totalAlertDeliveryErrs),
				atomic.LoadInt64(&alertOutboxPending),
				received-previousReceived,
				processed-previousPacketProcessed,
				parsed-previousParsed,
				inserted-previousInserted,
				(received-previousReceived)/10,
				(processed-previousPacketProcessed)/10,
				(parsed-previousParsed)/10,
				(inserted-previousInserted)/10,
				len(packetChannel),
				cap(packetChannel),
				len(batches),
				cap(batches),
				atomic.LoadInt64(&currentPacketWorkers),
				atomic.LoadInt64(&currentBatchBuilders),
				atomic.LoadInt64(&currentInsertWorkers),
				receiverBatchesDelta,
				averageBatch,
				fullBatchPct,
				receiverErrorsDelta,
				receiverKernelDropsDelta,
				receiverPacketsMin,
				receiverPacketsMax,
				strings.Join(receiverDistribution, ","),
			)

			previousReceived = received
			previousPacketProcessed = processed
			previousParsed = parsed
			previousInserted = inserted
		case <-stop:
			return
		}
	}
}

func autoTuneSupervisor(
	stop <-chan struct{},
	packetChannel <-chan UDPPacket,
	insertBatches <-chan InsertBatch,
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
			packetUsage := channelUsagePct(packetChannel)
			batchUsage := channelUsagePct(insertBatches)

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
	dailyTables = getEnvBool("CLICKHOUSE_DAILY_TABLES", true)

	rawSpoolBase = getEnv("RAW_SPOOL_BASE", "/index2/huawei-cgn-go/raw")
	failedSpoolBase = getEnv("FAILED_SPOOL_BASE", filepath.Join(rawSpoolBase, "failed"))
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
	udpReceiveMode = strings.ToLower(getEnv("UDP_RECEIVE_MODE", udpModeReusePortBPF))
	udpReusePortHashOffsetsValue = getEnv("UDP_REUSEPORT_HASH_OFFSETS", "20,36")
	if udpReceiveMode == udpModeShared {
		udpReusePort = false
	} else if udpReceiveMode == udpModeReusePortBPF {
		udpReusePort = true
	}
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

	alertConfig = loadAlertConfig(
		filepath.Dir(failedSpoolBase),
		clickhouseURL,
		clickhouseUser,
		clickhousePass,
	)
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

	if err := validateClickHouseTableName(clickhouseTable); err != nil {
		log.Fatal(err)
	}

	switch rawFormat {
	case rawFormatBinary, rawFormatJSONL:
	default:
		log.Fatal("RAW_SPOOL_FORMAT must be binary or jsonl")
	}

	switch udpReceiveMode {
	case udpModeReusePort, udpModeReusePortBPF, udpModeShared:
	default:
		log.Fatal("UDP_RECEIVE_MODE must be reuseport_bpf, reuseport, or shared_socket")
	}

	var err error
	udpReusePortHashOffsets, err = parseReusePortHashOffsets(udpReusePortHashOffsetsValue)
	if err != nil {
		log.Fatalf("UDP_REUSEPORT_HASH_OFFSETS is invalid: %v", err)
	}
	if udpReceiveMode == udpModeReusePortBPF && udpReceivers < 2 {
		log.Fatal("UDP_RECEIVERS must be at least 2 when UDP_RECEIVE_MODE=reuseport_bpf")
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
	stats *UDPReceiverStats,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	results := make([]UDPReadResult, udpBatchSize)

	for {
		count, err := receiver.ReadBatch(results)
		if err != nil {
			atomic.AddUint64(&stats.Errors, 1)
			select {
			case <-stop:
				return
			default:
			}
			log.Printf("udp_read_error receiver=%d error=%v", receiverID, err)
			continue
		}
		atomic.AddUint64(&stats.Batches, 1)
		atomic.AddUint64(&stats.Packets, uint64(count))
		if count == len(results) {
			atomic.AddUint64(&stats.FullBatches, 1)
		}

		receivedAt := time.Now().UnixNano()
		receivedSecond := receivedAt / int64(time.Second)
		for index := 0; index < count; index++ {
			result := results[index]
			bytesRead := len(result.Data)
			if bytesRead == 0 {
				continue
			}
			if result.HasSocketDrops {
				dropped := receiver.ObserveSocketDrops(result.SocketDrops)
				if dropped > 0 {
					atomic.AddUint64(&stats.KernelDrops, dropped)
					atomic.AddUint64(&totalUDPKernelDrops, dropped)
					if alertCohorts != nil {
						alertCohorts.recordUDPKernelDrops(receivedSecond, receiverID, dropped)
					}
				}
			}

			atomic.AddUint64(&totalReceived, 1)
			if alertCohorts != nil {
				alertCohorts.recordReceived(receivedSecond, receiverID)
			}

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

	if err := ensureFailedSpoolDirs(failedSpoolBase); err != nil {
		log.Fatalf("failed_spool_directory_error error=%v", err)
	}

	var alerts *alertManager
	if alertConfig.Enabled {
		alertCohorts = newCohortTracker(
			udpReceivers+autoTuneMaxPacketWorkers+1,
			alertCohortRetention(alertConfig),
		)
		var err error
		alerts, err = newAlertManager(alertConfig, alertCohorts)
		if err != nil {
			log.Printf("alerting_start_failed alerts_disabled=true error=%v", err)
			alertCohorts = nil
			alertConfig.Enabled = false
			atomic.StoreInt64(&activeAlerts, 0)
		}
	}

	readBufferBytes := udpReadBufferMB * 1024 * 1024
	receivers := make([]*udpReceiver, 0, udpReceivers)
	switch udpReceiveMode {
	case udpModeShared:
		receiver, err := openUDPReceiver(listenAddr, false, readBufferBytes, udpBatchSize)
		if err != nil {
			log.Fatalf(
				"listen_udp_error receiver=1 address=%s receive_mode=%s reuse_port=false error=%v",
				listenAddr,
				udpReceiveMode,
				err,
			)
		}
		receivers = append(receivers, receiver)
		for receiverID := 2; receiverID <= udpReceivers; receiverID++ {
			receivers = append(receivers, cloneUDPReceiver(receiver, udpBatchSize))
		}
	case udpModeReusePort, udpModeReusePortBPF:
		for receiverID := 1; receiverID <= udpReceivers; receiverID++ {
			receiver, err := openUDPReceiver(listenAddr, udpReusePort, readBufferBytes, udpBatchSize)
			if err != nil {
				closeUDPReceivers(receivers)
				log.Fatalf(
					"listen_udp_error receiver=%d address=%s receive_mode=%s reuse_port=%t error=%v",
					receiverID,
					listenAddr,
					udpReceiveMode,
					udpReusePort,
					err,
				)
			}
			receivers = append(receivers, receiver)
		}
		if udpReceiveMode == udpModeReusePortBPF {
			if err := attachReusePortPayloadSelector(receivers[0], len(receivers), udpReusePortHashOffsets); err != nil {
				closeUDPReceivers(receivers)
				log.Fatalf(
					"reuseport_bpf_setup_error sockets=%d hash_offsets=%s error=%v",
					len(receivers),
					formatReusePortHashOffsets(udpReusePortHashOffsets),
					err,
				)
			}
			log.Printf(
				"reuseport_bpf_attached sockets=%d hash_offsets=%s selector=cbpf_payload_hash",
				len(receivers),
				formatReusePortHashOffsets(udpReusePortHashOffsets),
			)
		}
	}
	defer closeUDPReceivers(receivers)
	alerts.Start()
	receiverStats := make([]UDPReceiverStats, len(receivers))

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
	var packetWG sync.WaitGroup
	var insertWG sync.WaitGroup
	var autoTuneWG sync.WaitGroup

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

	spawnPacketWorkers(packetWorkers)
	spawnInsertWorkers(insertWorkers)

	for receiverIndex, receiver := range receivers {
		readWG.Add(1)
		go udpReadLoop(receiverIndex+1, receiver, packetChannel, stop, &receiverStats[receiverIndex], &readWG)
	}

	go metricsLogger(stop, packetChannel, insertBatches, receiverStats)

	autoTuneWG.Add(1)
	go autoTuneSupervisor(
		stop,
		packetChannel,
		insertBatches,
		spawnPacketWorkers,
		spawnInsertWorkers,
		&autoTuneWG,
	)

	log.Printf(
		"collector_started listen_addr=%s clickhouse_url=%s clickhouse_table_base=%s clickhouse_daily_tables=%t clickhouse_daily_pattern=%s_YYYY_MM_DD clickhouse_insert_format=RowBinary live_batch_mode=packet_worker_direct raw_spool_mode=failed_inserts_only failed_spool=%s udp_receive_mode=%s udp_receivers=%d udp_reuse_port=%t udp_reuseport_hash_offsets=%s udp_batch_size=%d udp_rxq_overflow_metrics=true packet_workers=%d packet_channel_size=%d batch_builders_ignored=%d insert_workers=%d insert_batch_rows=%d insert_batch_bytes=%d insert_flush_ms=%d udp_read_buffer_mb=%d auto_tune=%t auto_tune_max_packet_workers=%d auto_tune_max_batch_builders_ignored=%d auto_tune_max_insert_workers=%d live_insert_overload_policy=%s live_insert_queue_high_watermark_pct=%d alerts_enabled=%t alerts_mode=%s alert_window_seconds=%d alert_sla_seconds=%d parsed_json_spool=false accept_any_header=true dynamic_multi_record=true start_end_time=true raw_first_pipeline=false direct_live_batching=true graceful_shutdown=true",
		listenAddr,
		clickhouseURL,
		clickhouseTable,
		dailyTables,
		clickhouseTable,
		failedSpoolBase,
		udpReceiveMode,
		udpReceivers,
		udpReusePort,
		formatReusePortHashOffsets(udpReusePortHashOffsets),
		udpBatchSize,
		packetWorkers,
		packetChannelSize,
		batchBuilders,
		insertWorkers,
		insertBatchRows,
		insertBatchBytes,
		insertFlushMS,
		udpReadBufferMB,
		autoTuneEnabled,
		autoTuneMaxPacketWorkers,
		autoTuneMaxBatchBuilders,
		autoTuneMaxInsertWorkers,
		liveInsertOverloadPolicy,
		liveInsertQueueHighWatermarkPct,
		alertConfig.Enabled,
		alertConfig.Mode,
		alertConfig.WindowSeconds,
		alertConfig.SLASeconds,
	)

	readWG.Wait()
	log.Printf("collector_draining_start")
	autoTuneWG.Wait()
	close(packetChannel)
	packetWG.Wait()
	close(insertBatches)
	insertWG.Wait()
	alerts.Stop()
	log.Printf("collector_stopped")
}
