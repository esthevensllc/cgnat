package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const failedDataSuffix = ".rowbinary.done"

var errMetadataMissing = errors.New("metadata file is not available")

type failedInsertBatchMeta struct {
	SchemaVersion int    `json:"schema_version"`
	CreatedTime   string `json:"created_time"`
	TableName     string `json:"table_name"`
	Rows          int    `json:"rows"`
	Bytes         int    `json:"bytes"`
	Format        string `json:"format"`
	Error         string `json:"error"`
}

type candidate struct {
	dataPath string
	metaPath string
	modTime  time.Time
	meta     failedInsertBatchMeta
}

type invalidCandidate struct {
	dataPath string
	metaPath string
	err      error
}

type scanResult struct {
	candidates      []candidate
	invalid         []invalidCandidate
	incompletePairs int
	tooRecent       int
}

type cycleResult struct {
	discovered      int
	processed       int
	insertedRows    uint64
	failed          int
	quarantined     int
	incompletePairs int
	tooRecent       int
}

type processOptions struct {
	dryRun bool
	limit  int
}

type reprocessor struct {
	config config
	client *http.Client
	alerts *reprocessAlertManager

	tableMu      sync.Mutex
	knownTables  map[string]struct{}
	totalBatches uint64
	totalRows    uint64
	totalErrors  uint64
}

func newReprocessor(cfg config) *reprocessor {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          cfg.workers * 4,
		MaxIdleConnsPerHost:   cfg.workers * 2,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: cfg.httpTimeout,
		DisableCompression:    true,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.httpTimeout,
	}
	if !cfg.allowHTTPRedirect {
		client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}

	return &reprocessor{
		config:      cfg,
		client:      client,
		knownTables: make(map[string]struct{}),
	}
}

func (r *reprocessor) prepareDirectories() error {
	for _, directory := range []string{r.config.doneDir, r.config.archiveDir, r.config.badDir} {
		if err := os.MkdirAll(directory, 0750); err != nil {
			return fmt.Errorf("create directory %s: %w", directory, err)
		}
	}
	return nil
}

func (r *reprocessor) run(ctx context.Context, once bool, options processOptions) error {
	for {
		result, err := r.processCycle(ctx, options)
		alertMetrics := r.alerts.Metrics()
		log.Printf(
			"reprocessor_metrics discovered=%d processed=%d inserted_rows=%d failed=%d quarantined=%d incomplete_pairs=%d too_recent=%d total_batches_inserted=%d total_rows_inserted=%d total_errors=%d alerts_enabled=%t alerts_mode=%s alerts_active=%d total_alerts_opened=%d total_alerts_cleared=%d total_alerts_delivered=%d total_alert_delivery_errors=%d alert_outbox_pending=%d",
			result.discovered,
			result.processed,
			result.insertedRows,
			result.failed,
			result.quarantined,
			result.incompletePairs,
			result.tooRecent,
			r.totalBatches,
			r.totalRows,
			r.totalErrors,
			r.config.alerts.Enabled,
			r.config.alerts.Mode,
			alertMetrics.Active,
			alertMetrics.Opened,
			alertMetrics.Cleared,
			alertMetrics.Delivered,
			alertMetrics.DeliveryErrors,
			alertMetrics.OutboxPending,
		)
		if once {
			return err
		}
		if err != nil {
			log.Printf("reprocessor_cycle_error error=%v", err)
		}

		timer := time.NewTimer(r.config.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (r *reprocessor) processCycle(ctx context.Context, options processOptions) (cycleResult, error) {
	limit := r.config.scanLimit
	if options.limit > 0 && (limit == 0 || options.limit < limit) {
		limit = options.limit
	}

	scan, err := r.scan(time.Now(), limit)
	if err != nil {
		return cycleResult{}, err
	}

	result := cycleResult{
		discovered:      len(scan.candidates),
		incompletePairs: scan.incompletePairs,
		tooRecent:       scan.tooRecent,
	}

	for _, invalid := range scan.invalid {
		log.Printf("reprocess_invalid file=%s error=%v", invalid.dataPath, invalid.err)
		if options.dryRun {
			continue
		}
		if err := movePair(invalid.dataPath, invalid.metaPath, r.config.badDir); err != nil {
			result.failed++
			r.totalErrors++
			log.Printf("reprocess_quarantine_failed file=%s error=%v", invalid.dataPath, err)
			continue
		}
		result.quarantined++
		log.Printf("reprocess_quarantined file=%s destination=%s", invalid.dataPath, r.config.badDir)
	}

	if options.dryRun {
		for _, item := range scan.candidates {
			log.Printf(
				"reprocess_dry_run file=%s table=%s rows=%d bytes=%d original_error=%q",
				item.dataPath,
				item.meta.TableName,
				item.meta.Rows,
				item.meta.Bytes,
				item.meta.Error,
			)
		}
		return result, nil
	}

	type processResult struct {
		candidate candidate
		err       error
	}

	jobs := make(chan candidate)
	results := make(chan processResult)
	var workers sync.WaitGroup
	for workerID := 1; workerID <= r.config.workers; workerID++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			for item := range jobs {
				err := r.processCandidate(ctx, id, item)
				results <- processResult{candidate: item, err: err}
				if !sleepContext(ctx, r.config.interBatchDelay) {
					return
				}
			}
		}(workerID)
	}

	go func() {
		defer close(jobs)
		for _, item := range scan.candidates {
			select {
			case <-ctx.Done():
				return
			case jobs <- item:
			}
		}
	}()

	go func() {
		workers.Wait()
		close(results)
	}()

	for processed := range results {
		result.processed++
		if processed.err != nil {
			result.failed++
			r.totalErrors++
			log.Printf(
				"reprocess_failed file=%s table=%s rows=%d error=%v",
				processed.candidate.dataPath,
				processed.candidate.meta.TableName,
				processed.candidate.meta.Rows,
				processed.err,
			)
			continue
		}

		rows := uint64(processed.candidate.meta.Rows)
		result.insertedRows += rows
		r.totalBatches++
		r.totalRows += rows
	}

	if !options.dryRun {
		r.alerts.RecordCycle(time.Now(), result)
	}
	if result.failed > 0 {
		return result, fmt.Errorf("reprocess cycle completed with %d failed items", result.failed)
	}
	return result, nil
}

func (r *reprocessor) scan(now time.Time, limit int) (scanResult, error) {
	entries, err := os.ReadDir(r.config.doneDir)
	if err != nil {
		return scanResult{}, fmt.Errorf("read failed spool: %w", err)
	}

	result := scanResult{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), failedDataSuffix) {
			continue
		}

		dataPath := filepath.Join(r.config.doneDir, entry.Name())
		metaPath := dataPath + ".json"
		info, err := entry.Info()
		if err != nil {
			result.invalid = append(result.invalid, invalidCandidate{dataPath: dataPath, metaPath: metaPath, err: err})
			continue
		}
		if now.Sub(info.ModTime()) < r.config.minAge {
			result.tooRecent++
			continue
		}
		metaInfo, err := os.Stat(metaPath)
		if errors.Is(err, os.ErrNotExist) {
			result.incompletePairs++
			continue
		}
		if err != nil {
			result.invalid = append(result.invalid, invalidCandidate{dataPath: dataPath, metaPath: metaPath, err: err})
			continue
		}
		if now.Sub(metaInfo.ModTime()) < r.config.minAge {
			result.tooRecent++
			continue
		}

		item, err := loadCandidate(dataPath, metaPath, info.ModTime(), info.Size())
		if errors.Is(err, errMetadataMissing) {
			result.incompletePairs++
			continue
		}
		if err != nil {
			result.invalid = append(result.invalid, invalidCandidate{dataPath: dataPath, metaPath: metaPath, err: err})
			continue
		}
		result.candidates = append(result.candidates, item)
	}

	sort.Slice(result.candidates, func(i, j int) bool {
		return result.candidates[i].modTime.Before(result.candidates[j].modTime)
	})
	if limit > 0 && len(result.candidates) > limit {
		result.candidates = result.candidates[:limit]
	}
	return result, nil
}

func loadCandidate(dataPath, metaPath string, modTime time.Time, actualSize int64) (candidate, error) {
	metaJSON, err := os.ReadFile(metaPath)
	if errors.Is(err, os.ErrNotExist) {
		return candidate{}, errMetadataMissing
	}
	if err != nil {
		return candidate{}, fmt.Errorf("read metadata: %w", err)
	}

	var meta failedInsertBatchMeta
	if err := json.Unmarshal(metaJSON, &meta); err != nil {
		return candidate{}, fmt.Errorf("decode metadata: %w", err)
	}
	if !strings.EqualFold(meta.Format, "RowBinary") {
		return candidate{}, fmt.Errorf("unsupported format %q", meta.Format)
	}
	schemaVersion, err := normalizeRowBinarySchema(meta.SchemaVersion)
	if err != nil {
		return candidate{}, err
	}
	meta.SchemaVersion = schemaVersion
	if err := validateTableName(meta.TableName); err != nil {
		return candidate{}, err
	}
	if meta.Rows <= 0 {
		return candidate{}, fmt.Errorf("metadata rows must be greater than zero")
	}
	if meta.Bytes <= 0 {
		return candidate{}, fmt.Errorf("metadata bytes must be greater than zero")
	}
	if actualSize != int64(meta.Bytes) {
		return candidate{}, fmt.Errorf("file size mismatch actual=%d metadata=%d", actualSize, meta.Bytes)
	}

	return candidate{
		dataPath: dataPath,
		metaPath: metaPath,
		modTime:  modTime,
		meta:     meta,
	}, nil
}

func (r *reprocessor) processCandidate(ctx context.Context, workerID int, item candidate) error {
	var lastErr error
	for attempt := 1; attempt <= r.config.maxRetries; attempt++ {
		if err := r.ensureTable(ctx, item.meta.TableName, item.meta.SchemaVersion); err != nil {
			lastErr = fmt.Errorf("ensure table: %w", err)
		} else {
			lastErr = r.insertFile(ctx, item)
		}
		if lastErr == nil {
			if err := r.finalizeSuccess(item); err != nil {
				return fmt.Errorf("insert succeeded but finalization failed: %w", err)
			}
			log.Printf(
				"reprocess_inserted worker=%d file=%s table=%s rows=%d bytes=%d action=%s",
				workerID,
				item.dataPath,
				item.meta.TableName,
				item.meta.Rows,
				item.meta.Bytes,
				r.config.successAction,
			)
			return nil
		}

		log.Printf(
			"reprocess_retry worker=%d file=%s table=%s rows=%d attempt=%d/%d error=%v",
			workerID,
			item.dataPath,
			item.meta.TableName,
			item.meta.Rows,
			attempt,
			r.config.maxRetries,
			lastErr,
		)
		if attempt < r.config.maxRetries {
			if !sleepContext(ctx, time.Duration(attempt)*r.config.retryDelay) {
				return ctx.Err()
			}
		}
	}
	return lastErr
}

func (r *reprocessor) ensureTable(ctx context.Context, tableName string, schemaVersion int) error {
	if !r.config.autoCreateTable {
		return nil
	}

	r.tableMu.Lock()
	_, exists := r.knownTables[tableName]
	r.tableMu.Unlock()
	if exists {
		return nil
	}

	ddl, err := clickHouseCreateTableDDL(tableName, schemaVersion)
	if err != nil {
		return err
	}
	if err := r.executeQuery(ctx, ddl); err != nil {
		return err
	}

	r.tableMu.Lock()
	r.knownTables[tableName] = struct{}{}
	r.tableMu.Unlock()
	log.Printf("reprocess_table_ready table=%s", tableName)
	return nil
}

func (r *reprocessor) executeQuery(ctx context.Context, query string) error {
	endpoint, err := url.Parse(strings.TrimRight(r.config.clickHouseURL, "/") + "/")
	if err != nil {
		return err
	}
	values := endpoint.Query()
	values.Set("query", query)
	endpoint.RawQuery = values.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), nil)
	if err != nil {
		return err
	}
	request.Header.Set("Connection", "keep-alive")
	if r.config.clickHouseUser != "" {
		request.SetBasicAuth(r.config.clickHouseUser, r.config.clickHousePass)
	}

	return r.doRequest(request)
}

func (r *reprocessor) insertFile(ctx context.Context, item candidate) error {
	file, err := os.Open(item.dataPath)
	if err != nil {
		return err
	}
	defer file.Close()

	queryID := queryIDForFile(filepath.Base(item.dataPath))
	insertURL, err := clickHouseInsertURL(r.config.clickHouseURL, item.meta.TableName, item.meta.SchemaVersion, queryID)
	if err != nil {
		return err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, insertURL, file)
	if err != nil {
		return err
	}
	request.ContentLength = int64(item.meta.Bytes)
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Connection", "keep-alive")
	if r.config.clickHouseUser != "" {
		request.SetBasicAuth(r.config.clickHouseUser, r.config.clickHousePass)
	}

	return r.doRequest(request)
}

func (r *reprocessor) doRequest(request *http.Request) error {
	response, err := r.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
		return fmt.Errorf(
			"clickhouse_status=%d body=%s",
			response.StatusCode,
			strings.TrimSpace(string(body)),
		)
	}

	_, _ = io.Copy(io.Discard, response.Body)
	return nil
}

func (r *reprocessor) finalizeSuccess(item candidate) error {
	archivedData, archivedMeta, err := movePairWithDestinations(item.dataPath, item.metaPath, r.config.archiveDir)
	if err != nil {
		return err
	}
	if r.config.successAction == successActionArchive {
		return nil
	}

	if err := os.Remove(archivedData); err != nil {
		return fmt.Errorf("delete reprocessed data: %w", err)
	}
	if err := os.Remove(archivedMeta); err != nil {
		return fmt.Errorf("delete reprocessed metadata: %w", err)
	}
	return nil
}

func movePair(dataPath, metaPath, destinationDir string) error {
	_, _, err := movePairWithDestinations(dataPath, metaPath, destinationDir)
	return err
}

func movePairWithDestinations(dataPath, metaPath, destinationDir string) (string, string, error) {
	if err := os.MkdirAll(destinationDir, 0750); err != nil {
		return "", "", err
	}

	destinationData := filepath.Join(destinationDir, filepath.Base(dataPath))
	destinationMeta := filepath.Join(destinationDir, filepath.Base(metaPath))
	if _, err := os.Stat(destinationData); err == nil {
		return "", "", fmt.Errorf("destination already exists: %s", destinationData)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	if _, err := os.Stat(destinationMeta); err == nil {
		return "", "", fmt.Errorf("destination already exists: %s", destinationMeta)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}

	if err := os.Rename(dataPath, destinationData); err != nil {
		return "", "", fmt.Errorf("move data: %w", err)
	}
	if err := os.Rename(metaPath, destinationMeta); err != nil {
		rollbackErr := os.Rename(destinationData, dataPath)
		if rollbackErr != nil {
			return destinationData, "", fmt.Errorf("move metadata: %v; rollback data: %w", err, rollbackErr)
		}
		return "", "", fmt.Errorf("move metadata: %w", err)
	}

	return destinationData, destinationMeta, nil
}

func queryIDForFile(fileName string) string {
	digest := sha256.Sum256([]byte(fileName))
	return "cgn_failed_reprocess_" + hex.EncodeToString(digest[:12])
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		return true
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
