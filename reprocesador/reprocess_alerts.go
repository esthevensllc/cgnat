package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	reprocessAlertModeObserve    = "observe"
	reprocessAlertModeClickHouse = "clickhouse"

	reprocessAlertStateActive  = "ACTIVE"
	reprocessAlertStateCleared = "CLEARED"

	reprocessAlertBacklogName     = "CGN_REPROCESS_BACKLOG_SLA"
	reprocessAlertFailureRateName = "CGN_REPROCESS_FAILURE_RATE_60S"
	reprocessAlertQuarantineName  = "CGN_REPROCESS_QUARANTINE_RATE_60S"
)

// reprocessAlertConfig intentionally uses its own state directory. The
// collector and reprocessor can write to the same ClickHouse table, but their
// state and outbox files must never share a directory.
type reprocessAlertConfig struct {
	Enabled bool
	Mode    string

	ServerIP string
	StateDir string

	EvaluationInterval time.Duration
	Window             time.Duration
	BacklogSLA         time.Duration
	MinimumBatches     uint64
	TriggerWindows     int
	ClearWindows       int
	ActiveUpdate       time.Duration

	BacklogThresholdPct    float64
	BacklogClearPct        float64
	FailureThresholdPct    float64
	FailureClearPct        float64
	QuarantineThresholdPct float64
	QuarantineClearPct     float64

	ClickHouse reprocessClickHouseAlertConfig
}

type reprocessClickHouseAlertConfig struct {
	URL              string
	User             string
	Password         string
	Table            string
	AutoCreate       bool
	ConnectTimeout   time.Duration
	OperationTimeout time.Duration
	Retry            time.Duration
	MaxRetry         time.Duration
}

type reprocessAlertRecord struct {
	ID        string     `json:"id"`
	Hostname  string     `json:"hostname"`
	IP        string     `json:"ip"`
	StartTime time.Time  `json:"fecha_inicio"`
	EndTime   *time.Time `json:"fecha_fin,omitempty"`
	Name      string     `json:"nombre"`
	Threshold float64    `json:"umbral"`
	Indicator float64    `json:"porcentaje_indicador"`
	State     string     `json:"estado"`
}

type reprocessAlertState struct {
	Record         reprocessAlertRecord `json:"record"`
	Active         bool                 `json:"active"`
	BreachWindows  int                  `json:"breach_windows"`
	ClearWindows   int                  `json:"clear_windows"`
	LastEmitted    time.Time            `json:"last_emitted,omitempty"`
	DeliveryNeeded bool                 `json:"delivery_needed"`
}

type persistedReprocessAlertStates struct {
	Alerts map[string]*reprocessAlertState `json:"alerts"`
}

type reprocessAlertObservation struct {
	Name           string
	Threshold      float64
	ClearThreshold float64
	Indicator      float64
	Numerator      uint64
	Denominator    uint64
	Valid          bool
}

type reprocessCycleCounters struct {
	Attempted   uint64
	Failed      uint64
	Quarantined uint64
}

type reprocessAlertMetrics struct {
	Active         int64
	Opened         uint64
	Cleared        uint64
	Delivered      uint64
	DeliveryErrors uint64
	OutboxPending  int64
}

type reprocessAlertSink interface {
	Prepare(context.Context) error
	Upsert(context.Context, reprocessAlertRecord) error
	Close() error
}

type reprocessAlertManager struct {
	config      reprocessAlertConfig
	doneDir     string
	minimumAge  time.Duration
	hostname    string
	states      map[string]*reprocessAlertState
	statePath   string
	outboxDir   string
	badDir      string
	sink        reprocessAlertSink
	stop        chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
	counterMu   sync.Mutex
	counters    map[int64]reprocessCycleCounters
	startSecond int64

	active         int64
	opened         uint64
	cleared        uint64
	delivered      uint64
	deliveryErrors uint64
	outboxPending  int64
}

func loadReprocessAlertConfig(spoolBase, clickHouseURL, clickHouseUser, clickHousePass string) (reprocessAlertConfig, error) {
	enabled, err := getEnvBoolInherited("REPROCESS_ALERTS_ENABLED", "ALERTS_ENABLED", false)
	if err != nil {
		return reprocessAlertConfig{}, err
	}
	evaluationSeconds, err := getEnvIntInherited("REPROCESS_ALERT_EVALUATION_INTERVAL_SECONDS", "ALERT_EVALUATION_INTERVAL_SECONDS", 10)
	if err != nil {
		return reprocessAlertConfig{}, err
	}
	windowSeconds, err := getEnvIntInherited("REPROCESS_ALERT_WINDOW_SECONDS", "ALERT_WINDOW_SECONDS", 60)
	if err != nil {
		return reprocessAlertConfig{}, err
	}
	backlogSLASeconds, err := getEnvInt("REPROCESS_ALERT_BACKLOG_SLA_SECONDS", 300)
	if err != nil {
		return reprocessAlertConfig{}, err
	}
	minimumBatches, err := getEnvUint64("REPROCESS_ALERT_MIN_BATCHES", 1)
	if err != nil {
		return reprocessAlertConfig{}, err
	}
	triggerWindows, err := getEnvIntInherited("REPROCESS_ALERT_TRIGGER_WINDOWS", "ALERT_TRIGGER_WINDOWS", 1)
	if err != nil {
		return reprocessAlertConfig{}, err
	}
	clearWindows, err := getEnvIntInherited("REPROCESS_ALERT_CLEAR_WINDOWS", "ALERT_CLEAR_WINDOWS", 3)
	if err != nil {
		return reprocessAlertConfig{}, err
	}
	activeUpdateSeconds, err := getEnvIntInherited("REPROCESS_ALERT_ACTIVE_UPDATE_SECONDS", "ALERT_ACTIVE_UPDATE_SECONDS", 60)
	if err != nil {
		return reprocessAlertConfig{}, err
	}

	backlogThreshold, err := getEnvFloat("REPROCESS_ALERT_BACKLOG_THRESHOLD_PCT", 1)
	if err != nil {
		return reprocessAlertConfig{}, err
	}
	backlogClear, err := getEnvFloat("REPROCESS_ALERT_BACKLOG_CLEAR_PCT", 0.01)
	if err != nil {
		return reprocessAlertConfig{}, err
	}
	failureThreshold, err := getEnvFloat("REPROCESS_ALERT_FAILURE_THRESHOLD_PCT", 1)
	if err != nil {
		return reprocessAlertConfig{}, err
	}
	failureClear, err := getEnvFloat("REPROCESS_ALERT_FAILURE_CLEAR_PCT", 0.01)
	if err != nil {
		return reprocessAlertConfig{}, err
	}
	quarantineThreshold, err := getEnvFloat("REPROCESS_ALERT_QUARANTINE_THRESHOLD_PCT", 1)
	if err != nil {
		return reprocessAlertConfig{}, err
	}
	quarantineClear, err := getEnvFloat("REPROCESS_ALERT_QUARANTINE_CLEAR_PCT", 0.01)
	if err != nil {
		return reprocessAlertConfig{}, err
	}

	config := reprocessAlertConfig{
		Enabled:                enabled,
		Mode:                   strings.ToLower(getEnvInherited("REPROCESS_ALERTS_MODE", "ALERTS_MODE", reprocessAlertModeObserve)),
		ServerIP:               getEnvInherited("REPROCESS_ALERT_SERVER_IP", "ALERT_SERVER_IP", ""),
		StateDir:               getEnv("REPROCESS_ALERT_STATE_DIR", filepath.Join(spoolBase, "alerts")),
		EvaluationInterval:     time.Duration(evaluationSeconds) * time.Second,
		Window:                 time.Duration(windowSeconds) * time.Second,
		BacklogSLA:             time.Duration(backlogSLASeconds) * time.Second,
		MinimumBatches:         minimumBatches,
		TriggerWindows:         triggerWindows,
		ClearWindows:           clearWindows,
		ActiveUpdate:           time.Duration(activeUpdateSeconds) * time.Second,
		BacklogThresholdPct:    backlogThreshold,
		BacklogClearPct:        backlogClear,
		FailureThresholdPct:    failureThreshold,
		FailureClearPct:        failureClear,
		QuarantineThresholdPct: quarantineThreshold,
		QuarantineClearPct:     quarantineClear,
		ClickHouse: reprocessClickHouseAlertConfig{
			URL:              clickHouseURL,
			User:             clickHouseUser,
			Password:         clickHousePass,
			Table:            getEnv("REPROCESS_CLICKHOUSE_ALERT_TABLE", getEnv("CLICKHOUSE_ALERT_TABLE", "cgnat.collector_alerts")),
			AutoCreate:       getEnvBoolDefault("REPROCESS_CLICKHOUSE_ALERT_AUTO_CREATE", getEnvBoolDefault("CLICKHOUSE_ALERT_AUTO_CREATE", true)),
			ConnectTimeout:   time.Duration(getEnvIntDefault("CLICKHOUSE_ALERT_CONNECT_TIMEOUT_SECONDS", 5)) * time.Second,
			OperationTimeout: time.Duration(getEnvIntDefault("CLICKHOUSE_ALERT_OPERATION_TIMEOUT_SECONDS", 10)) * time.Second,
			Retry:            time.Duration(getEnvIntDefault("CLICKHOUSE_ALERT_RETRY_SECONDS", 5)) * time.Second,
			MaxRetry:         time.Duration(getEnvIntDefault("CLICKHOUSE_ALERT_MAX_RETRY_SECONDS", 300)) * time.Second,
		},
	}
	if err := config.validate(); err != nil {
		return reprocessAlertConfig{}, err
	}
	return config, nil
}

func (config reprocessAlertConfig) validate() error {
	if !config.Enabled {
		return nil
	}
	if config.Mode != reprocessAlertModeObserve && config.Mode != reprocessAlertModeClickHouse {
		return fmt.Errorf("REPROCESS_ALERTS_MODE must be observe or clickhouse")
	}
	if net.ParseIP(config.ServerIP) == nil {
		return fmt.Errorf("REPROCESS_ALERT_SERVER_IP or ALERT_SERVER_IP must contain a valid IPv4 or IPv6 address")
	}
	if config.StateDir == "" {
		return fmt.Errorf("REPROCESS_ALERT_STATE_DIR cannot be empty")
	}
	if config.EvaluationInterval <= 0 || config.Window <= 0 || config.BacklogSLA <= 0 {
		return fmt.Errorf("reprocess alert intervals must be greater than zero")
	}
	if config.MinimumBatches == 0 || config.TriggerWindows <= 0 || config.ClearWindows <= 0 || config.ActiveUpdate <= 0 {
		return fmt.Errorf("reprocess alert batch and window settings must be greater than zero")
	}
	for _, value := range []struct {
		name      string
		threshold float64
		clear     float64
	}{
		{"backlog", config.BacklogThresholdPct, config.BacklogClearPct},
		{"failure", config.FailureThresholdPct, config.FailureClearPct},
		{"quarantine", config.QuarantineThresholdPct, config.QuarantineClearPct},
	} {
		if math.IsNaN(value.threshold) || math.IsInf(value.threshold, 0) || value.threshold <= 0 || value.threshold > 100 {
			return fmt.Errorf("reprocess %s alert threshold must be greater than zero and at most 100", value.name)
		}
		if math.IsNaN(value.clear) || math.IsInf(value.clear, 0) || value.clear < 0 || value.clear >= value.threshold {
			return fmt.Errorf("reprocess %s alert clear percentage must be non-negative and lower than its threshold", value.name)
		}
	}
	if config.Mode == reprocessAlertModeClickHouse {
		return config.ClickHouse.validate()
	}
	return nil
}

func (config reprocessClickHouseAlertConfig) validate() error {
	endpoint, err := url.Parse(config.URL)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return fmt.Errorf("CLICKHOUSE_URL must contain a valid HTTP or HTTPS URL")
	}
	if config.ConnectTimeout <= 0 || config.OperationTimeout <= 0 || config.Retry <= 0 || config.MaxRetry < config.Retry {
		return fmt.Errorf("reprocess ClickHouse alert timeouts or retry values are invalid")
	}
	return validateTableName(config.Table)
}

func newReprocessAlertManager(config reprocessAlertConfig, doneDir string, minimumAge time.Duration) (*reprocessAlertManager, error) {
	if !config.Enabled {
		return nil, nil
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("reprocess_alert_hostname_error: %w", err)
	}
	manager := &reprocessAlertManager{
		config:      config,
		doneDir:     doneDir,
		minimumAge:  minimumAge,
		hostname:    hostname,
		states:      make(map[string]*reprocessAlertState),
		statePath:   filepath.Join(config.StateDir, "state.json"),
		outboxDir:   filepath.Join(config.StateDir, "outbox"),
		badDir:      filepath.Join(config.StateDir, "bad"),
		stop:        make(chan struct{}),
		counters:    make(map[int64]reprocessCycleCounters),
		startSecond: time.Now().Unix(),
	}
	for _, directory := range []string{config.StateDir, manager.outboxDir, manager.badDir} {
		if err := ensureWritableDirectory(directory, 0750); err != nil {
			return nil, fmt.Errorf("reprocess_alert_directory_error directory=%s: %w", directory, err)
		}
	}
	if err := manager.loadStates(); err != nil {
		return nil, err
	}
	if config.Mode == reprocessAlertModeClickHouse {
		manager.sink, err = newReprocessClickHouseAlertSink(config.ClickHouse)
		if err != nil {
			return nil, err
		}
	}
	return manager, nil
}

func (manager *reprocessAlertManager) Start() {
	if manager == nil {
		return
	}
	manager.startSecond = time.Now().Unix()
	if manager.config.Mode == reprocessAlertModeClickHouse {
		manager.wg.Add(1)
		go manager.outboxWorker()
	}
	manager.wg.Add(1)
	go manager.evaluator()
	log.Printf(
		"reprocess_alerting_started mode=%s server_ip=%s window_seconds=%.0f backlog_sla_seconds=%.0f backlog_threshold_pct=%g failure_threshold_pct=%g quarantine_threshold_pct=%g clickhouse_alert_table=%s state_dir=%s",
		manager.config.Mode,
		manager.config.ServerIP,
		manager.config.Window.Seconds(),
		manager.config.BacklogSLA.Seconds(),
		manager.config.BacklogThresholdPct,
		manager.config.FailureThresholdPct,
		manager.config.QuarantineThresholdPct,
		manager.config.ClickHouse.Table,
		manager.config.StateDir,
	)
}

func (manager *reprocessAlertManager) Stop() {
	if manager == nil {
		return
	}
	manager.stopOnce.Do(func() { close(manager.stop) })
	manager.wg.Wait()
	if manager.sink != nil {
		if err := manager.sink.Close(); err != nil {
			log.Printf("reprocess_alert_sink_close_error error=%v", err)
		}
	}
}

func (manager *reprocessAlertManager) RecordCycle(now time.Time, result cycleResult) {
	if manager == nil {
		return
	}
	second := now.Unix()
	manager.counterMu.Lock()
	counter := manager.counters[second]
	counter.Attempted += uint64(result.processed)
	counter.Failed += uint64(result.failed)
	counter.Quarantined += uint64(result.quarantined)
	manager.counters[second] = counter
	for key := range manager.counters {
		if key < second-int64(manager.config.Window.Seconds())-120 {
			delete(manager.counters, key)
		}
	}
	manager.counterMu.Unlock()
}

func (manager *reprocessAlertManager) Metrics() reprocessAlertMetrics {
	if manager == nil {
		return reprocessAlertMetrics{}
	}
	return reprocessAlertMetrics{
		Active:         atomic.LoadInt64(&manager.active),
		Opened:         atomic.LoadUint64(&manager.opened),
		Cleared:        atomic.LoadUint64(&manager.cleared),
		Delivered:      atomic.LoadUint64(&manager.delivered),
		DeliveryErrors: atomic.LoadUint64(&manager.deliveryErrors),
		OutboxPending:  atomic.LoadInt64(&manager.outboxPending),
	}
}

func (manager *reprocessAlertManager) evaluator() {
	defer manager.wg.Done()
	ticker := time.NewTicker(manager.config.EvaluationInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			manager.evaluate(now)
		case <-manager.stop:
			return
		}
	}
}

func (manager *reprocessAlertManager) evaluate(now time.Time) {
	observations, err := manager.observations(now)
	if err != nil {
		log.Printf("reprocess_alert_evaluation_error error=%v", err)
		return
	}
	for _, observation := range observations {
		if !observation.Valid {
			continue
		}
		if err := manager.applyObservation(observation, now); err != nil {
			log.Printf("reprocess_alert_evaluation_error name=%s error=%v", observation.Name, err)
		}
	}
}

func (manager *reprocessAlertManager) observations(now time.Time) ([]reprocessAlertObservation, error) {
	windowStart := now.Add(-manager.config.Window).Unix() + 1
	windowEnd := now.Unix()
	counters := manager.counterSnapshot(windowStart, windowEnd)

	eligible, overdue, err := manager.backlogSnapshot(now)
	if err != nil {
		return nil, err
	}
	backlog := reprocessAlertObservation{
		Name:           reprocessAlertBacklogName,
		Threshold:      manager.config.BacklogThresholdPct,
		ClearThreshold: manager.config.BacklogClearPct,
		Indicator:      reprocessPercentage(overdue, eligible),
		Numerator:      overdue,
		Denominator:    eligible,
		Valid:          manager.shouldEvaluate(reprocessAlertBacklogName, eligible),
	}
	failure := reprocessAlertObservation{
		Name:           reprocessAlertFailureRateName,
		Threshold:      manager.config.FailureThresholdPct,
		ClearThreshold: manager.config.FailureClearPct,
		Indicator:      reprocessPercentage(counters.Failed, counters.Attempted),
		Numerator:      counters.Failed,
		Denominator:    counters.Attempted,
		Valid:          manager.shouldEvaluate(reprocessAlertFailureRateName, counters.Attempted),
	}
	quarantineDenominator := counters.Attempted + counters.Quarantined
	quarantine := reprocessAlertObservation{
		Name:           reprocessAlertQuarantineName,
		Threshold:      manager.config.QuarantineThresholdPct,
		ClearThreshold: manager.config.QuarantineClearPct,
		Indicator:      reprocessPercentage(counters.Quarantined, quarantineDenominator),
		Numerator:      counters.Quarantined,
		Denominator:    quarantineDenominator,
		Valid:          manager.shouldEvaluate(reprocessAlertQuarantineName, quarantineDenominator),
	}
	return []reprocessAlertObservation{backlog, failure, quarantine}, nil
}

// An inactive alert requires enough data to open. An active alert must keep
// being evaluated with a zero indicator so it can clear after recovery.
func (manager *reprocessAlertManager) shouldEvaluate(name string, denominator uint64) bool {
	if denominator >= manager.config.MinimumBatches {
		return true
	}
	state := manager.states[name]
	return state != nil && state.Active
}

func (manager *reprocessAlertManager) counterSnapshot(start, end int64) reprocessCycleCounters {
	manager.counterMu.Lock()
	defer manager.counterMu.Unlock()
	var result reprocessCycleCounters
	for second, counter := range manager.counters {
		if second < start || second > end {
			continue
		}
		result.Attempted += counter.Attempted
		result.Failed += counter.Failed
		result.Quarantined += counter.Quarantined
	}
	return result
}

func (manager *reprocessAlertManager) backlogSnapshot(now time.Time) (uint64, uint64, error) {
	entries, err := os.ReadDir(manager.doneDir)
	if err != nil {
		return 0, 0, fmt.Errorf("read reprocess alert backlog: %w", err)
	}
	var eligible uint64
	var overdue uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), failedDataSuffix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return 0, 0, fmt.Errorf("stat reprocess alert backlog file=%s: %w", entry.Name(), err)
		}
		age := now.Sub(info.ModTime())
		if age < manager.minimumAge {
			continue
		}
		eligible++
		if age >= manager.config.BacklogSLA {
			overdue++
		}
	}
	return eligible, overdue, nil
}

func reprocessPercentage(numerator, denominator uint64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) * 100 / float64(denominator)
}

func (manager *reprocessAlertManager) applyObservation(observation reprocessAlertObservation, now time.Time) error {
	state := manager.states[observation.Name]
	if state == nil {
		state = &reprocessAlertState{}
		manager.states[observation.Name] = state
	}
	if !state.Active {
		state.ClearWindows = 0
		if observation.Indicator < observation.Threshold {
			state.BreachWindows = 0
			return nil
		}
		state.BreachWindows++
		if state.BreachWindows < manager.config.TriggerWindows {
			return nil
		}
		id, err := newReprocessAlertID()
		if err != nil {
			return err
		}
		state.Record = reprocessAlertRecord{
			ID:        id,
			Hostname:  manager.hostname,
			IP:        manager.config.ServerIP,
			StartTime: now,
			Name:      observation.Name,
			Threshold: observation.Threshold,
			Indicator: observation.Indicator,
			State:     reprocessAlertStateActive,
		}
		state.Active = true
		state.BreachWindows = 0
		state.LastEmitted = now
		state.DeliveryNeeded = true
		atomic.AddInt64(&manager.active, 1)
		atomic.AddUint64(&manager.opened, 1)
		if err := manager.persistStates(); err != nil {
			return err
		}
		err = manager.emit(state.Record)
		if err == nil {
			state.DeliveryNeeded = false
			if persistErr := manager.persistStates(); persistErr != nil {
				return persistErr
			}
		}
		log.Printf("reprocess_alert_transition name=%s state=%s id=%s indicator_pct=%.6f threshold_pct=%.6f numerator=%d denominator=%d", observation.Name, reprocessAlertStateActive, state.Record.ID, observation.Indicator, observation.Threshold, observation.Numerator, observation.Denominator)
		return err
	}

	state.BreachWindows = 0
	if observation.Indicator > state.Record.Indicator {
		state.Record.Indicator = observation.Indicator
		state.DeliveryNeeded = true
	}
	if observation.Indicator < observation.ClearThreshold {
		state.ClearWindows++
	} else {
		state.ClearWindows = 0
	}
	if state.ClearWindows >= manager.config.ClearWindows {
		state.Active = false
		state.ClearWindows = 0
		state.Record.State = reprocessAlertStateCleared
		state.Record.EndTime = &now
		state.LastEmitted = now
		state.DeliveryNeeded = true
		atomic.AddInt64(&manager.active, -1)
		atomic.AddUint64(&manager.cleared, 1)
		if err := manager.persistStates(); err != nil {
			return err
		}
		err := manager.emit(state.Record)
		if err == nil {
			state.DeliveryNeeded = false
			if persistErr := manager.persistStates(); persistErr != nil {
				return persistErr
			}
		}
		log.Printf("reprocess_alert_transition name=%s state=%s id=%s peak_indicator_pct=%.6f recovery_indicator_pct=%.6f clear_threshold_pct=%.6f numerator=%d denominator=%d", observation.Name, reprocessAlertStateCleared, state.Record.ID, state.Record.Indicator, observation.Indicator, observation.ClearThreshold, observation.Numerator, observation.Denominator)
		return err
	}
	if state.DeliveryNeeded && now.Sub(state.LastEmitted) >= manager.config.ActiveUpdate {
		state.LastEmitted = now
		if err := manager.persistStates(); err != nil {
			return err
		}
		err := manager.emit(state.Record)
		if err == nil {
			state.DeliveryNeeded = false
			if persistErr := manager.persistStates(); persistErr != nil {
				return persistErr
			}
		}
		return err
	}
	return nil
}

func (manager *reprocessAlertManager) emit(record reprocessAlertRecord) error {
	if manager.config.Mode == reprocessAlertModeObserve {
		log.Printf("reprocess_alert_observe name=%s state=%s id=%s threshold_pct=%.6f indicator_pct=%.6f", record.Name, record.State, record.ID, record.Threshold, record.Indicator)
		return nil
	}
	if err := manager.enqueue(record); err != nil {
		atomic.AddUint64(&manager.deliveryErrors, 1)
		return err
	}
	return nil
}

func (manager *reprocessAlertManager) persistStates() error {
	payload, err := json.MarshalIndent(persistedReprocessAlertStates{Alerts: manager.states}, "", "  ")
	if err != nil {
		return err
	}
	return writeReprocessAlertFile(manager.statePath, append(payload, '\n'))
}

func (manager *reprocessAlertManager) loadStates() error {
	payload, err := os.ReadFile(manager.statePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reprocess_alert_state_read_error: %w", err)
	}
	var persisted persistedReprocessAlertStates
	if err := json.Unmarshal(payload, &persisted); err != nil {
		return fmt.Errorf("reprocess_alert_state_decode_error: %w", err)
	}
	if persisted.Alerts != nil {
		manager.states = persisted.Alerts
	}
	var active int64
	for _, state := range manager.states {
		if state.Active {
			active++
		}
	}
	atomic.StoreInt64(&manager.active, active)
	return nil
}

func (manager *reprocessAlertManager) enqueue(record reprocessAlertRecord) error {
	filename := fmt.Sprintf("%020d_%s_%s.json", time.Now().UnixNano(), safeReprocessAlertFileToken(record.Name), record.ID)
	payload, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	if err := writeReprocessAlertFile(filepath.Join(manager.outboxDir, filename), append(payload, '\n')); err != nil {
		return err
	}
	manager.refreshOutboxPending()
	return nil
}

func writeReprocessAlertFile(path string, payload []byte) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".reprocess-alert-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0640); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func (manager *reprocessAlertManager) outboxWorker() {
	defer manager.wg.Done()
	retry := manager.config.ClickHouse.Retry
	prepared := false
	for {
		if !prepared {
			ctx, cancel := context.WithTimeout(context.Background(), manager.config.ClickHouse.OperationTimeout)
			err := manager.sink.Prepare(ctx)
			cancel()
			if err != nil {
				atomic.AddUint64(&manager.deliveryErrors, 1)
				log.Printf("reprocess_alert_clickhouse_prepare_error table=%s error=%v", manager.config.ClickHouse.Table, err)
				if !waitForReprocessAlert(manager.stop, retry) {
					return
				}
				retry *= 2
				if retry > manager.config.ClickHouse.MaxRetry {
					retry = manager.config.ClickHouse.MaxRetry
				}
				continue
			}
			prepared = true
			retry = manager.config.ClickHouse.Retry
			log.Printf("reprocess_alert_clickhouse_ready table=%s auto_create=%t", manager.config.ClickHouse.Table, manager.config.ClickHouse.AutoCreate)
		}
		delivered, failed := manager.deliverOutbox()
		if failed {
			if !waitForReprocessAlert(manager.stop, retry) {
				return
			}
			retry *= 2
			if retry > manager.config.ClickHouse.MaxRetry {
				retry = manager.config.ClickHouse.MaxRetry
			}
			continue
		}
		retry = manager.config.ClickHouse.Retry
		wait := 5 * time.Second
		if delivered {
			wait = time.Second
		}
		if !waitForReprocessAlert(manager.stop, wait) {
			return
		}
	}
}

func waitForReprocessAlert(stop <-chan struct{}, wait time.Duration) bool {
	select {
	case <-time.After(wait):
		return true
	case <-stop:
		return false
	}
}

func (manager *reprocessAlertManager) deliverOutbox() (bool, bool) {
	entries, err := os.ReadDir(manager.outboxDir)
	if err != nil {
		atomic.AddUint64(&manager.deliveryErrors, 1)
		return false, true
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			files = append(files, entry.Name())
		}
	}
	sort.Strings(files)
	atomic.StoreInt64(&manager.outboxPending, int64(len(files)))
	delivered := false
	for _, filename := range files {
		path := filepath.Join(manager.outboxDir, filename)
		payload, err := os.ReadFile(path)
		if err != nil {
			atomic.AddUint64(&manager.deliveryErrors, 1)
			return delivered, true
		}
		var record reprocessAlertRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			atomic.AddUint64(&manager.deliveryErrors, 1)
			if renameErr := os.Rename(path, filepath.Join(manager.badDir, filename+".bad")); renameErr != nil {
				return delivered, true
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), manager.config.ClickHouse.OperationTimeout)
		err = manager.sink.Upsert(ctx, record)
		cancel()
		if err != nil {
			atomic.AddUint64(&manager.deliveryErrors, 1)
			log.Printf("reprocess_alert_clickhouse_delivery_error name=%s state=%s id=%s error=%v", record.Name, record.State, record.ID, err)
			return delivered, true
		}
		if err := os.Remove(path); err != nil {
			atomic.AddUint64(&manager.deliveryErrors, 1)
			return delivered, true
		}
		delivered = true
		atomic.AddUint64(&manager.delivered, 1)
	}
	manager.refreshOutboxPending()
	return delivered, false
}

func (manager *reprocessAlertManager) refreshOutboxPending() {
	entries, err := os.ReadDir(manager.outboxDir)
	if err != nil {
		return
	}
	var count int64
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			count++
		}
	}
	atomic.StoreInt64(&manager.outboxPending, count)
}

func newReprocessAlertID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(raw[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32], nil
}

func safeReprocessAlertFileToken(value string) string {
	var builder strings.Builder
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '_' || character == '-' {
			builder.WriteRune(character)
		} else {
			builder.WriteByte('_')
		}
	}
	return builder.String()
}

func newReprocessClickHouseAlertSink(config reprocessClickHouseAlertConfig) (*reprocessClickHouseAlertSink, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   config.ConnectTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          2,
		MaxIdleConnsPerHost:   1,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: config.OperationTimeout,
		DisableCompression:    true,
	}
	return &reprocessClickHouseAlertSink{config: config, client: &http.Client{Transport: transport}}, nil
}

type reprocessClickHouseAlertSink struct {
	config   reprocessClickHouseAlertConfig
	client   *http.Client
	prepare  sync.Mutex
	prepared bool
}

func (sink *reprocessClickHouseAlertSink) Prepare(ctx context.Context) error {
	if !sink.config.AutoCreate {
		return nil
	}
	sink.prepare.Lock()
	defer sink.prepare.Unlock()
	if sink.prepared {
		return nil
	}
	if err := sink.execute(ctx, reprocessClickHouseAlertCreateTableDDL(sink.config.Table), nil, "text/plain"); err != nil {
		return fmt.Errorf("reprocess_alert_create_table table=%s: %w", sink.config.Table, err)
	}
	sink.prepared = true
	return nil
}

func (sink *reprocessClickHouseAlertSink) Upsert(ctx context.Context, record reprocessAlertRecord) error {
	if err := sink.Prepare(ctx); err != nil {
		return err
	}
	now := time.Now().In(time.FixedZone("PET", -5*60*60))
	row := struct {
		ID        string  `json:"id"`
		Hostname  string  `json:"hostname"`
		IP        string  `json:"ip"`
		StartTime string  `json:"fecha_inicio"`
		EndTime   *string `json:"fecha_fin"`
		Name      string  `json:"nombre"`
		Threshold float64 `json:"umbral"`
		Indicator float64 `json:"porcentaje_indicador"`
		State     string  `json:"estado"`
		SentTime  string  `json:"fecha_envio"`
		Version   uint64  `json:"version"`
	}{
		ID:        record.ID,
		Hostname:  record.Hostname,
		IP:        record.IP,
		StartTime: formatReprocessAlertTime(record.StartTime),
		Name:      record.Name,
		Threshold: record.Threshold,
		Indicator: record.Indicator,
		State:     record.State,
		SentTime:  formatReprocessAlertTime(now),
		Version:   uint64(now.UnixNano()),
	}
	if record.EndTime != nil {
		formatted := formatReprocessAlertTime(*record.EndTime)
		row.EndTime = &formatted
	}
	payload, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("reprocess_alert_encode_error: %w", err)
	}
	payload = append(payload, '\n')
	if err := sink.execute(ctx, reprocessClickHouseAlertInsertQuery(sink.config.Table), payload, "application/x-ndjson"); err != nil {
		return fmt.Errorf("reprocess_alert_insert_error table=%s: %w", sink.config.Table, err)
	}
	return nil
}

func (sink *reprocessClickHouseAlertSink) execute(ctx context.Context, query string, body []byte, contentType string) error {
	endpoint, err := url.Parse(sink.config.URL)
	if err != nil {
		return err
	}
	if endpoint.Path == "" {
		endpoint.Path = "/"
	}
	parameters := endpoint.Query()
	parameters.Set("query", query)
	parameters.Set("date_time_input_format", "best_effort")
	endpoint.RawQuery = parameters.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("Connection", "keep-alive")
	if sink.config.User != "" {
		request.SetBasicAuth(sink.config.User, sink.config.Password)
	}
	response, err := sink.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
		return fmt.Errorf("clickhouse_status=%d body=%s", response.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	_, _ = io.Copy(io.Discard, response.Body)
	return nil
}

func (sink *reprocessClickHouseAlertSink) Close() error {
	if transport, ok := sink.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	return nil
}

func reprocessClickHouseAlertCreateTableDDL(tableName string) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s
(
    id UUID,
    hostname LowCardinality(String),
    ip String,
    fecha_inicio DateTime64(6, 'America/Lima'),
    fecha_fin Nullable(DateTime64(6, 'America/Lima')),
    nombre LowCardinality(String),
    umbral Float64,
    porcentaje_indicador Float64,
    estado LowCardinality(String),
    fecha_envio DateTime64(6, 'America/Lima'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(fecha_inicio)
ORDER BY id
SETTINGS index_granularity = 8192`, tableName)
}

func reprocessClickHouseAlertInsertQuery(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s (id, hostname, ip, fecha_inicio, fecha_fin, nombre, umbral, porcentaje_indicador, estado, fecha_envio, version) FORMAT JSONEachRow", tableName)
}

func formatReprocessAlertTime(value time.Time) string {
	return value.In(time.FixedZone("PET", -5*60*60)).Format("2006-01-02 15:04:05.000000")
}

func getEnvUint64(key string, fallback uint64) (uint64, error) {
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an unsigned integer: %w", key, err)
	}
	return parsed, nil
}

func getEnvFloat(key string, fallback float64) (float64, error) {
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number: %w", key, err)
	}
	return parsed, nil
}

func getEnvInherited(primary, inherited, fallback string) string {
	if value, ok := os.LookupEnv(primary); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return getEnv(inherited, fallback)
}

func getEnvBoolInherited(primary, inherited string, fallback bool) (bool, error) {
	if value, ok := os.LookupEnv(primary); ok && strings.TrimSpace(value) != "" {
		parsed, err := strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return false, fmt.Errorf("%s must be true or false: %w", primary, err)
		}
		return parsed, nil
	}
	return getEnvBool(inherited, fallback)
}

func getEnvIntInherited(primary, inherited string, fallback int) (int, error) {
	if value, ok := os.LookupEnv(primary); ok && strings.TrimSpace(value) != "" {
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return 0, fmt.Errorf("%s must be an integer: %w", primary, err)
		}
		return parsed, nil
	}
	return getEnvInt(inherited, fallback)
}

func getEnvBoolDefault(key string, fallback bool) bool {
	value, err := getEnvBool(key, fallback)
	if err != nil {
		return fallback
	}
	return value
}

func getEnvIntDefault(key string, fallback int) int {
	value, err := getEnvInt(key, fallback)
	if err != nil {
		return fallback
	}
	return value
}
