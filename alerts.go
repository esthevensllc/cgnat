package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
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
	alertModeObserve = "observe"
	alertModeOracle  = "oracle"

	alertStateActive  = "ACTIVE"
	alertStateCleared = "CLEARED"

	alertUDPDropRateName = "CGN_UDP_DROP_RATE_60S"
	alertParseSLAName    = "CGN_PARSE_SLA_60S"
	alertInsertSLAName   = "CGN_INSERT_SLA_60S"
)

type AlertConfig struct {
	Enabled bool
	Mode    string

	ServerIP string
	StateDir string

	EvaluationIntervalSeconds int
	WindowSeconds             int
	SLASeconds                int
	MinimumPackets            uint64
	MinimumRows               uint64
	TriggerWindows            int
	ClearWindows              int
	ActiveUpdateSeconds       int

	UDPThresholdPct    float64
	UDPClearPct        float64
	ParseThresholdPct  float64
	ParseClearPct      float64
	InsertThresholdPct float64
	InsertClearPct     float64

	Oracle OracleAlertConfig
}

type AlertRecord struct {
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

type alertRuntimeState struct {
	Record AlertRecord `json:"record"`
	Active bool        `json:"active"`

	BreachWindows int       `json:"breach_windows"`
	ClearWindows  int       `json:"clear_windows"`
	LastEmitted   time.Time `json:"last_emitted,omitempty"`
	Dirty         bool      `json:"dirty"`
}

type persistedAlertStates struct {
	Alerts map[string]*alertRuntimeState `json:"alerts"`
}

type alertObservation struct {
	Name           string
	Threshold      float64
	ClearThreshold float64
	Indicator      float64
	Numerator      uint64
	Denominator    uint64
	Valid          bool
}

type alertSink interface {
	Upsert(context.Context, AlertRecord) error
	Close() error
}

type alertManager struct {
	config      AlertConfig
	tracker     *cohortTracker
	hostname    string
	startSecond int64
	states      map[string]*alertRuntimeState
	statePath   string
	outboxDir   string
	badDir      string
	sink        alertSink
	stop        chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
}

var (
	alertConfig  AlertConfig
	alertCohorts *cohortTracker

	totalUDPKernelDrops    uint64
	totalAlertsOpened      uint64
	totalAlertsCleared     uint64
	totalAlertsDelivered   uint64
	totalAlertDeliveryErrs uint64
	activeAlerts           int64
	alertOutboxPending     int64
)

func getEnvFloat(key string, defaultValue float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	number, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Printf("invalid_env_float key=%s value=%q default=%g", key, value, defaultValue)
		return defaultValue
	}
	return number
}

func getEnvUint64(key string, defaultValue uint64) uint64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	number, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		log.Printf("invalid_env_uint key=%s value=%q default=%d", key, value, defaultValue)
		return defaultValue
	}
	return number
}

func loadAlertConfig(defaultBaseDir string) AlertConfig {
	return AlertConfig{
		Enabled: getEnvBool("ALERTS_ENABLED", false),
		Mode:    strings.ToLower(getEnv("ALERTS_MODE", alertModeObserve)),

		ServerIP: getEnv("ALERT_SERVER_IP", ""),
		StateDir: getEnv("ALERT_STATE_DIR", filepath.Join(defaultBaseDir, "alerts")),

		EvaluationIntervalSeconds: getEnvInt("ALERT_EVALUATION_INTERVAL_SECONDS", 10),
		WindowSeconds:             getEnvInt("ALERT_WINDOW_SECONDS", 60),
		SLASeconds:                getEnvInt("ALERT_SLA_SECONDS", 60),
		MinimumPackets:            getEnvUint64("ALERT_MIN_PACKETS", 1000),
		MinimumRows:               getEnvUint64("ALERT_MIN_ROWS", 1000),
		TriggerWindows:            getEnvInt("ALERT_TRIGGER_WINDOWS", 1),
		ClearWindows:              getEnvInt("ALERT_CLEAR_WINDOWS", 3),
		ActiveUpdateSeconds:       getEnvInt("ALERT_ACTIVE_UPDATE_SECONDS", 60),

		UDPThresholdPct:    getEnvFloat("ALERT_UDP_DROP_THRESHOLD_PCT", 0.1),
		UDPClearPct:        getEnvFloat("ALERT_UDP_DROP_CLEAR_PCT", 0.01),
		ParseThresholdPct:  getEnvFloat("ALERT_PARSE_THRESHOLD_PCT", 1.0),
		ParseClearPct:      getEnvFloat("ALERT_PARSE_CLEAR_PCT", 0.1),
		InsertThresholdPct: getEnvFloat("ALERT_INSERT_THRESHOLD_PCT", 1.0),
		InsertClearPct:     getEnvFloat("ALERT_INSERT_CLEAR_PCT", 0.1),

		Oracle: loadOracleAlertConfig(),
	}
}

func (config AlertConfig) validate() error {
	if !config.Enabled {
		return nil
	}
	if config.Mode != alertModeObserve && config.Mode != alertModeOracle {
		return fmt.Errorf("ALERTS_MODE must be observe or oracle")
	}
	if net.ParseIP(config.ServerIP) == nil {
		return fmt.Errorf("ALERT_SERVER_IP must contain a valid IPv4 or IPv6 address")
	}
	if config.StateDir == "" {
		return fmt.Errorf("ALERT_STATE_DIR cannot be empty")
	}
	if config.EvaluationIntervalSeconds <= 0 || config.WindowSeconds <= 0 || config.SLASeconds <= 0 {
		return fmt.Errorf("alert evaluation, window, and SLA seconds must be greater than zero")
	}
	if config.MinimumPackets == 0 || config.MinimumRows == 0 {
		return fmt.Errorf("ALERT_MIN_PACKETS and ALERT_MIN_ROWS must be greater than zero")
	}
	if config.TriggerWindows <= 0 || config.ClearWindows <= 0 || config.ActiveUpdateSeconds <= 0 {
		return fmt.Errorf("alert trigger, clear, and active update values must be greater than zero")
	}

	thresholds := []struct {
		name      string
		threshold float64
		clear     float64
	}{
		{"UDP", config.UDPThresholdPct, config.UDPClearPct},
		{"parse", config.ParseThresholdPct, config.ParseClearPct},
		{"insert", config.InsertThresholdPct, config.InsertClearPct},
	}
	for _, value := range thresholds {
		if math.IsNaN(value.threshold) || math.IsInf(value.threshold, 0) || value.threshold <= 0 || value.threshold > 100 {
			return fmt.Errorf("%s alert threshold must be greater than zero and at most 100", value.name)
		}
		if math.IsNaN(value.clear) || math.IsInf(value.clear, 0) || value.clear < 0 || value.clear >= value.threshold {
			return fmt.Errorf("%s alert clear percentage must be non-negative and lower than its threshold", value.name)
		}
	}
	if config.Mode == alertModeOracle {
		return config.Oracle.validate()
	}
	return nil
}

func alertCohortRetention(config AlertConfig) int {
	retention := config.WindowSeconds + config.SLASeconds + 120
	if retention < minimumCohortRetentionSeconds {
		return minimumCohortRetentionSeconds
	}
	return retention
}

func newAlertManager(config AlertConfig, tracker *cohortTracker) (*alertManager, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if !config.Enabled {
		return nil, nil
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("alert_hostname_error: %w", err)
	}

	manager := &alertManager{
		config:      config,
		tracker:     tracker,
		hostname:    hostname,
		startSecond: time.Now().Unix(),
		states:      make(map[string]*alertRuntimeState),
		statePath:   filepath.Join(config.StateDir, "state.json"),
		outboxDir:   filepath.Join(config.StateDir, "outbox"),
		badDir:      filepath.Join(config.StateDir, "bad"),
		stop:        make(chan struct{}),
	}

	for _, directory := range []string{config.StateDir, manager.outboxDir, manager.badDir} {
		if err := os.MkdirAll(directory, 0750); err != nil {
			return nil, fmt.Errorf("alert_directory_error directory=%s: %w", directory, err)
		}
	}
	if err := manager.loadStates(); err != nil {
		return nil, err
	}
	if config.Mode == alertModeOracle {
		manager.sink, err = newOracleAlertSink(config.Oracle)
		if err != nil {
			return nil, err
		}
	}
	return manager, nil
}

func (manager *alertManager) Start() {
	if manager == nil {
		return
	}
	manager.startSecond = time.Now().Unix()

	statesChanged := false
	for _, state := range manager.states {
		if !state.Dirty && !(manager.config.Mode == alertModeOracle && state.Active) {
			continue
		}
		if err := manager.emit(state.Record); err != nil {
			state.Dirty = true
			statesChanged = true
			continue
		}
		if state.Dirty {
			state.Dirty = false
			statesChanged = true
		}
	}
	if statesChanged {
		if err := manager.persistStates(); err != nil {
			log.Printf("alert_state_reconcile_error error=%v", err)
		}
	}

	if manager.config.Mode == alertModeOracle {
		manager.wg.Add(1)
		go manager.outboxWorker()
	}

	manager.wg.Add(1)
	go manager.evaluator()
	log.Printf(
		"alerting_started mode=%s server_ip=%s window_seconds=%d sla_seconds=%d udp_threshold_pct=%g parse_threshold_pct=%g insert_threshold_pct=%g state_dir=%s",
		manager.config.Mode,
		manager.config.ServerIP,
		manager.config.WindowSeconds,
		manager.config.SLASeconds,
		manager.config.UDPThresholdPct,
		manager.config.ParseThresholdPct,
		manager.config.InsertThresholdPct,
		manager.config.StateDir,
	)
}

func (manager *alertManager) Stop() {
	if manager == nil {
		return
	}
	manager.stopOnce.Do(func() { close(manager.stop) })
	manager.wg.Wait()
	if manager.sink != nil {
		if err := manager.sink.Close(); err != nil {
			log.Printf("alert_oracle_close_error error=%v", err)
		}
	}
}

func (manager *alertManager) evaluator() {
	defer manager.wg.Done()
	ticker := time.NewTicker(time.Duration(manager.config.EvaluationIntervalSeconds) * time.Second)
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

func (manager *alertManager) evaluate(now time.Time) {
	for _, observation := range manager.observations(now.Unix()) {
		if !observation.Valid {
			continue
		}
		if err := manager.applyObservation(observation, now.In(peruTZ)); err != nil {
			log.Printf("alert_evaluation_error name=%s error=%v", observation.Name, err)
		}
	}
}

func (manager *alertManager) observations(nowSecond int64) []alertObservation {
	window := int64(manager.config.WindowSeconds)
	sla := int64(manager.config.SLASeconds)
	result := make([]alertObservation, 0, 3)

	udpEnd := nowSecond - 1
	udpStart := udpEnd - window + 1
	udpSnapshot := manager.tracker.snapshot(udpStart, udpEnd)
	udpDenominator := udpSnapshot.ReceivedPackets + udpSnapshot.UDPKernelDrops
	result = append(result, alertObservation{
		Name:           alertUDPDropRateName,
		Threshold:      manager.config.UDPThresholdPct,
		ClearThreshold: manager.config.UDPClearPct,
		Indicator:      percentage(udpSnapshot.UDPKernelDrops, udpDenominator),
		Numerator:      udpSnapshot.UDPKernelDrops,
		Denominator:    udpDenominator,
		Valid:          udpStart >= manager.startSecond && udpDenominator >= manager.config.MinimumPackets,
	})

	delayedEnd := nowSecond - sla - 1
	delayedStart := delayedEnd - window + 1
	delayed := manager.tracker.snapshot(delayedStart, delayedEnd)
	unparsed := saturatingDifference(delayed.ReceivedPackets, delayed.ParsedPackets)
	result = append(result, alertObservation{
		Name:           alertParseSLAName,
		Threshold:      manager.config.ParseThresholdPct,
		ClearThreshold: manager.config.ParseClearPct,
		Indicator:      percentage(unparsed, delayed.ReceivedPackets),
		Numerator:      unparsed,
		Denominator:    delayed.ReceivedPackets,
		Valid:          delayedStart >= manager.startSecond && delayed.ReceivedPackets >= manager.config.MinimumPackets,
	})

	uninserted := saturatingDifference(delayed.ExpectedRows, delayed.InsertedRows)
	result = append(result, alertObservation{
		Name:           alertInsertSLAName,
		Threshold:      manager.config.InsertThresholdPct,
		ClearThreshold: manager.config.InsertClearPct,
		Indicator:      percentage(uninserted, delayed.ExpectedRows),
		Numerator:      uninserted,
		Denominator:    delayed.ExpectedRows,
		Valid:          delayedStart >= manager.startSecond && delayed.ExpectedRows >= manager.config.MinimumRows,
	})
	return result
}

func percentage(numerator uint64, denominator uint64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) * 100 / float64(denominator)
}

func (manager *alertManager) applyObservation(observation alertObservation, now time.Time) error {
	state := manager.states[observation.Name]
	if state == nil {
		state = &alertRuntimeState{}
		manager.states[observation.Name] = state
	}

	if !state.Active {
		if state.Dirty && now.Sub(state.LastEmitted) >= time.Duration(manager.config.ActiveUpdateSeconds)*time.Second {
			state.LastEmitted = now
			if err := manager.persistStates(); err != nil {
				return err
			}
			if err := manager.emit(state.Record); err != nil {
				return err
			}
			state.Dirty = false
			if err := manager.persistStates(); err != nil {
				return err
			}
		}
		state.ClearWindows = 0
		if observation.Indicator < observation.Threshold {
			state.BreachWindows = 0
			return nil
		}
		state.BreachWindows++
		if state.BreachWindows < manager.config.TriggerWindows {
			return nil
		}

		id, err := newAlertID()
		if err != nil {
			return err
		}
		state.Record = AlertRecord{
			ID:        id,
			Hostname:  manager.hostname,
			IP:        manager.config.ServerIP,
			StartTime: now,
			Name:      observation.Name,
			Threshold: observation.Threshold,
			Indicator: observation.Indicator,
			State:     alertStateActive,
		}
		state.Active = true
		state.BreachWindows = 0
		state.LastEmitted = now
		state.Dirty = true
		atomic.AddInt64(&activeAlerts, 1)
		atomic.AddUint64(&totalAlertsOpened, 1)
		if err := manager.persistStates(); err != nil {
			return err
		}
		emitErr := manager.emit(state.Record)
		if emitErr == nil {
			state.Dirty = false
			if err := manager.persistStates(); err != nil {
				return err
			}
		}
		log.Printf(
			"alert_transition name=%s state=%s id=%s indicator_pct=%.6f threshold_pct=%.6f numerator=%d denominator=%d",
			observation.Name,
			alertStateActive,
			state.Record.ID,
			observation.Indicator,
			observation.Threshold,
			observation.Numerator,
			observation.Denominator,
		)
		return emitErr
	}

	state.BreachWindows = 0
	if observation.Indicator > state.Record.Indicator {
		state.Record.Indicator = observation.Indicator
		state.Dirty = true
	}

	if observation.Indicator < observation.ClearThreshold {
		state.ClearWindows++
	} else {
		state.ClearWindows = 0
	}
	if state.ClearWindows >= manager.config.ClearWindows {
		state.Active = false
		state.ClearWindows = 0
		state.Record.State = alertStateCleared
		state.Record.EndTime = &now
		state.LastEmitted = now
		state.Dirty = true
		atomic.AddInt64(&activeAlerts, -1)
		atomic.AddUint64(&totalAlertsCleared, 1)
		if err := manager.persistStates(); err != nil {
			return err
		}
		emitErr := manager.emit(state.Record)
		if emitErr == nil {
			state.Dirty = false
			if err := manager.persistStates(); err != nil {
				return err
			}
		}
		log.Printf(
			"alert_transition name=%s state=%s id=%s peak_indicator_pct=%.6f recovery_indicator_pct=%.6f clear_threshold_pct=%.6f numerator=%d denominator=%d",
			observation.Name,
			alertStateCleared,
			state.Record.ID,
			state.Record.Indicator,
			observation.Indicator,
			observation.ClearThreshold,
			observation.Numerator,
			observation.Denominator,
		)
		return emitErr
	}

	if state.Dirty && now.Sub(state.LastEmitted) >= time.Duration(manager.config.ActiveUpdateSeconds)*time.Second {
		state.LastEmitted = now
		if err := manager.persistStates(); err != nil {
			return err
		}
		if err := manager.emit(state.Record); err != nil {
			return err
		}
		state.Dirty = false
		if err := manager.persistStates(); err != nil {
			return err
		}
	}
	return nil
}

func (manager *alertManager) emit(record AlertRecord) error {
	if manager.config.Mode == alertModeObserve {
		log.Printf(
			"alert_observe name=%s state=%s id=%s threshold_pct=%.6f indicator_pct=%.6f",
			record.Name,
			record.State,
			record.ID,
			record.Threshold,
			record.Indicator,
		)
		return nil
	}
	if err := manager.enqueue(record); err != nil {
		atomic.AddUint64(&totalAlertDeliveryErrs, 1)
		log.Printf("alert_outbox_enqueue_error name=%s state=%s id=%s error=%v", record.Name, record.State, record.ID, err)
		return err
	}
	return nil
}

func (manager *alertManager) persistStates() error {
	payload := persistedAlertStates{Alerts: manager.states}
	return writeJSONAtomically(manager.statePath, payload)
}

func (manager *alertManager) loadStates() error {
	payload, err := os.ReadFile(manager.statePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("alert_state_read_error: %w", err)
	}

	var persisted persistedAlertStates
	if err := json.Unmarshal(payload, &persisted); err != nil {
		return fmt.Errorf("alert_state_decode_error: %w", err)
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
	atomic.StoreInt64(&activeAlerts, active)
	return nil
}

func writeJSONAtomically(path string, value any) error {
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	payload = append(payload, '\n')

	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".alert-*.tmp")
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

func (manager *alertManager) enqueue(record AlertRecord) error {
	filename := fmt.Sprintf(
		"%020d_%s_%s.json",
		time.Now().UnixNano(),
		safeFileToken(record.Name),
		record.ID,
	)
	if err := writeJSONAtomically(filepath.Join(manager.outboxDir, filename), record); err != nil {
		return err
	}
	manager.refreshOutboxPending()
	return nil
}

func (manager *alertManager) outboxWorker() {
	defer manager.wg.Done()
	retry := time.Duration(manager.config.Oracle.RetrySeconds) * time.Second
	maximumRetry := time.Duration(manager.config.Oracle.MaxRetrySeconds) * time.Second

	for {
		delivered, failed := manager.deliverOutbox()
		if failed {
			select {
			case <-time.After(retry):
			case <-manager.stop:
				return
			}
			retry *= 2
			if retry > maximumRetry {
				retry = maximumRetry
			}
			continue
		}
		retry = time.Duration(manager.config.Oracle.RetrySeconds) * time.Second
		wait := time.Second
		if !delivered {
			wait = 5 * time.Second
		}
		select {
		case <-time.After(wait):
		case <-manager.stop:
			return
		}
	}
}

func (manager *alertManager) deliverOutbox() (bool, bool) {
	entries, err := os.ReadDir(manager.outboxDir)
	if err != nil {
		atomic.AddUint64(&totalAlertDeliveryErrs, 1)
		log.Printf("alert_outbox_read_error error=%v", err)
		return false, true
	}

	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			files = append(files, entry.Name())
		}
	}
	sort.Strings(files)
	atomic.StoreInt64(&alertOutboxPending, int64(len(files)))

	delivered := false
	for _, filename := range files {
		path := filepath.Join(manager.outboxDir, filename)
		payload, err := os.ReadFile(path)
		if err != nil {
			atomic.AddUint64(&totalAlertDeliveryErrs, 1)
			log.Printf("alert_outbox_file_read_error file=%s error=%v", path, err)
			return delivered, true
		}
		var record AlertRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			atomic.AddUint64(&totalAlertDeliveryErrs, 1)
			badPath := filepath.Join(manager.badDir, filename+".bad")
			if renameErr := os.Rename(path, badPath); renameErr != nil {
				log.Printf("alert_outbox_decode_error file=%s error=%v quarantine_error=%v", path, err, renameErr)
				return delivered, true
			}
			log.Printf("alert_outbox_quarantined file=%s error=%v", badPath, err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(manager.config.Oracle.OperationTimeoutSeconds)*time.Second)
		err = manager.sink.Upsert(ctx, record)
		cancel()
		if err != nil {
			atomic.AddUint64(&totalAlertDeliveryErrs, 1)
			log.Printf("alert_oracle_delivery_error name=%s state=%s id=%s error=%v", record.Name, record.State, record.ID, err)
			return delivered, true
		}
		if err := os.Remove(path); err != nil {
			atomic.AddUint64(&totalAlertDeliveryErrs, 1)
			log.Printf("alert_outbox_remove_error file=%s error=%v", path, err)
			return delivered, true
		}
		delivered = true
		atomic.AddUint64(&totalAlertsDelivered, 1)
		log.Printf("alert_oracle_delivered name=%s state=%s id=%s", record.Name, record.State, record.ID)
	}
	manager.refreshOutboxPending()
	return delivered, false
}

func (manager *alertManager) refreshOutboxPending() {
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
	atomic.StoreInt64(&alertOutboxPending, count)
}

func newAlertID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("alert_id_generation_error: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(raw[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32], nil
}
