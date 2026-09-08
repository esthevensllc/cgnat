package main

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fakeAlertSink struct {
	mu      sync.Mutex
	records []AlertRecord
	notify  chan struct{}
}

func (sink *fakeAlertSink) Prepare(_ context.Context) error {
	return nil
}

func (sink *fakeAlertSink) Upsert(_ context.Context, record AlertRecord) error {
	sink.mu.Lock()
	sink.records = append(sink.records, record)
	sink.mu.Unlock()
	select {
	case sink.notify <- struct{}{}:
	default:
	}
	return nil
}

func (sink *fakeAlertSink) Close() error {
	return nil
}

func TestCohortTrackerKeepsPacketAndRowUnitsSeparate(t *testing.T) {
	tracker := newCohortTracker(4, minimumCohortRetentionSeconds)
	second := time.Now().Unix()

	for range 10 {
		tracker.recordReceived(second, 1)
	}
	for range 9 {
		tracker.recordParseResult(second, 2, true, 8)
	}
	tracker.recordParseResult(second, 2, false, 0)
	cohorts := []insertCohort{{Second: second, Rows: 70}}
	tracker.recordInserted(cohorts, 2, second)
	tracker.recordSpooled([]insertCohort{{Second: second, Rows: 2}}, 2, second)
	tracker.recordUDPKernelDrops(second, 1, 3)

	snapshot := tracker.snapshot(second, second)
	if snapshot.ReceivedPackets != 10 {
		t.Fatalf("received packets = %d, want 10", snapshot.ReceivedPackets)
	}
	if snapshot.ParsedPackets != 9 || snapshot.ParseFailedPackets != 1 {
		t.Fatalf("parse counters = %d/%d, want 9/1", snapshot.ParsedPackets, snapshot.ParseFailedPackets)
	}
	if snapshot.ExpectedRows != 72 || snapshot.InsertedRows != 70 {
		t.Fatalf("row counters = %d/%d, want 72/70", snapshot.ExpectedRows, snapshot.InsertedRows)
	}
	if snapshot.SpooledRows != 2 || snapshot.UDPKernelDrops != 3 {
		t.Fatalf("spooled/drops = %d/%d, want 2/3", snapshot.SpooledRows, snapshot.UDPKernelDrops)
	}
}

func TestAlertObservationsUseDelayedSLAWindow(t *testing.T) {
	config := testAlertConfig(t.TempDir())
	config.WindowSeconds = 2
	config.SLASeconds = 2
	config.MinimumPackets = 1
	config.MinimumRows = 1
	tracker := newCohortTracker(4, minimumCohortRetentionSeconds)
	nowSecond := time.Now().Unix()
	delayedStart := nowSecond - 4

	for second := delayedStart; second <= delayedStart+1; second++ {
		for range 100 {
			tracker.recordReceived(second, 1)
		}
		for range 99 {
			tracker.recordParseResult(second, 2, true, 8)
		}
		tracker.recordParseResult(second, 2, false, 0)
		tracker.recordInserted([]insertCohort{{Second: second, Rows: 784}}, 2, nowSecond)
	}

	manager := &alertManager{
		config:      config,
		tracker:     tracker,
		startSecond: delayedStart,
	}
	observations := manager.observations(nowSecond)
	byName := make(map[string]alertObservation, len(observations))
	for _, observation := range observations {
		byName[observation.Name] = observation
	}

	parse := byName[alertParseSLAName]
	if !parse.Valid || math.Abs(parse.Indicator-1.0) > 0.000001 {
		t.Fatalf("parse observation valid=%t indicator=%f, want true/1.0", parse.Valid, parse.Indicator)
	}
	insert := byName[alertInsertSLAName]
	wantInsertPct := float64(16) * 100 / float64(1584)
	if !insert.Valid || math.Abs(insert.Indicator-wantInsertPct) > 0.000001 {
		t.Fatalf("insert observation valid=%t indicator=%f, want true/%f", insert.Valid, insert.Indicator, wantInsertPct)
	}
}

func TestNoTrafficObservationDoesNotRequireMinimumPackets(t *testing.T) {
	config := testAlertConfig(t.TempDir())
	config.WindowSeconds = 2
	config.MinimumPackets = 1000
	tracker := newCohortTracker(2, minimumCohortRetentionSeconds)
	nowSecond := time.Now().Unix()
	manager := &alertManager{
		config:      config,
		tracker:     tracker,
		startSecond: nowSecond - 2,
	}

	observations := manager.observations(nowSecond)
	noTraffic := observationByName(observations, alertNoUDPTrafficName)
	if !noTraffic.Valid || noTraffic.Indicator != 100 {
		t.Fatalf(
			"no-traffic observation valid=%t indicator=%f, want true/100",
			noTraffic.Valid,
			noTraffic.Indicator,
		)
	}

	tracker.recordReceived(nowSecond-1, 1)
	observations = manager.observations(nowSecond)
	noTraffic = observationByName(observations, alertNoUDPTrafficName)
	if !noTraffic.Valid || noTraffic.Indicator != 0 {
		t.Fatalf(
			"traffic recovery observation valid=%t indicator=%f, want true/0",
			noTraffic.Valid,
			noTraffic.Indicator,
		)
	}
}

func observationByName(observations []alertObservation, name string) alertObservation {
	for _, observation := range observations {
		if observation.Name == name {
			return observation
		}
	}
	return alertObservation{}
}

func TestAlertLifecycleUsesOneIncidentID(t *testing.T) {
	config := testAlertConfig(t.TempDir())
	config.ClearWindows = 2
	tracker := newCohortTracker(2, minimumCohortRetentionSeconds)
	manager, err := newAlertManager(config, tracker)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().In(peruTZ)
	breach := alertObservation{
		Name:           alertParseSLAName,
		Threshold:      1,
		ClearThreshold: 0.1,
		Indicator:      1.5,
		Numerator:      15,
		Denominator:    1000,
		Valid:          true,
	}
	if err := manager.applyObservation(breach, now); err != nil {
		t.Fatal(err)
	}
	state := manager.states[alertParseSLAName]
	if !state.Active || state.Record.State != alertStateActive {
		t.Fatalf("state = active:%t/%s, want true/ACTIVE", state.Active, state.Record.State)
	}
	incidentID := state.Record.ID

	recovery := breach
	recovery.Indicator = 0
	recovery.Numerator = 0
	if err := manager.applyObservation(recovery, now.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !state.Active {
		t.Fatal("alert cleared before the configured consecutive windows")
	}
	if err := manager.applyObservation(recovery, now.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	if state.Active || state.Record.State != alertStateCleared {
		t.Fatalf("state = active:%t/%s, want false/CLEARED", state.Active, state.Record.State)
	}
	if state.Record.ID != incidentID {
		t.Fatalf("incident ID changed from %s to %s", incidentID, state.Record.ID)
	}
	if state.Record.EndTime == nil {
		t.Fatal("cleared alert has no end time")
	}
	if _, err := filepath.Abs(manager.statePath); err != nil {
		t.Fatal(err)
	}
}

func TestClickHouseAlertTableNameValidation(t *testing.T) {
	valid := []string{"cgnat.collector_alerts", "collector_alerts", "_ops.alert_1"}
	for _, table := range valid {
		if err := validateClickHouseTableName(table); err != nil {
			t.Fatalf("valid table %q rejected: %v", table, err)
		}
	}
	invalid := []string{"cgnat.alerts;DROP TABLE x", "cgnat..alerts", "1cgn.alerts", `"cgnat"."alerts"`}
	for _, table := range invalid {
		if err := validateClickHouseTableName(table); err == nil {
			t.Fatalf("invalid table %q accepted", table)
		}
	}
}

func TestDirtyClearedStateIsReconciledOnStart(t *testing.T) {
	directory := t.TempDir()
	outboxDir := filepath.Join(directory, "outbox")
	badDir := filepath.Join(directory, "bad")
	if err := os.MkdirAll(outboxDir, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(badDir, 0750); err != nil {
		t.Fatal(err)
	}

	now := time.Now().In(peruTZ)
	sink := &fakeAlertSink{notify: make(chan struct{}, 1)}
	manager := &alertManager{
		config: AlertConfig{
			Enabled:                   true,
			Mode:                      alertModeClickHouse,
			ServerIP:                  "10.0.0.1",
			EvaluationIntervalSeconds: 60,
			ClickHouse: ClickHouseAlertConfig{
				OperationTimeoutSeconds: 1,
				RetrySeconds:            1,
				MaxRetrySeconds:         2,
			},
		},
		tracker:   newCohortTracker(1, minimumCohortRetentionSeconds),
		hostname:  "collector-1",
		statePath: filepath.Join(directory, "state.json"),
		outboxDir: outboxDir,
		badDir:    badDir,
		sink:      sink,
		stop:      make(chan struct{}),
		states: map[string]*alertRuntimeState{
			alertParseSLAName: {
				Record: AlertRecord{
					ID:        "89c94cf1-05d5-45d2-98be-10798cd77ce2",
					Hostname:  "collector-1",
					IP:        "10.0.0.1",
					StartTime: now.Add(-time.Minute),
					EndTime:   &now,
					Name:      alertParseSLAName,
					Threshold: 1,
					Indicator: 2.5,
					State:     alertStateCleared,
				},
				Dirty: true,
			},
		},
	}

	manager.Start()
	select {
	case <-sink.notify:
	case <-time.After(3 * time.Second):
		manager.Stop()
		t.Fatal("dirty cleared state was not delivered")
	}
	manager.Stop()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.records) != 1 || sink.records[0].State != alertStateCleared {
		t.Fatalf("delivered records = %#v, want one CLEARED record", sink.records)
	}
}

func TestCleanActiveStateIsNotReconciledOnStart(t *testing.T) {
	directory := t.TempDir()
	outboxDir := filepath.Join(directory, "outbox")
	if err := os.MkdirAll(outboxDir, 0750); err != nil {
		t.Fatal(err)
	}

	now := time.Now().In(peruTZ)
	manager := &alertManager{
		config:    AlertConfig{Mode: alertModeClickHouse},
		outboxDir: outboxDir,
		states: map[string]*alertRuntimeState{
			alertNoUDPTrafficName: {
				Record: AlertRecord{
					ID:        "a67c822d-6786-485a-b926-a10dacb78216",
					Hostname:  "collector-1",
					IP:        "10.0.0.1",
					StartTime: now.Add(-time.Minute),
					Name:      alertNoUDPTrafficName,
					Threshold: 100,
					Indicator: 100,
					State:     alertStateActive,
				},
				Active: true,
			},
		},
	}

	if manager.reconcileDirtyStates() {
		t.Fatal("clean active state must not be marked for delivery")
	}
	entries, err := os.ReadDir(outboxDir)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("outbox entries = %d, want 0", len(entries))
	}
}

func testAlertConfig(directory string) AlertConfig {
	return AlertConfig{
		Enabled:                   true,
		Mode:                      alertModeObserve,
		ServerIP:                  "10.0.0.1",
		StateDir:                  directory,
		EvaluationIntervalSeconds: 10,
		WindowSeconds:             60,
		SLASeconds:                60,
		MinimumPackets:            1000,
		MinimumRows:               1000,
		TriggerWindows:            1,
		ClearWindows:              3,
		ActiveUpdateSeconds:       60,
		NoTrafficEnabled:          true,
		UDPThresholdPct:           0.1,
		UDPClearPct:               0.01,
		ParseThresholdPct:         1,
		ParseClearPct:             0.1,
		InsertThresholdPct:        1,
		InsertClearPct:            0.1,
		ClickHouse: ClickHouseAlertConfig{
			URL:                     "http://127.0.0.1:8123",
			Table:                   "cgnat.collector_alerts",
			ConnectTimeoutSeconds:   5,
			OperationTimeoutSeconds: 10,
			RetrySeconds:            5,
			MaxRetrySeconds:         300,
		},
	}
}
