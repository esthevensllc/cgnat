package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReprocessAlertObservationsDetectBacklogAndFailures(t *testing.T) {
	t.Parallel()

	spoolBase := t.TempDir()
	doneDir := filepath.Join(spoolBase, "done")
	if err := os.MkdirAll(doneDir, 0750); err != nil {
		t.Fatal(err)
	}
	oldFile := filepath.Join(doneDir, "failed_old.rowbinary.done")
	recentFile := filepath.Join(doneDir, "failed_recent.rowbinary.done")
	if err := os.WriteFile(oldFile, []byte("old"), 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(recentFile, []byte("recent"), 0640); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 27, 14, 0, 0, 0, time.UTC)
	if err := os.Chtimes(oldFile, now.Add(-10*time.Minute), now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(recentFile, now.Add(-time.Minute), now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}

	manager, err := newReprocessAlertManager(testReprocessAlertConfig(spoolBase), doneDir, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()

	manager.RecordCycle(now, cycleResult{processed: 4, failed: 1, quarantined: 1})
	observations, err := manager.observations(now)
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]reprocessAlertObservation, len(observations))
	for _, observation := range observations {
		byName[observation.Name] = observation
	}

	backlog := byName[reprocessAlertBacklogName]
	if !backlog.Valid || backlog.Numerator != 1 || backlog.Denominator != 2 || backlog.Indicator != 50 {
		t.Fatalf("unexpected backlog observation: %+v", backlog)
	}
	failure := byName[reprocessAlertFailureRateName]
	if !failure.Valid || failure.Numerator != 1 || failure.Denominator != 4 || failure.Indicator != 25 {
		t.Fatalf("unexpected failure observation: %+v", failure)
	}
	quarantine := byName[reprocessAlertQuarantineName]
	if !quarantine.Valid || quarantine.Numerator != 1 || quarantine.Denominator != 5 || quarantine.Indicator != 20 {
		t.Fatalf("unexpected quarantine observation: %+v", quarantine)
	}
}

func TestReprocessAlertTransitionsPersistActiveAndClearedState(t *testing.T) {
	t.Parallel()

	spoolBase := t.TempDir()
	doneDir := filepath.Join(spoolBase, "done")
	if err := os.MkdirAll(doneDir, 0750); err != nil {
		t.Fatal(err)
	}
	config := testReprocessAlertConfig(spoolBase)
	config.ClearWindows = 2
	manager, err := newReprocessAlertManager(config, doneDir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()

	now := time.Date(2026, time.August, 27, 14, 0, 0, 0, time.UTC)
	breach := reprocessAlertObservation{
		Name:           reprocessAlertFailureRateName,
		Threshold:      1,
		ClearThreshold: 0,
		Indicator:      100,
		Numerator:      1,
		Denominator:    1,
		Valid:          true,
	}
	if err := manager.applyObservation(breach, now); err != nil {
		t.Fatal(err)
	}
	if !manager.states[reprocessAlertFailureRateName].Active {
		t.Fatal("expected active alert")
	}

	clear := breach
	clear.ClearThreshold = 0.01
	clear.Indicator = 0
	clear.Numerator = 0
	if err := manager.applyObservation(clear, now.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !manager.states[reprocessAlertFailureRateName].Active {
		t.Fatal("alert cleared too early")
	}
	if err := manager.applyObservation(clear, now.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	state := manager.states[reprocessAlertFailureRateName]
	if state.Active || state.Record.State != reprocessAlertStateCleared || state.Record.EndTime == nil {
		t.Fatalf("expected cleared alert state: %+v", state)
	}
}

func TestReprocessAlertKeepsEvaluatingAnActiveAlertWithoutNewBatches(t *testing.T) {
	t.Parallel()

	spoolBase := t.TempDir()
	doneDir := filepath.Join(spoolBase, "done")
	if err := os.MkdirAll(doneDir, 0750); err != nil {
		t.Fatal(err)
	}
	manager, err := newReprocessAlertManager(testReprocessAlertConfig(spoolBase), doneDir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()

	manager.states[reprocessAlertFailureRateName] = &reprocessAlertState{Active: true}
	observations, err := manager.observations(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, observation := range observations {
		if observation.Name == reprocessAlertFailureRateName {
			if !observation.Valid || observation.Indicator != 0 {
				t.Fatalf("active alert must receive a zero recovery observation: %+v", observation)
			}
			return
		}
	}
	t.Fatal("failure observation not found")
}

func testReprocessAlertConfig(spoolBase string) reprocessAlertConfig {
	return reprocessAlertConfig{
		Enabled:                true,
		Mode:                   reprocessAlertModeObserve,
		ServerIP:               "10.96.167.132",
		StateDir:               filepath.Join(spoolBase, "alerts"),
		EvaluationInterval:     time.Second,
		Window:                 time.Minute,
		BacklogSLA:             5 * time.Minute,
		MinimumBatches:         1,
		TriggerWindows:         1,
		ClearWindows:           3,
		ActiveUpdate:           time.Minute,
		BacklogThresholdPct:    1,
		BacklogClearPct:        0.01,
		FailureThresholdPct:    1,
		FailureClearPct:        0.01,
		QuarantineThresholdPct: 1,
		QuarantineClearPct:     0.01,
	}
}
