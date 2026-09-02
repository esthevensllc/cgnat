package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	var once bool
	var dryRun bool
	var limit int
	flag.BoolVar(&once, "once", false, "process one scan and exit")
	flag.BoolVar(&dryRun, "dry-run", false, "validate and list candidates without inserting or moving files")
	flag.IntVar(&limit, "limit", 0, "maximum batches to process in this execution; zero uses the configured limit")
	flag.Parse()

	if limit < 0 {
		log.Fatal("-limit cannot be negative")
	}
	if dryRun {
		once = true
	}

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	processor := newReprocessor(cfg)
	if err := processor.prepareDirectories(); err != nil {
		log.Fatalf("prepare reprocessor: %v", err)
	}
	alerts, err := newReprocessAlertManager(cfg.alerts, cfg.doneDir, cfg.minAge)
	if err != nil {
		log.Fatalf("prepare reprocessor alerts: %v", err)
	}
	processor.alerts = alerts
	releaseLock, err := acquireProcessLock(filepath.Join(cfg.spoolBase, ".failed-reprocessor.lock"))
	if err != nil {
		log.Fatalf("acquire reprocessor lock: %v", err)
	}
	defer releaseLock()

	log.Printf(
		"reprocessor_started failed_spool=%s clickhouse_url=%s workers=%d poll_seconds=%.0f min_age_seconds=%.0f max_retries=%d scan_limit=%d success_action=%s auto_create_table=%t alerts_enabled=%t alerts_mode=%s alert_backlog_sla_seconds=%.0f alert_table=%s once=%t dry_run=%t",
		cfg.spoolBase,
		cfg.clickHouseURL,
		cfg.workers,
		cfg.pollInterval.Seconds(),
		cfg.minAge.Seconds(),
		cfg.maxRetries,
		cfg.scanLimit,
		cfg.successAction,
		cfg.autoCreateTable,
		cfg.alerts.Enabled,
		cfg.alerts.Mode,
		cfg.alerts.BacklogSLA.Seconds(),
		cfg.alerts.ClickHouse.Table,
		once,
		dryRun,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if !dryRun {
		alerts.Start()
		defer alerts.Stop()
	}

	if err := processor.run(ctx, once, processOptions{dryRun: dryRun, limit: limit}); err != nil {
		log.Fatalf("reprocessor stopped with error: %v", err)
	}
	log.Printf("reprocessor_stopped")
}
