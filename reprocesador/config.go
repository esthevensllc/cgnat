package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	successActionDelete  = "delete"
	successActionArchive = "archive"
)

type config struct {
	clickHouseURL  string
	clickHouseUser string
	clickHousePass string

	spoolBase  string
	doneDir    string
	archiveDir string
	badDir     string

	workers           int
	pollInterval      time.Duration
	minAge            time.Duration
	httpTimeout       time.Duration
	maxRetries        int
	retryDelay        time.Duration
	interBatchDelay   time.Duration
	scanLimit         int
	autoCreateTable   bool
	successAction     string
	allowHTTPRedirect bool
}

func loadConfig() (config, error) {
	spoolBase := getEnv("FAILED_SPOOL_BASE", "/index2/huawei-cgn-go/failed")

	workers, err := getEnvInt("REPROCESS_WORKERS", 1)
	if err != nil {
		return config{}, err
	}
	pollSeconds, err := getEnvInt("REPROCESS_POLL_SECONDS", 30)
	if err != nil {
		return config{}, err
	}
	minAgeSeconds, err := getEnvInt("REPROCESS_MIN_AGE_SECONDS", 60)
	if err != nil {
		return config{}, err
	}
	httpTimeoutSeconds, err := getEnvInt("REPROCESS_HTTP_TIMEOUT_SECONDS", 180)
	if err != nil {
		return config{}, err
	}
	maxRetries, err := getEnvInt("REPROCESS_MAX_RETRIES", 3)
	if err != nil {
		return config{}, err
	}
	retryMS, err := getEnvInt("REPROCESS_RETRY_MS", 1000)
	if err != nil {
		return config{}, err
	}
	interBatchDelayMS, err := getEnvInt("REPROCESS_INTER_BATCH_DELAY_MS", 250)
	if err != nil {
		return config{}, err
	}
	scanLimit, err := getEnvInt("REPROCESS_SCAN_LIMIT", 1000)
	if err != nil {
		return config{}, err
	}
	autoCreateTable, err := getEnvBool("REPROCESS_AUTO_CREATE_TABLE", true)
	if err != nil {
		return config{}, err
	}
	allowHTTPRedirect, err := getEnvBool("REPROCESS_ALLOW_HTTP_REDIRECT", false)
	if err != nil {
		return config{}, err
	}

	successAction := strings.ToLower(getEnv("REPROCESS_SUCCESS_ACTION", successActionDelete))
	if successAction != successActionDelete && successAction != successActionArchive {
		return config{}, fmt.Errorf("REPROCESS_SUCCESS_ACTION must be delete or archive")
	}

	if workers <= 0 {
		return config{}, fmt.Errorf("REPROCESS_WORKERS must be greater than zero")
	}
	if pollSeconds <= 0 {
		return config{}, fmt.Errorf("REPROCESS_POLL_SECONDS must be greater than zero")
	}
	if minAgeSeconds < 0 {
		return config{}, fmt.Errorf("REPROCESS_MIN_AGE_SECONDS cannot be negative")
	}
	if httpTimeoutSeconds <= 0 {
		return config{}, fmt.Errorf("REPROCESS_HTTP_TIMEOUT_SECONDS must be greater than zero")
	}
	if maxRetries <= 0 {
		return config{}, fmt.Errorf("REPROCESS_MAX_RETRIES must be greater than zero")
	}
	if retryMS < 0 {
		return config{}, fmt.Errorf("REPROCESS_RETRY_MS cannot be negative")
	}
	if interBatchDelayMS < 0 {
		return config{}, fmt.Errorf("REPROCESS_INTER_BATCH_DELAY_MS cannot be negative")
	}
	if scanLimit < 0 {
		return config{}, fmt.Errorf("REPROCESS_SCAN_LIMIT cannot be negative")
	}

	return config{
		clickHouseURL:     getEnv("CLICKHOUSE_URL", "http://127.0.0.1:8123"),
		clickHouseUser:    getEnv("CLICKHOUSE_USER", "admin"),
		clickHousePass:    getEnv("CLICKHOUSE_PASS", ""),
		spoolBase:         spoolBase,
		doneDir:           filepath.Join(spoolBase, "done"),
		archiveDir:        filepath.Join(spoolBase, "reprocessed"),
		badDir:            filepath.Join(spoolBase, "bad"),
		workers:           workers,
		pollInterval:      time.Duration(pollSeconds) * time.Second,
		minAge:            time.Duration(minAgeSeconds) * time.Second,
		httpTimeout:       time.Duration(httpTimeoutSeconds) * time.Second,
		maxRetries:        maxRetries,
		retryDelay:        time.Duration(retryMS) * time.Millisecond,
		interBatchDelay:   time.Duration(interBatchDelayMS) * time.Millisecond,
		scanLimit:         scanLimit,
		autoCreateTable:   autoCreateTable,
		successAction:     successAction,
		allowHTTPRedirect: allowHTTPRedirect,
	}, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return parsed, nil
}

func getEnvBool(key string, fallback bool) (bool, error) {
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback, nil
	}

	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false, fmt.Errorf("%s must be true or false: %w", key, err)
	}
	return parsed, nil
}
