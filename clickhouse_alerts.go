package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type ClickHouseAlertConfig struct {
	URL                     string
	User                    string
	Password                string
	Table                   string
	AutoCreate              bool
	ConnectTimeoutSeconds   int
	OperationTimeoutSeconds int
	RetrySeconds            int
	MaxRetrySeconds         int
}

type clickHouseAlertSink struct {
	config   ClickHouseAlertConfig
	client   *http.Client
	prepare  sync.Mutex
	prepared bool
}

type clickHouseAlertRow struct {
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
}

func loadClickHouseAlertConfig(clickHouseURL string, clickHouseUser string, clickHousePass string) ClickHouseAlertConfig {
	return ClickHouseAlertConfig{
		URL:                     clickHouseURL,
		User:                    clickHouseUser,
		Password:                clickHousePass,
		Table:                   getEnv("CLICKHOUSE_ALERT_TABLE", "cgnat.collector_alerts"),
		AutoCreate:              getEnvBool("CLICKHOUSE_ALERT_AUTO_CREATE", true),
		ConnectTimeoutSeconds:   getEnvInt("CLICKHOUSE_ALERT_CONNECT_TIMEOUT_SECONDS", 5),
		OperationTimeoutSeconds: getEnvInt("CLICKHOUSE_ALERT_OPERATION_TIMEOUT_SECONDS", 10),
		RetrySeconds:            getEnvInt("CLICKHOUSE_ALERT_RETRY_SECONDS", 5),
		MaxRetrySeconds:         getEnvInt("CLICKHOUSE_ALERT_MAX_RETRY_SECONDS", 300),
	}
}

func (config ClickHouseAlertConfig) validate() error {
	parsed, err := url.Parse(config.URL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("CLICKHOUSE_URL must contain a valid HTTP or HTTPS URL")
	}
	if config.ConnectTimeoutSeconds <= 0 || config.OperationTimeoutSeconds <= 0 {
		return fmt.Errorf("ClickHouse alert timeouts must be greater than zero")
	}
	if config.RetrySeconds <= 0 || config.MaxRetrySeconds < config.RetrySeconds {
		return fmt.Errorf("ClickHouse alert retry values are invalid")
	}
	return validateClickHouseTableName(config.Table)
}

func newClickHouseAlertSink(config ClickHouseAlertConfig) (*clickHouseAlertSink, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   time.Duration(config.ConnectTimeoutSeconds) * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          2,
		MaxIdleConnsPerHost:   1,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: time.Duration(config.OperationTimeoutSeconds) * time.Second,
		DisableCompression:    true,
	}

	return &clickHouseAlertSink{
		config: config,
		client: &http.Client{
			Transport: transport,
		},
	}, nil
}

func clickHouseAlertCreateTableDDL(tableName string) string {
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

func clickHouseAlertInsertQuery(tableName string) string {
	return fmt.Sprintf(
		"INSERT INTO %s (id, hostname, ip, fecha_inicio, fecha_fin, nombre, umbral, porcentaje_indicador, estado, fecha_envio, version) FORMAT JSONEachRow",
		tableName,
	)
}

func (sink *clickHouseAlertSink) Prepare(ctx context.Context) error {
	if !sink.config.AutoCreate {
		return nil
	}

	sink.prepare.Lock()
	defer sink.prepare.Unlock()
	if sink.prepared {
		return nil
	}
	if err := sink.execute(ctx, clickHouseAlertCreateTableDDL(sink.config.Table), nil, "text/plain"); err != nil {
		return fmt.Errorf("clickhouse_alert_create_table table=%s: %w", sink.config.Table, err)
	}
	sink.prepared = true
	return nil
}

func (sink *clickHouseAlertSink) Upsert(ctx context.Context, record AlertRecord) error {
	if err := sink.Prepare(ctx); err != nil {
		return err
	}

	now := time.Now().In(peruTZ)
	var endTime *string
	if record.EndTime != nil {
		formatted := formatClickHouseAlertTime(*record.EndTime)
		endTime = &formatted
	}
	row := clickHouseAlertRow{
		ID:        record.ID,
		Hostname:  record.Hostname,
		IP:        record.IP,
		StartTime: formatClickHouseAlertTime(record.StartTime),
		EndTime:   endTime,
		Name:      record.Name,
		Threshold: record.Threshold,
		Indicator: record.Indicator,
		State:     record.State,
		SentTime:  formatClickHouseAlertTime(now),
		Version:   uint64(now.UnixNano()),
	}
	payload, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("clickhouse_alert_encode_error: %w", err)
	}
	payload = append(payload, '\n')
	if err := sink.execute(
		ctx,
		clickHouseAlertInsertQuery(sink.config.Table),
		payload,
		"application/x-ndjson",
	); err != nil {
		return fmt.Errorf("clickhouse_alert_insert_error table=%s: %w", sink.config.Table, err)
	}
	return nil
}

func formatClickHouseAlertTime(value time.Time) string {
	return value.In(peruTZ).Format("2006-01-02 15:04:05.000000")
}

func (sink *clickHouseAlertSink) execute(
	ctx context.Context,
	query string,
	body []byte,
	contentType string,
) error {
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

func (sink *clickHouseAlertSink) Close() error {
	if transport, ok := sink.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	return nil
}
