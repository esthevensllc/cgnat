package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
)

type OracleAlertConfig struct {
	Host                    string
	Port                    int
	Service                 string
	User                    string
	Password                string
	Table                   string
	ConnectTimeoutSeconds   int
	OperationTimeoutSeconds int
	RetrySeconds            int
	MaxRetrySeconds         int
}

type oracleAlertSink struct {
	db        *sql.DB
	upsertSQL string
}

func loadOracleAlertConfig() OracleAlertConfig {
	return OracleAlertConfig{
		Host:                    getEnv("ORACLE_ALERT_HOST", ""),
		Port:                    getEnvInt("ORACLE_ALERT_PORT", 1521),
		Service:                 getEnv("ORACLE_ALERT_SERVICE", ""),
		User:                    getEnv("ORACLE_ALERT_USER", ""),
		Password:                getEnv("ORACLE_ALERT_PASS", ""),
		Table:                   getEnv("ORACLE_ALERT_TABLE", "CGNAT.COLLECTOR_ALERTS"),
		ConnectTimeoutSeconds:   getEnvInt("ORACLE_ALERT_CONNECT_TIMEOUT_SECONDS", 5),
		OperationTimeoutSeconds: getEnvInt("ORACLE_ALERT_OPERATION_TIMEOUT_SECONDS", 10),
		RetrySeconds:            getEnvInt("ORACLE_ALERT_RETRY_SECONDS", 5),
		MaxRetrySeconds:         getEnvInt("ORACLE_ALERT_MAX_RETRY_SECONDS", 300),
	}
}

func (config OracleAlertConfig) validate() error {
	if config.Host == "" || config.Service == "" || config.User == "" || config.Password == "" {
		return fmt.Errorf("ORACLE_ALERT_HOST, ORACLE_ALERT_SERVICE, ORACLE_ALERT_USER, and ORACLE_ALERT_PASS are required in oracle mode")
	}
	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("ORACLE_ALERT_PORT must be between 1 and 65535")
	}
	if config.ConnectTimeoutSeconds <= 0 || config.OperationTimeoutSeconds <= 0 {
		return fmt.Errorf("Oracle alert timeouts must be greater than zero")
	}
	if config.RetrySeconds <= 0 || config.MaxRetrySeconds < config.RetrySeconds {
		return fmt.Errorf("Oracle alert retry values are invalid")
	}
	return validateOracleTableName(config.Table)
}

func validateOracleTableName(tableName string) error {
	parts := strings.Split(tableName, ".")
	if len(parts) < 1 || len(parts) > 2 {
		return fmt.Errorf("invalid Oracle alert table name: %s", tableName)
	}
	for _, part := range parts {
		if part == "" || len(part) > 128 {
			return fmt.Errorf("invalid Oracle identifier in alert table: %s", tableName)
		}
		for index, character := range part {
			letter := (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z')
			digit := character >= '0' && character <= '9'
			if index == 0 {
				if !letter {
					return fmt.Errorf("invalid Oracle identifier in alert table: %s", tableName)
				}
				continue
			}
			if !letter && !digit && character != '_' && character != '$' && character != '#' {
				return fmt.Errorf("invalid Oracle identifier in alert table: %s", tableName)
			}
		}
	}
	return nil
}

func newOracleAlertSink(config OracleAlertConfig) (*oracleAlertSink, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	dsn := go_ora.BuildUrl(
		config.Host,
		config.Port,
		config.Service,
		config.User,
		config.Password,
		map[string]string{
			"CONNECTION TIMEOUT": strconv.Itoa(config.ConnectTimeoutSeconds),
			"SOCKET TIMEOUT":     strconv.Itoa(config.OperationTimeoutSeconds),
		},
	)
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, fmt.Errorf("oracle_alert_open_error: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Minute)

	return &oracleAlertSink{
		db:        db,
		upsertSQL: oracleAlertMergeSQL(config.Table),
	}, nil
}

func oracleAlertMergeSQL(tableName string) string {
	return fmt.Sprintf(`
MERGE INTO %s dst
USING (
    SELECT
        :1 id,
        :2 hostname,
        :3 ip,
        CAST(:4 AS TIMESTAMP WITH TIME ZONE) fecha_inicio,
        CAST(:5 AS TIMESTAMP WITH TIME ZONE) fecha_fin,
        :6 nombre,
        CAST(:7 AS NUMBER) umbral,
        CAST(:8 AS NUMBER) porcentaje_indicador,
        :9 estado
    FROM dual
) src
ON (dst.id = src.id)
WHEN MATCHED THEN UPDATE SET
    dst.hostname = src.hostname,
    dst.ip = src.ip,
    dst.fecha_inicio = src.fecha_inicio,
    dst.fecha_fin = src.fecha_fin,
    dst.nombre = src.nombre,
    dst.umbral = src.umbral,
    dst.porcentaje_indicador = src.porcentaje_indicador,
    dst.estado = src.estado,
    dst.fecha_envio = SYSTIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    id,
    hostname,
    ip,
    fecha_inicio,
    fecha_fin,
    nombre,
    umbral,
    porcentaje_indicador,
    estado,
    fecha_envio
) VALUES (
    src.id,
    src.hostname,
    src.ip,
    src.fecha_inicio,
    src.fecha_fin,
    src.nombre,
    src.umbral,
    src.porcentaje_indicador,
    src.estado,
    SYSTIMESTAMP
)`, tableName)
}

func (sink *oracleAlertSink) Upsert(ctx context.Context, record AlertRecord) error {
	var endTime any
	if record.EndTime != nil {
		endTime = *record.EndTime
	}
	_, err := sink.db.ExecContext(
		ctx,
		sink.upsertSQL,
		record.ID,
		record.Hostname,
		record.IP,
		record.StartTime,
		endTime,
		record.Name,
		record.Threshold,
		record.Indicator,
		record.State,
	)
	if err != nil {
		return fmt.Errorf("oracle_alert_merge_error: %w", err)
	}
	return nil
}

func (sink *oracleAlertSink) Close() error {
	return sink.db.Close()
}
