#!/bin/sh
set -eu
umask 077

BASE_DIR=${CGNAT_BASE_DIR:-/index1/tareas/proyectos_python}
APP_DIR="$BASE_DIR/apps/cgnat_etl"
LOG_DIR="$BASE_DIR/logs/cgnat_etl"
ENV_FILE=${CGNAT_ENV_FILE:-$BASE_DIR/conf/cgnat_etl.env}
mkdir -p "$LOG_DIR"
export TZ=America/Lima
exec >> "$LOG_DIR/cgnat_etl_$(date +%Y%m%d).log" 2>&1

if [ -f "$ENV_FILE" ]; then
    set -a
    . "$ENV_FILE"
    set +a
fi

# POSIX sh usa punto; source corresponde a bash.
. "$BASE_DIR/env/bin/activate"
cd "$APP_DIR"
exec "$BASE_DIR/env/bin/python3" -u main.py --config "$APP_DIR/config.ini" "$@"
