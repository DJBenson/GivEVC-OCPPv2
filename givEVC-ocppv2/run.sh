#!/bin/sh
# Entrypoint — works inside Home Assistant and plain Docker.
#
# Home Assistant add-ons expose options at /data/options.json. Reading that
# directly avoids depending on bashio being installed or sourced by the base
# image.

read_option() {
    key="$1"
    default="$2"
    if [ ! -f /data/options.json ]; then
        printf '%s' "${default}"
        return
    fi
    python3 -c 'import json, sys
path = "/data/options.json"
key = sys.argv[1]
default = sys.argv[2]
try:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    value = data.get(key, default)
except Exception:
    value = default
if value is None or value == "":
    value = default
if isinstance(value, bool):
    value = "true" if value else "false"
print(value)' "${key}" "${default}"
}

select_data_dir() {
    if [ -n "${DATA_DIR:-}" ]; then
        printf '%s' "${DATA_DIR}"
        return
    fi
    if [ -f /data/options.json ]; then
        if mkdir -p /config 2>/dev/null; then
            printf '%s' "/config"
            return
        fi
    fi
    printf '%s' "/data"
}

migrate_legacy_data_dir() {
    if [ "${DATA_DIR}" = "/data" ] || [ ! -d /data ]; then
        return
    fi

    mkdir -p "${DATA_DIR}" 2>/dev/null || return

    if [ -f /data/auth_secret.key ] && [ ! -f "${DATA_DIR}/auth_secret.key" ]; then
        cp -p /data/auth_secret.key "${DATA_DIR}/auth_secret.key"
        echo "Migrated auth secret to ${DATA_DIR}"
    fi

    for suffix in "" "-wal" "-shm"; do
        source="/data/auth.db${suffix}"
        target="${DATA_DIR}/auth.db${suffix}"
        if [ -f "${source}" ] && [ ! -f "${target}" ]; then
            cp -p "${source}" "${target}"
            echo "Migrated ${source} to ${target}"
        fi
    done

    if [ -f /data/state.json ] && [ ! -f "${DATA_DIR}/state.json" ]; then
        cp -p /data/state.json "${DATA_DIR}/state.json"
        echo "Migrated legacy state.json to ${DATA_DIR}"
    fi

    if [ -d /data/firmware ] && [ ! -d "${DATA_DIR}/firmware" ]; then
        cp -a /data/firmware "${DATA_DIR}/firmware"
        echo "Migrated firmware cache to ${DATA_DIR}"
    fi
}

export OCPP_PORT="${OCPP_PORT:-$(read_option ocpp_port 7655)}"
export FIRMWARE_PORT="${FIRMWARE_PORT:-$(read_option firmware_port 9688)}"
export PUBLIC_OCPP_BASE_URL="${PUBLIC_OCPP_BASE_URL:-$(read_option public_ocpp_base_url "")}"
export PUBLIC_FIRMWARE_HOST="${PUBLIC_FIRMWARE_HOST:-$(read_option public_firmware_host "")}"
export PUBLIC_FIRMWARE_PORT="${PUBLIC_FIRMWARE_PORT:-$(read_option public_firmware_port "${FIRMWARE_PORT}")}"
export INGRESS_PORT="${INGRESS_PORT:-${HASSIO_INGRESS_PORT:-$(read_option ingress_port 8099)}}"
export DEBUG_LOGGING="${DEBUG_LOGGING:-$(read_option debug_logging false)}"
export DATA_DIR="$(select_data_dir)"
migrate_legacy_data_dir
export FIRMWARE_ROOT="${FIRMWARE_ROOT:-${DATA_DIR}/firmware}"
export FIRMWARE_MANIFEST_URL="${FIRMWARE_MANIFEST_URL:-$(read_option firmware_manifest_url https://raw.githubusercontent.com/DJBenson/giv-firmware/refs/heads/main/Firmware/EVC/manifest.json)}"
export SMTP_HOST="${SMTP_HOST:-$(read_option smtp_host "")}"
export SMTP_PORT="${SMTP_PORT:-$(read_option smtp_port 587)}"
export SMTP_USERNAME="${SMTP_USERNAME:-$(read_option smtp_username "")}"
export SMTP_PASSWORD="${SMTP_PASSWORD:-$(read_option smtp_password "")}"
export SMTP_FROM="${SMTP_FROM:-$(read_option smtp_from "")}"
export SMTP_TLS="${SMTP_TLS:-$(read_option smtp_tls true)}"

if [ -f /data/options.json ]; then
    echo "Starting GivEVC OCPPv2 (Home Assistant add-on mode)"
else
    echo "Starting GivEVC OCPPv2 (standalone Docker mode)"
fi
echo "  OCPP port     : ${OCPP_PORT}"
echo "  Firmware port : ${FIRMWARE_PORT}"
echo "  OCPP base URL : ${PUBLIC_OCPP_BASE_URL:-auto}"
echo "  Firmware host : ${PUBLIC_FIRMWARE_HOST:-auto}"
echo "  Public fw port: ${PUBLIC_FIRMWARE_PORT}"
echo "  Web UI port   : ${INGRESS_PORT}"
echo "  Data dir      : ${DATA_DIR}"
echo "  Firmware root : ${FIRMWARE_ROOT}"
echo "  SMTP host     : ${SMTP_HOST:-not configured}"
echo "  SMTP from     : ${SMTP_FROM:-not configured}"

exec python3 /app/main.py
