#!/bin/sh
# Entrypoint — works both inside Home Assistant (bashio available) and plain Docker.

if command -v bashio > /dev/null 2>&1; then
    # ── Home Assistant addon mode ────────────────────────────────────────
    OCPP_PORT=$(bashio::config 'ocpp_port')
    FIRMWARE_PORT=$(bashio::config 'firmware_port')
    INGRESS_PORT=$(bashio::addon.ingress_port)
    ADOPT_FIRST=$(bashio::config 'adopt_first_charger')
    EXPECTED_CP=$(bashio::config 'expected_charge_point_id')
    DEBUG=$(bashio::config 'debug_logging')
    FIRMWARE_MANIFEST_URL=$(bashio::config 'firmware_manifest_url')

    bashio::log.info "Starting GivEVC OCPPv2 (HA addon mode)"
    bashio::log.info "  OCPP port     : ${OCPP_PORT}"
    bashio::log.info "  Firmware port : ${FIRMWARE_PORT}"
    bashio::log.info "  Ingress port  : ${INGRESS_PORT}"

    export OCPP_PORT="${OCPP_PORT}"
    export FIRMWARE_PORT="${FIRMWARE_PORT}"
    export INGRESS_PORT="${INGRESS_PORT}"
    export ADOPT_FIRST_CHARGER="${ADOPT_FIRST}"
    export EXPECTED_CHARGE_POINT_ID="${EXPECTED_CP}"
    export DEBUG_LOGGING="${DEBUG}"
    export FIRMWARE_ROOT="/data/firmware"
    export FIRMWARE_MANIFEST_URL="${FIRMWARE_MANIFEST_URL}"
else
    # ── Standalone Docker mode ───────────────────────────────────────────
    export OCPP_PORT="${OCPP_PORT:-7655}"
    export FIRMWARE_PORT="${FIRMWARE_PORT:-9688}"
    export INGRESS_PORT="${INGRESS_PORT:-8099}"
    export ADOPT_FIRST_CHARGER="${ADOPT_FIRST_CHARGER:-true}"
    export EXPECTED_CHARGE_POINT_ID="${EXPECTED_CHARGE_POINT_ID:-}"
    export DEBUG_LOGGING="${DEBUG_LOGGING:-false}"
    export FIRMWARE_ROOT="${FIRMWARE_ROOT:-/data/firmware}"
    export FIRMWARE_MANIFEST_URL="${FIRMWARE_MANIFEST_URL:-https://raw.githubusercontent.com/DJBenson/giv-firmware/refs/heads/main/Firmware/EVC/manifest.json}"

    echo "Starting GivEVC OCPPv2 (standalone Docker mode)"
    echo "  OCPP port     : ${OCPP_PORT}"
    echo "  Firmware port : ${FIRMWARE_PORT}"
    echo "  Web UI port   : ${INGRESS_PORT}"
fi

exec python3 /app/main.py
