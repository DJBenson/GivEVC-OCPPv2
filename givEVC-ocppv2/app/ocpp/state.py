"""OCPP server state dataclass — no Home Assistant dependency."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class ChargerState:
    """Mutable state for a single connected charger."""

    connected: bool = False
    charge_point_id: str | None = None
    manufacturer: str | None = None
    model: str | None = None
    firmware_version: str | None = None
    firmware_status: str | None = None
    firmware_update_state: str | None = None
    firmware_update_target_file: str | None = None
    firmware_update_target_version: str | None = None
    firmware_update_previous_version: str | None = None
    firmware_update_started_at: datetime | None = None
    firmware_update_completed_at: datetime | None = None
    firmware_update_failure_reason: str | None = None
    firmware_server_host: str | None = None
    firmware_manifest_error: str | None = None
    firmware_manifest_refreshed_at: datetime | None = None
    firmware_manifest_entries: dict[str, dict[str, Any]] = field(default_factory=dict)
    selected_firmware_file: str | None = None
    available_firmware_files: list[str] = field(default_factory=list)
    last_update_firmware_request: dict[str, Any] | None = None
    charge_point_serial_number: str | None = None
    charge_box_serial_number: str | None = None
    websocket_remote_address: str | None = None
    local_ip_address: str | None = None
    connection_state: str = "disconnected"
    status: str | None = None
    error_code: str | None = None
    vendor_error_code: str | None = None
    car_plugged_in: bool | None = None
    last_seen: datetime | None = None
    last_heartbeat: datetime | None = None

    # Transaction
    transaction_id: int | None = None
    transaction_active: bool = False
    transaction_id_tag: str | None = None
    transaction_meter_start_wh: float | None = None
    transaction_started_at: datetime | None = None
    transaction_ended_at: datetime | None = None
    session_energy_kwh: float | None = None
    total_energy_kwh: float | None = None

    # Live meter values
    live_power_kw: float | None = None
    live_current_a: float | None = None
    live_voltage_v: float | None = None
    cp_voltage_v: float | None = None
    cp_duty_cycle_percent: float | None = None

    # Settings (populated from GetConfiguration responses / local state)
    current_limit_a: float | None = None
    current_limit_key: str = "MaxCurrent"  # "ChargeRate" or "MaxCurrent"
    max_energy_per_session_kwh: float = 0
    max_import_capacity_a: int | None = None
    suspend_timeout_s: int | None = None
    plug_and_go_enabled: bool = False
    plug_and_go_start_pending: bool = False
    plug_and_go_last_error: str | None = None
    charge_mode: str | None = None
    local_modbus_enabled: bool | None = None
    front_panel_leds_enabled: bool | None = None
    cp_voltage_lower_limit: float | None = None  # tenths-of-volts on wire, stored as volts
    cp_voltage_upper_limit: float | None = None
    randomised_delay_s: int | None = None

    # Config & diagnostics
    configuration: dict[str, dict[str, Any]] = field(default_factory=dict)
    charging_schedule: list[dict[str, Any]] = field(default_factory=list)
    rfid_tags: list[dict[str, Any]] = field(default_factory=list)
    action_log: list[dict[str, Any]] = field(default_factory=list)
    ocpp_frame_history: list[dict[str, Any]] = field(default_factory=list)

    # Raw last payloads for diagnostics
    last_boot_notification: dict[str, Any] | None = None
    last_status_notification: dict[str, Any] | None = None
    last_meter_values: dict[str, Any] | None = None
    meter_samples: list[dict[str, Any]] = field(default_factory=list)
    parsed_meter_values: dict[str, Any] = field(default_factory=dict)
