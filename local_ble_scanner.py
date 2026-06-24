import asyncio
import requests
from bleak import BleakScanner
from collections import deque
from datetime import datetime
from typing import Dict

# ─── CONFIGURATION ───────────────────────────────────────────────────────────
TARGET_NAME = "Melody's A07"       # BLE advertisement name to track
CHILD_ID    = "CH_01"
CHILD_NAME  = "Ifeoluwa Olaloye"

# RSSI thresholds (dBm)
STRONG_THRESHOLD = -70             # ≥ this  →  SECURE
WEAK_THRESHOLD   = -100            # ≥ this  →  WEAK  (else BREACH if detected)

# Polling / smoothing
RSSI_BUFFER_SIZE           = 5     # sliding-window size
MIN_CONSECUTIVE_FOR_BREACH = 5     # missed cycles before BREACH
SCAN_INTERVAL              = 1.0   # seconds between telemetry posts


CLOUD_API_BASE_URL     = "http://localhost:8000"
API_TELEMETRY_ENDPOINT = f"{CLOUD_API_BASE_URL}/api/telemetry"
API_HEALTH_ENDPOINT    = f"{CLOUD_API_BASE_URL}/health"


# ─── SCANNER CLASS ───────────────────────────────────────────────────────────
class LocalBLEScanner:
    def __init__(self):
        self.rssi_buffer            = deque(maxlen=RSSI_BUFFER_SIZE)
        self.consecutive_not_found  = 0
        self.last_status            = "INIT"
        self.last_seen_time         = datetime.now()
        self.running                = False

    # ── BLE callback ─────────────────────────────────────────────────────────
    async def detection_callback(self, device, adv_data):
        name = adv_data.local_name or device.name
        if name and TARGET_NAME.lower() in name.lower():
            self.consecutive_not_found = 0
            self.last_seen_time        = datetime.now()
            self.rssi_buffer.append(adv_data.rssi)
            print(f"📡 Beacon detected: {name} | RSSI: {adv_data.rssi} dBm")

    # ── Status calculation ────────────────────────────────────────────────────
    async def calculate_status(self) -> Dict:
        """
        Builds the telemetry payload.

        Status contract
        ───────────────
        SECURE      beacon visible AND RSSI ≥ STRONG_THRESHOLD
        WEAK        beacon visible AND STRONG > RSSI ≥ WEAK_THRESHOLD
        NOT_FOUND   beacon timed out, consecutive misses < MIN_CONSECUTIVE
        BREACH      beacon timed out, consecutive misses ≥ MIN_CONSECUTIVE
        """
        elapsed = (datetime.now() - self.last_seen_time).total_seconds()

        # Age out the buffer slowly when beacon is missing
        if elapsed > 2.5:
            self.consecutive_not_found += 1
            if self.rssi_buffer:
                self.rssi_buffer.popleft()

        avg_rssi = (
            round(sum(self.rssi_buffer) / len(self.rssi_buffer))
            if self.rssi_buffer
            else -120
        )

        # ── FIX: clear status vocabulary ─────────────────────────────────────
        if elapsed <= 2.5:
            # Beacon was seen recently
            if avg_rssi >= STRONG_THRESHOLD:
                status = "SECURE"
                msg    = "Phone inside safe perimeter"
            elif avg_rssi >= WEAK_THRESHOLD:
                status = "WEAK"                         # ← was "NOT_FOUND" — fixed
                msg    = "Signal weak — move closer"
            else:
                status = "BREACH"
                msg    = "Signal critically low!"
        else:
            # Beacon has not been seen for > 2.5 s
            if self.consecutive_not_found >= MIN_CONSECUTIVE_FOR_BREACH:
                status   = "BREACH"
                msg      = "Beacon NOT DETECTED"
                avg_rssi = -120
            else:
                status = "NOT_FOUND"                    # searching, not yet BREACH
                msg    = "Searching for beacon…"

        self.last_status = status

        # Console feedback
        if elapsed <= 2.5:
            print(f"📱 {CHILD_NAME} | RSSI: {avg_rssi} dBm | {status}")
        else:
            print(
                f"🚫 {CHILD_NAME} | NOT SEEN | {status} "
                f"(missed cycles: {self.consecutive_not_found})"
            )

        return {
            "child_id":   CHILD_ID,
            "child_name": CHILD_NAME,
            "rssi":       avg_rssi,
            "status":     status,
            "message":    msg,
        }

    # ── HTTP POST (thread-pool safe) ──────────────────────────────────────────
    def _post_sync(self, payload: Dict) -> bool:
        """Synchronous POST — called via asyncio.to_thread to avoid blocking."""
        try:
            r = requests.post(API_TELEMETRY_ENDPOINT, json=payload, timeout=5)
            if r.status_code == 200:
                result = r.json()
                print(f"☁️  Cloud ACK: {result.get('child_status', 'UNKNOWN')}")
                return True
            else:
                print(f"❌ Cloud HTTP {r.status_code}: {r.text[:120]}")
                return False
        except requests.exceptions.ConnectionError:
            print(f"❌ Cannot reach {CLOUD_API_BASE_URL} — is cloud_api_server.py running?")
            return False
        except Exception as exc:
            print(f"❌ POST error: {exc}")
            return False

    async def send_to_cloud(self, payload: Dict) -> bool:
        # FIX: run blocking HTTP call in a thread so the BLE event loop isn't stalled
        return await asyncio.to_thread(self._post_sync, payload)

    # ── Connectivity pre-check ────────────────────────────────────────────────
    async def check_cloud_connectivity(self):
        print(f"🌐 Checking server connectivity → {API_HEALTH_ENDPOINT}")
        try:
            r = await asyncio.to_thread(
                lambda: requests.get(API_HEALTH_ENDPOINT, timeout=10)
            )
            if r.status_code == 200:
                print(f"✅ Server reachable: {r.json()}")
            else:
                print(f"⚠️  Server responded with HTTP {r.status_code}")
        except Exception as exc:
            print(f"⚠️  Health check failed: {exc}")
            print("    Make sure cloud_api_server.py is running first (python cloud_api_server.py).")

    # ── Main scan loop ────────────────────────────────────────────────────────
    async def start_scanning(self):
        print(f"🔍 BLE Scanner starting — tracking: {TARGET_NAME}")
        print(f"☁️  Target API: {CLOUD_API_BASE_URL}")

        await self.check_cloud_connectivity()

        scanner = BleakScanner(detection_callback=self.detection_callback)
        await scanner.start()
        self.running = True

        try:
            while self.running:
                payload = await self.calculate_status()
                await self.send_to_cloud(payload)
                await asyncio.sleep(SCAN_INTERVAL)
        except KeyboardInterrupt:
            print("\n⏹️  Stopping scanner…")
        finally:
            await scanner.stop()
            self.running = False
            print("🛑 BLE scanner stopped.")

    def stop_scanning(self):
        self.running = False


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────
async def main():
    scanner = LocalBLEScanner()
    await scanner.start_scanning()


if __name__ == "__main__":
    asyncio.run(main())
