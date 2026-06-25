import asyncio
import requests
from bleak import BleakScanner
from collections import deque
from datetime import datetime
from typing import Dict

# ─── CONFIGURATION ───────────────────────────────────────────────────────────
TARGET_NAME = "Melody's A07"
CHILD_ID    = "CH_01"
CHILD_NAME  = "Ifeoluwa Olaloye"

# RSSI thresholds (dBm)
STRONG_THRESHOLD = -70          # ≥ this  → SECURE
WEAK_THRESHOLD   = -100         # ≥ this  → WEAK   (else BREACH/LOST)

# Polling / smoothing
RSSI_BUFFER_SIZE           = 5  # sliding-window size for avg RSSI
MIN_CONSECUTIVE_FOR_LOST   = 5  # missed cycles (after buffer drains) → LOST

SCAN_INTERVAL = 1.0             # seconds between telemetry posts

CLOUD_TARGETS = {
    "local":  "http://localhost:8000",
    "render": "https://hedge-trial.onrender.com",
}

REQUEST_TIMEOUT = 5
RENDER_TIMEOUT  = 12


# ─── SCANNER CLASS ───────────────────────────────────────────────────────────
class LocalBLEScanner:
    def __init__(self):
        self.rssi_buffer           = deque(maxlen=RSSI_BUFFER_SIZE)
        self.consecutive_not_found = 0
        self.last_status           = "INIT"
        self.last_seen_time        = datetime.now()
        self.running               = False

        # Rolling history of avg_rssi values used to compute movement trend.
        # We compare the newest average against the oldest in the window so
        # short-term BLE jitter (±3-5 dBm per cycle) doesn't drown out the
        # real movement signal.  A 6-cycle window at 1 s/cycle = 6 s of
        # history; real walking speed changes RSSI by ~10-20 dBm over that
        # span, which comfortably clears the dead-band below.
        self._trend_history: deque = deque(maxlen=6)

        self._target_failure_streak: Dict[str, int] = {n: 0 for n in CLOUD_TARGETS}

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
        Status contract
        ───────────────
        SECURE   beacon visible AND avg RSSI ≥ STRONG_THRESHOLD
        WEAK     avg RSSI between WEAK_THRESHOLD and STRONG_THRESHOLD
                 (whether beacon is live or buffer still holds values)
        BREACH   beacon timed out, buffer still has sub-WEAK values
                 OR beacon live but RSSI critically below WEAK_THRESHOLD
        LOST     beacon gone long enough that buffer is exhausted AND
                 consecutive misses ≥ MIN_CONSECUTIVE_FOR_LOST.
                 rssi field is sent as null — UI shows '--'.

        Payload also includes `trend`: 'approaching' | 'departing' | 'steady'
        derived by comparing current avg_rssi to the previous cycle's value.
        Dead-band of 2 dBm to suppress noise.
        """
        elapsed = (datetime.now() - self.last_seen_time).total_seconds()

        # Age out the buffer one value per missed cycle
        if elapsed > 2.5:
            self.consecutive_not_found += 1
            if self.rssi_buffer:
                self.rssi_buffer.popleft()

        buffer_empty = len(self.rssi_buffer) == 0
        avg_rssi = (
            round(sum(self.rssi_buffer) / len(self.rssi_buffer))
            if not buffer_empty
            else None        # None = truly no data; frontend shows '--'
        )
        beacon_live = elapsed <= 2.5

        # ── Trend (direction of travel) ───────────────────────────────────────
        # Push current avg into the history window, then compare newest vs
        # oldest across the full 6-cycle span (~6 s at 1 s/cycle).
        # A 5 dBm net change is needed to call a direction — large enough to
        # ignore per-cycle BLE jitter (±3-5 dBm) but small enough to catch
        # real movement within a few steps.
        DEAD_BAND = 5   # dBm net change across the window to call a direction
        if avg_rssi is not None:
            self._trend_history.append(avg_rssi)

        if len(self._trend_history) >= 2:
            oldest = self._trend_history[0]
            newest = self._trend_history[-1]
            delta  = newest - oldest        # positive = stronger signal = closer
            if delta > DEAD_BAND:
                trend = "approaching"
            elif delta < -DEAD_BAND:
                trend = "departing"
            else:
                trend = "steady"
        else:
            trend = "steady"

        # ── Status & message ──────────────────────────────────────────────────
        if beacon_live:
            if avg_rssi is not None and avg_rssi >= STRONG_THRESHOLD:
                status = "SECURE"
                msg    = "Phone inside safe perimeter"
            elif avg_rssi is not None and avg_rssi >= WEAK_THRESHOLD:
                status = "WEAK"
                msg    = "Signal weak — move closer"
            else:
                status = "BREACH"
                msg    = "Signal critically low!"
        else:
            if self.consecutive_not_found >= MIN_CONSECUTIVE_FOR_LOST and buffer_empty:
                # Buffer fully drained AND enough time has passed → LOST
                status   = "LOST"
                msg      = "Beacon signal lost"
                avg_rssi = None          # explicit null for frontend
            elif not buffer_empty and avg_rssi >= WEAK_THRESHOLD:
                status = "WEAK"
                msg    = "Signal weak — beacon drifting out of range"
            else:
                status   = "BREACH"
                msg      = "Beacon NOT DETECTED"
                avg_rssi = None

        self.last_status = status

        if beacon_live:
            print(f"📱 {CHILD_NAME} | RSSI: {avg_rssi} dBm | {status} | trend: {trend}")
        else:
            print(
                f"🚫 {CHILD_NAME} | NOT SEEN | {status} "
                f"(missed: {self.consecutive_not_found}) | trend: {trend}"
            )

        return {
            "child_id":   CHILD_ID,
            "child_name": CHILD_NAME,
            "rssi":       avg_rssi,    # None → frontend shows '--'
            "status":     status,
            "message":    msg,
            "trend":      trend,       # 'approaching' | 'departing' | 'steady'
        }

    # ── HTTP POST ─────────────────────────────────────────────────────────────
    def _post_sync(self, name: str, base_url: str, payload: Dict) -> bool:
        endpoint = f"{base_url}/api/telemetry"
        timeout  = RENDER_TIMEOUT if name == "render" else REQUEST_TIMEOUT
        try:
            r = requests.post(endpoint, json=payload, timeout=timeout)
            if r.status_code == 200:
                self._target_failure_streak[name] = 0
                print(f"☁️  [{name}] ACK: {r.json().get('child_status', 'UNKNOWN')}")
                return True
            self._target_failure_streak[name] += 1
            print(f"❌ [{name}] HTTP {r.status_code}: {r.text[:120]}")
            return False
        except requests.exceptions.ConnectionError:
            self._target_failure_streak[name] += 1
            streak = self._target_failure_streak[name]
            if streak <= 3 or streak % 30 == 0:
                print(f"❌ [{name}] Cannot reach {base_url} (fail streak: {streak})")
            return False
        except requests.exceptions.Timeout:
            self._target_failure_streak[name] += 1
            print(f"⏱️  [{name}] Timed out after {timeout}s")
            return False
        except Exception as exc:
            self._target_failure_streak[name] += 1
            print(f"❌ [{name}] POST error: {exc}")
            return False

    async def send_to_targets(self, payload: Dict) -> Dict[str, bool]:
        tasks = {
            name: asyncio.to_thread(self._post_sync, name, base_url, payload)
            for name, base_url in CLOUD_TARGETS.items()
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {name: (res is True) for name, res in zip(tasks.keys(), results)}

    async def check_cloud_connectivity(self):
        for name, base_url in CLOUD_TARGETS.items():
            health_url = f"{base_url}/health"
            timeout    = RENDER_TIMEOUT if name == "render" else REQUEST_TIMEOUT
            print(f"🌐 [{name}] Checking → {health_url}")
            try:
                r = await asyncio.to_thread(
                    lambda u=health_url, t=timeout: requests.get(u, timeout=t)
                )
                if r.status_code == 200:
                    print(f"✅ [{name}] Reachable: {r.json()}")
                else:
                    print(f"⚠️  [{name}] HTTP {r.status_code}")
            except Exception as exc:
                print(f"⚠️  [{name}] Health check failed: {exc}")
                if name == "local":
                    print("    Make sure cloud_api_server.py is running locally first.")
                else:
                    print("    Render free-tier can take 30-60s to wake up.")

    async def start_scanning(self):
        print(f"🔍 BLE Scanner starting — tracking: {TARGET_NAME}")
        for name, base_url in CLOUD_TARGETS.items():
            print(f"☁️  Target [{name}]: {base_url}")
        await self.check_cloud_connectivity()

        scanner = BleakScanner(detection_callback=self.detection_callback)
        await scanner.start()
        self.running = True
        try:
            while self.running:
                payload = await self.calculate_status()
                outcome = await self.send_to_targets(payload)
                fail = [n for n, ok in outcome.items() if not ok]
                ok   = [n for n, ok in outcome.items() if ok]
                if fail:
                    print(f"   ↳ delivered: {ok or '—'} | failed: {fail}")
                await asyncio.sleep(SCAN_INTERVAL)
        except KeyboardInterrupt:
            print("\n⏹️  Stopping scanner…")
        finally:
            await scanner.stop()
            self.running = False
            print("🛑 BLE scanner stopped.")

    def stop_scanning(self):
        self.running = False


async def main():
    scanner = LocalBLEScanner()
    await scanner.start_scanning()

if __name__ == "__main__":
    asyncio.run(main())
