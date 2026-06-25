import asyncio
import requests
from bleak import BleakScanner
from collections import deque
from datetime import datetime
from typing import Dict, Optional

# ─── CONFIGURATION ───────────────────────────────────────────────────────────
TARGET_NAME = "Melody's A07"
CHILD_ID    = "CH_01"
CHILD_NAME  = "Ifeoluwa Olaloye"

# RSSI thresholds (dBm)
STRONG_THRESHOLD = -70          # ≥ this  → SECURE
WEAK_THRESHOLD   = -100         # ≥ this  → WEAK   (else BREACH/LOST)

# ── Smoothing ────────────────────────────────────────────────────────────────
# EMA (exponential moving average) updated on *every*
# advertisement the instant it's received (not once per poll cycle). Recent
# readings dominate immediately, so status tracks real movement closely,
# while still smoothing out single-packet jitter (±3-5 dBm).
EMA_ALPHA = 0.4   # higher = snappier / less smoothing, lower = smoother / slower

# Time tiers for "how long since we last actually heard the beacon"
NOT_FOUND_GRACE_SEC = 2.5    # below this → still considered "live"
MIN_CONSECUTIVE_FOR_LOST = 5  # cycles of silence (after grace) → LOST
SCAN_INTERVAL = 1.0          # seconds between telemetry posts
LOST_AFTER_SEC = NOT_FOUND_GRACE_SEC + (MIN_CONSECUTIVE_FOR_LOST * SCAN_INTERVAL)

CLOUD_TARGETS = {
    "local":  "http://localhost:8000",
    "render": "https://hedge-trial.onrender.com",
}

REQUEST_TIMEOUT = 5
RENDER_TIMEOUT  = 12


# ─── SCANNER CLASS ───────────────────────────────────────────────────────────
class LocalBLEScanner:
    def __init__(self):
        # Continuously-updated EMA — this is now the single source of truth
        # for "what is the signal doing right now". Updated in the BLE
        # callback itself, not on the 1Hz poll loop, so it's never more than
        # one advertisement-interval stale.
        self.smoothed_rssi: Optional[float] = None

        # Frozen snapshot of the last EMA value seen while "live". Used for
        # display/decisions during the FADING tier (beacon recently lost)
        # WITHOUT being recomputed from stale data — it just doesn't change
        # until either a fresh reading arrives or we give up and go LOST.
        self.last_known_rssi: Optional[int] = None

        self.consecutive_not_found = 0
        self.last_status           = "INIT"
        self.last_seen_time        = datetime.now()
        self.running               = False

        # Rolling history of EMA snapshots (one per poll cycle, only while
        # live) used to compute movement trend. Comparing newest vs oldest
        # across this window filters out per-cycle jitter while still
        # catching real movement within a few seconds.
        self._trend_history: deque = deque(maxlen=6)

        self._target_failure_streak: Dict[str, int] = {n: 0 for n in CLOUD_TARGETS}

    # ── BLE callback ─────────────────────────────────────────────────────────
    async def detection_callback(self, device, adv_data):
        name = adv_data.local_name or device.name
        if name and TARGET_NAME.lower() in name.lower():
            self.consecutive_not_found = 0
            self.last_seen_time        = datetime.now()

            # Update EMA immediately — this is what fixes the lag. A single
            # strong reading right after a weak streak pulls the EMA up by
            # EMA_ALPHA's worth straight away, instead of waiting for 5
            # samples to cycle through a buffer.
            if self.smoothed_rssi is None:
                self.smoothed_rssi = float(adv_data.rssi)
            else:
                self.smoothed_rssi = (
                    EMA_ALPHA * adv_data.rssi + (1 - EMA_ALPHA) * self.smoothed_rssi
                )

            print(f"📡 Beacon detected: {name} | RSSI: {adv_data.rssi} dBm | EMA: {round(self.smoothed_rssi)} dBm")

    # ── Status calculation ────────────────────────────────────────────────────
    async def calculate_status(self) -> Dict:
        elapsed = (datetime.now() - self.last_seen_time).total_seconds()
        beacon_live = elapsed <= NOT_FOUND_GRACE_SEC

        if not beacon_live:
            self.consecutive_not_found += 1

        # ── Trend (direction of travel) ───────────────────────────────────────
        # Sample the EMA once per cycle into a slower window purely for
        # computing movement direction. A 5 dBm net change across the window
        # is needed to call a direction — large enough to ignore jitter but
        # small enough to catch real movement within a few seconds.
        DEAD_BAND = 5
        if beacon_live and self.smoothed_rssi is not None:
            self._trend_history.append(self.smoothed_rssi)

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
            # LIVE tier: trust the EMA, it's at most one advertisement old.
            current_rssi = round(self.smoothed_rssi) if self.smoothed_rssi is not None else None
            self.last_known_rssi = current_rssi

            if current_rssi is not None and current_rssi >= STRONG_THRESHOLD:
                status = "SECURE"
                msg    = "Phone inside safe perimeter"
            elif current_rssi is not None and current_rssi >= WEAK_THRESHOLD:
                status = "WEAK"
                msg    = "Signal weak — move closer"
            else:
                status = "BREACH"
                msg    = "Signal critically low!"

        elif elapsed <= LOST_AFTER_SEC:
            # FADING tier: beacon hasn't been heard this cycle, but we
            # haven't given up yet. Use the FROZEN last-known reading (do
            # NOT recompute an average from old samples) so we don't get
            # the old bug where stale strong values kept reporting SECURE
            # well after the phone had actually left range.
            current_rssi = self.last_known_rssi
            if current_rssi is not None and current_rssi >= WEAK_THRESHOLD:
                status = "WEAK"
                msg    = "Signal weak — beacon drifting out of range"
            else:
                status   = "BREACH"
                msg      = "Beacon NOT DETECTED"
                current_rssi = None

        else:
            # LOST tier: silence has gone on too long — stop pretending we
            # know anything and report it plainly. Reset EMA so the next
            # detection starts clean instead of being dragged by ancient data.
            status        = "LOST"
            msg           = "Beacon signal lost"
            current_rssi  = None
            self.smoothed_rssi   = None
            self.last_known_rssi = None
            self._trend_history.clear()

        self.last_status = status

        if beacon_live:
            print(f"📱 {CHILD_NAME} | RSSI: {current_rssi} dBm | {status} | trend: {trend}")
        else:
            print(
                f"🚫 {CHILD_NAME} | NOT SEEN | {status} "
                f"(missed: {self.consecutive_not_found}) | trend: {trend}"
            )

        return {
            "child_id":   CHILD_ID,
            "child_name": CHILD_NAME,
            "rssi":       current_rssi,    # None → frontend shows '--'
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
