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

# ── Smoothing parameters ──────────────────────────────────────────────────────
#
#  Layer 1 — Outlier rejection (per raw reading, in detection_callback)
#    A raw RSSI reading is dropped if it deviates more than OUTLIER_GATE dBm
#    from the current smoothed value.  This stops a single bad antenna
#    reflection from entering the pipeline at all.
OUTLIER_GATE = 15               # dBm; raw readings outside ±this are discarded

#  Layer 2 — Exponential weighted moving average (EMA)
#    Instead of a plain mean over the last N readings, we apply an EMA so
#    recent readings count more but old readings fade gradually rather than
#    dropping off a cliff when they leave the window.
#    alpha=0.3 → new reading contributes 30 %, history 70 %.
#    Lower alpha = smoother but slower to react; 0.25-0.35 is a good range
#    for indoor BLE.
EMA_ALPHA    = 0.3

#  Layer 3 — Status hysteresis (in calculate_status)
#    A status change is only confirmed once the candidate status has been
#    stable for HYSTERESIS_CYCLES consecutive cycles.  This prevents the
#    display from flipping between SECURE and WEAK when the EMA is hovering
#    right at the boundary.
HYSTERESIS_CYCLES = 3

# Trend window / dead-band
TREND_WINDOW  = 6               # cycles to span the oldest-vs-newest comparison
TREND_DEADBAND = 5              # dBm net change across window to call a direction

# Timing
MIN_CONSECUTIVE_FOR_LOST = 5   # missed cycles after buffer drains → LOST
SCAN_INTERVAL            = 1.0  # seconds between telemetry posts

CLOUD_TARGETS = {
    "local":  "http://localhost:8000",
    "render": "https://hedge-trial.onrender.com",
}
REQUEST_TIMEOUT = 5
RENDER_TIMEOUT  = 12


# ─── SCANNER CLASS ───────────────────────────────────────────────────────────
class LocalBLEScanner:
    def __init__(self):
        self.last_seen_time        = datetime.now()
        self.consecutive_not_found = 0
        self.running               = False

        # Layer 2: EMA state — starts as None until first reading arrives
        self._ema: Optional[float] = None

        # Layer 3: hysteresis — track what status the EMA "wants" to be,
        # and only commit once it has held for HYSTERESIS_CYCLES cycles
        self._candidate_status: Optional[str] = None
        self._candidate_count:  int           = 0
        self._confirmed_status: str           = "INIT"

        # Trend history: stores the committed EMA value each cycle so we
        # can compare newest vs oldest across TREND_WINDOW cycles
        self._trend_history: deque = deque(maxlen=TREND_WINDOW)

        self._target_failure_streak: Dict[str, int] = {n: 0 for n in CLOUD_TARGETS}

    # ── BLE callback — Layer 1: outlier rejection ─────────────────────────────
    async def detection_callback(self, device, adv_data):
        name = adv_data.local_name or device.name
        if not (name and TARGET_NAME.lower() in name.lower()):
            return

        raw = adv_data.rssi

        # Reject raw readings that are implausibly far from the current EMA.
        # On the very first reading there is no EMA yet, so we always accept.
        if self._ema is not None and abs(raw - self._ema) > OUTLIER_GATE:
            print(f"🗑️  Outlier dropped: {raw} dBm (EMA={self._ema:.1f}, gate=±{OUTLIER_GATE})")
            return

        # Update EMA — Layer 2
        if self._ema is None:
            self._ema = float(raw)          # seed with first real reading
        else:
            self._ema = EMA_ALPHA * raw + (1 - EMA_ALPHA) * self._ema

        self.consecutive_not_found = 0
        self.last_seen_time        = datetime.now()
        print(f"📡 {name} | raw={raw} dBm  EMA={self._ema:.1f} dBm")

    # ── Status calculation ────────────────────────────────────────────────────
    async def calculate_status(self) -> Dict:
        """
        Smoothing pipeline summary
        ──────────────────────────
        1. Outlier rejection  — bad raw readings never reach the EMA
        2. EMA               — smooth float representation of current signal
        3. Hysteresis        — status only changes after N consecutive cycles
                               in the new state; boundary jitter is invisible

        Status contract
        ───────────────
        SECURE   EMA ≥ STRONG_THRESHOLD (confirmed)
        WEAK     WEAK_THRESHOLD ≤ EMA < STRONG_THRESHOLD (confirmed)
        BREACH   beacon timed out OR EMA < WEAK_THRESHOLD (confirmed)
        LOST     beacon gone, EMA decayed to None (no data at all)
        """
        elapsed     = (datetime.now() - self.last_seen_time).total_seconds()
        beacon_live = elapsed <= 2.5

        # ── Decay EMA when beacon is missing ─────────────────────────────────
        # Rather than abruptly zeroing the EMA, nudge it toward -120 each
        # missed cycle so the value degrades gracefully.  Rate chosen so the
        # EMA crosses WEAK_THRESHOLD in ~5 missed cycles if it was at
        # STRONG_THRESHOLD when the beacon disappeared.
        if not beacon_live:
            self.consecutive_not_found += 1
            if self._ema is not None:
                # Pull 15 % toward the floor each missed cycle
                self._ema = 0.85 * self._ema + 0.15 * (-120)

        # Round for display; None if we never received any reading
        ema_rounded: Optional[int] = round(self._ema) if self._ema is not None else None

        # ── Determine raw candidate status from EMA ───────────────────────────
        if self._ema is None:
            raw_candidate = "LOST"
        elif not beacon_live and self.consecutive_not_found >= MIN_CONSECUTIVE_FOR_LOST and self._ema < WEAK_THRESHOLD:
            raw_candidate = "LOST"
        elif self._ema >= STRONG_THRESHOLD:
            raw_candidate = "SECURE"
        elif self._ema >= WEAK_THRESHOLD:
            raw_candidate = "WEAK"
        else:
            raw_candidate = "BREACH"

        # ── Layer 3: hysteresis ───────────────────────────────────────────────
        # Count consecutive cycles where the EMA wants the same status.
        # Only flip _confirmed_status once the candidate has held long enough.
        # Exception: LOST and BREACH escalate immediately (safety-first).
        if raw_candidate == self._candidate_status:
            self._candidate_count += 1
        else:
            self._candidate_status = raw_candidate
            self._candidate_count  = 1

        immediate_escalation = raw_candidate in ("LOST", "BREACH")
        if immediate_escalation or self._candidate_count >= HYSTERESIS_CYCLES:
            self._confirmed_status = raw_candidate

        status = self._confirmed_status

        # ── Message text ─────────────────────────────────────────────────────
        msg_map = {
            "SECURE": "Phone inside safe perimeter",
            "WEAK":   "Signal weak — move closer",
            "BREACH": "Beacon NOT DETECTED",
            "LOST":   "Beacon signal lost",
        }
        msg = msg_map.get(status, "Scanning…")

        # For LOST, report rssi as None so the frontend shows '--'
        rssi_out: Optional[int] = None if status == "LOST" else ema_rounded

        # ── Trend ────────────────────────────────────────────────────────────
        if ema_rounded is not None:
            self._trend_history.append(ema_rounded)

        trend = "steady"
        if len(self._trend_history) >= 2:
            delta = self._trend_history[-1] - self._trend_history[0]
            if delta > TREND_DEADBAND:
                trend = "approaching"
            elif delta < -TREND_DEADBAND:
                trend = "departing"

        # ── Console log ──────────────────────────────────────────────────────
        ema_str = f"{self._ema:.1f}" if self._ema is not None else "None"
        print(
            f"{'📱' if beacon_live else '🚫'} {CHILD_NAME} | "
            f"EMA={ema_str} dBm | confirmed={status} | "
            f"candidate={self._candidate_status}×{self._candidate_count} | "
            f"trend={trend}"
        )

        return {
            "child_id":   CHILD_ID,
            "child_name": CHILD_NAME,
            "rssi":       rssi_out,
            "status":     status,
            "message":    msg,
            "trend":      trend,
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
