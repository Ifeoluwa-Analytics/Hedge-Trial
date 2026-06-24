import asyncio
import requests
from bleak import BleakScanner
from collections import deque
from datetime import datetime
from typing import Dict, List

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


CLOUD_TARGETS = {
    "local":  "http://localhost:8000",
    "render": "https://hedge-trial.onrender.com",   # ← change to your real Render URL
}

REQUEST_TIMEOUT = 5      # seconds, per-target HTTP timeout
RENDER_TIMEOUT   = 12     # Render free-tier instances can be slow to wake from sleep


# ─── SCANNER CLASS ───────────────────────────────────────────────────────────
class LocalBLEScanner:
    def __init__(self):
        self.rssi_buffer            = deque(maxlen=RSSI_BUFFER_SIZE)
        self.consecutive_not_found  = 0
        self.last_status            = "INIT"
        self.last_seen_time         = datetime.now()
        self.running                = False

        # Per-target health tracking so we can log "target X has been down
        # for N cycles" instead of spamming identical error lines.
        self._target_failure_streak: Dict[str, int] = {name: 0 for name in CLOUD_TARGETS}

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

        if elapsed <= 2.5:
            # Beacon was seen recently
            if avg_rssi >= STRONG_THRESHOLD:
                status = "SECURE"
                msg    = "Phone inside safe perimeter"
            elif avg_rssi >= WEAK_THRESHOLD:
                status = "WEAK"
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

    # ── HTTP POST (thread-pool safe, single target) ──────────────────────────
    def _post_sync(self, name: str, base_url: str, payload: Dict) -> bool:
        """
        Synchronous POST to ONE target — called via asyncio.to_thread so it
        never blocks the BLE event loop. Failures here are caught and
        reported but never raised, so one bad target can't take down the
        other dispatch tasks running alongside it.
        """
        endpoint = f"{base_url}/api/telemetry"
        timeout  = RENDER_TIMEOUT if name == "render" else REQUEST_TIMEOUT
        try:
            r = requests.post(endpoint, json=payload, timeout=timeout)
            if r.status_code == 200:
                result = r.json()
                self._target_failure_streak[name] = 0
                print(f"☁️  [{name}] ACK: {result.get('child_status', 'UNKNOWN')}")
                return True
            else:
                self._target_failure_streak[name] += 1
                print(f"❌ [{name}] HTTP {r.status_code}: {r.text[:120]}")
                return False
        except requests.exceptions.ConnectionError:
            self._target_failure_streak[name] += 1
            streak = self._target_failure_streak[name]
            # Only print the full hint occasionally to avoid log spam once a
            # target has clearly been down for a while.
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

    # ── Fan-out dispatch to every configured target ─────────────────────────
    async def send_to_targets(self, payload: Dict) -> Dict[str, bool]:
        """
        Sends the SAME payload to every entry in CLOUD_TARGETS concurrently.
        Each target runs in its own thread so a slow/down target (e.g. a
        sleeping Render free instance) never delays delivery to the others —
        local DB logging and local frontend broadcast happen on their own
        timeline regardless of what Render is doing.
        """
        tasks = {
            name: asyncio.to_thread(self._post_sync, name, base_url, payload)
            for name, base_url in CLOUD_TARGETS.items()
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        return {
            name: (res is True)
            for name, res in zip(tasks.keys(), results)
        }

    # ── Connectivity pre-check (all targets) ─────────────────────────────────
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
                    print(f"⚠️  [{name}] Responded with HTTP {r.status_code}")
            except Exception as exc:
                print(f"⚠️  [{name}] Health check failed: {exc}")
                if name == "local":
                    print("    Make sure cloud_api_server.py is running locally first.")
                else:
                    print("    Render free-tier instances can take 30-60s to wake up — this is normal on first ping.")

    # ── Main scan loop ────────────────────────────────────────────────────────
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
                ok   = [n for n, success in outcome.items() if success]
                fail = [n for n, success in outcome.items() if not success]
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


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────
async def main():
    scanner = LocalBLEScanner()
    await scanner.start_scanning()


if __name__ == "__main__":
    asyncio.run(main())
