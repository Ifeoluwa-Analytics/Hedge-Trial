"""
LOCAL BLE SCANNER MODULE
========================
Runs on local machine to handle Bluetooth Low Energy scanning.
Sends RSSI readings to cloud API server running on Render.com

This module is lightweight and only requires:
- bleak (BLE library)
- requests (HTTP client)
"""

import asyncio
import requests
from bleak import BleakScanner
from collections import deque
from datetime import datetime
from typing import Optional, Dict

# --- CONFIGURATION ---
TARGET_NAME = "Melody's A07"
CHILD_ID = "CH_01"
CHILD_NAME = "Ifeoluwa Olaloye"

# THRESHOLDS
STRONG_THRESHOLD = -70
WEAK_THRESHOLD = -100

# Polling configuration
RSSI_BUFFER_SIZE = 5
MIN_CONSECUTIVE_FOR_BREACH = 5
SCAN_INTERVAL = 1.0  # seconds

# --- CLOUD API CONFIGURATION ---
# Point to your Render.com deployment
CLOUD_API_BASE_URL = "https://hedge-trial.onrender.com"  # UPDATE THIS
API_TELEMETRY_ENDPOINT = f"{CLOUD_API_BASE_URL}/api/telemetry"


class LocalBLEScanner:
    """
    Handles BLE scanning on the local machine.
    Processes RSSI data and sends it to cloud API.
    """

    def __init__(self):
        self.rssi_buffer = deque(maxlen=RSSI_BUFFER_SIZE)
        self.consecutive_not_found = 0
        self.last_status = "INIT"
        self.last_seen_time = datetime.now()
        self.running = False

    async def detection_callback(self, device, adv_data):
        """Called when a BLE device is detected"""
        name = adv_data.local_name or device.name

        if name and TARGET_NAME.lower() in name.lower():
            self.consecutive_not_found = 0
            self.last_seen_time = datetime.now()
            self.rssi_buffer.append(adv_data.rssi)
            print(f"📡 Beacon detected: {name} | RSSI: {adv_data.rssi} dBm")

    async def calculate_rssi_status(self) -> Dict[str, any]:
        """
        Calculates current RSSI status and returns payload for cloud API.
        
        Returns:
            dict: Payload containing child_id, child_name, rssi, status, message
        """
        time_since_last_seen = (datetime.now() - self.last_seen_time).total_seconds()

        if time_since_last_seen > 2.5:
            self.consecutive_not_found += 1

        # Calculate average RSSI
        if self.rssi_buffer:
            avg_rssi = round(sum(self.rssi_buffer) / len(self.rssi_buffer))
            if time_since_last_seen > 1.5:
                self.rssi_buffer.popleft()
        else:
            avg_rssi = -120

        # Determine security status
        if time_since_last_seen <= 2.5:
            if avg_rssi >= STRONG_THRESHOLD:
                new_status = "SECURE"
                msg = "Phone inside safe perimeter"
            elif avg_rssi >= WEAK_THRESHOLD:
                new_status = "NOT_FOUND"
                msg = "Signal weak - Move closer"
            else:
                new_status = "BREACH"
                msg = "Phone out of bounds!"
        else:
            if self.consecutive_not_found >= MIN_CONSECUTIVE_FOR_BREACH:
                new_status = "BREACH"
                msg = "Beacon NOT DETECTED"
                avg_rssi = -120
            else:
                new_status = self.last_status
                msg = "Searching..."

        self.last_status = new_status  

        # Pack the full state context variables before sending to Render
        payload = {
            "child_id": CHILD_ID,
            "child_name": CHILD_NAME,
            "rssi": avg_rssi,
            "status": new_status,  # ◄ Add this
            "message": msg         # ◄ Add this
        }

        # Log locally
        if time_since_last_seen <= 2.5:
            print(f"📱 {CHILD_NAME} | RSSI: {avg_rssi} dBm | {new_status}")
        else:
            print(
                f"🚫 {CHILD_NAME} | NOT FOUND | {new_status} (missed cycles: {self.consecutive_not_found})"
            )

        return payload

    async def send_to_cloud(self, payload: Dict) -> bool:
        """
        Sends RSSI payload to cloud API server.
        
        Args:
            payload: Dictionary with child_id, child_name, rssi
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            response = requests.post(
                API_TELEMETRY_ENDPOINT, json=payload, timeout=5
            )
            if response.status_code == 200:
                result = response.json()
                status = result.get("child_status", "UNKNOWN")
                print(f"☁️ Cloud API Response: {status}")
                return True
            else:
                print(f"❌ Cloud API Error: {response.status_code}")
                return False
        except requests.exceptions.ConnectionError:
            print(f"❌ Cannot connect to cloud API. Is {CLOUD_API_BASE_URL} running?")
            return False
        except Exception as e:
            print(f"❌ API Error: {e}")
            return False

    async def start_scanning(self):
        """
        Starts continuous BLE scanning and sends data to cloud API.
        """
        print(f"🔍 Starting BLE scanner - tracking: {TARGET_NAME}")
        print(f"☁️ Cloud API: {CLOUD_API_BASE_URL}")

        scanner = BleakScanner(detection_callback=self.detection_callback)
        await scanner.start()

        self.running = True

        try:
            while self.running:
                payload = await self.calculate_rssi_status()
                await self.send_to_cloud(payload)
                await asyncio.sleep(SCAN_INTERVAL)
        except KeyboardInterrupt:
            print("\n⏹️ Stopping scanner...")
        finally:
            await scanner.stop()
            self.running = False

    def stop_scanning(self):
        """Stops the scanner"""
        self.running = False


async def main():
    """Main entry point for local BLE scanner"""
    scanner = LocalBLEScanner()
    await scanner.start_scanning()


if __name__ == "__main__":
    asyncio.run(main())
