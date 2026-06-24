"""
CLOUD API SERVER MODULE
=======================
Runs on Render.com to handle WebSocket connections, database logging,
and real-time status broadcasting to the frontend (index.html).

This module receives RSSI data from local_ble_scanner.py and:
1. Logs telemetry to SQLite database
2. Replays the last known database record to newly connected web clients
3. Broadcasts status updates via WebSocket to connected clients
4. Serves the frontend HTML dashboard
"""

import os
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Database imports
from sqlalchemy import create_engine, Column, String, Integer, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# --- DATABASE CONFIGURATION ---
import os

# --- DATABASE CONFIGURATION ---
# ◄ FIX: Force an absolute environment path so Render can always read/write the DB securely
DATABASE_URL = "sqlite:////tmp/hedge_tracker.db" 

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- DATABASE MODELS ---
class Guardian(Base):
    __tablename__ = "guardians"
    guardian_id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    phone_number = Column(String, nullable=False)
    session_token = Column(String, nullable=True)


class Child(Base):
    __tablename__ = "children"
    child_id = Column(String, primary_key=True, index=True)
    guardian_id = Column(String, ForeignKey("guardians.guardian_id"), nullable=False)
    name = Column(String, nullable=False)


class BeaconTelemetry(Base):
    __tablename__ = "beacons"
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    beacon_mac = Column(String, nullable=False)
    child_id = Column(String, ForeignKey("children.child_id"), nullable=False)
    rssi_threshold = Column(Integer, nullable=False)
    current_rssi = Column(Integer, nullable=False)
    current_status = Column(String, nullable=False)


# --- CONFIGURATION ---
MOCK_GUARDIAN_ID = "g-uuid-1111-2222"
MOCK_BEACON_MAC = "00:1a:7d:da:71:11"
CHILD_ID = "CH_01"
CHILD_NAME = "Ifeoluwa Olaloye"

STRONG_THRESHOLD = -70
WEAK_THRESHOLD = -100


# --- DATABASE FUNCTIONS ---
def initialize_and_seed_database():
    """Creates database tables and seeds mock data"""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        if not db.query(Guardian).filter(Guardian.guardian_id == MOCK_GUARDIAN_ID).first():
            mock_guardian = Guardian(
                guardian_id=MOCK_GUARDIAN_ID,
                name="Aduojo Ilemona",
                phone_number="+2348000000000",
                session_token="session_active_abc123",
            )
            db.add(mock_guardian)
            db.commit()

        if not db.query(Child).filter(Child.child_id == CHILD_ID).first():
            mock_child = Child(
                child_id=CHILD_ID,
                guardian_id=MOCK_GUARDIAN_ID,
                name=CHILD_NAME,
            )
            db.add(mock_child)
            db.commit()
        print("💾 Database initialized and verified successfully.")
    except Exception as e:
        print(f"Seed warning: {e}")
    finally:
        db.close()


def log_telemetry_to_db(child_id: str, rssi: int, status: str, threshold: int):
    """Logs telemetry reading to database"""
    db = SessionLocal()
    try:
        log_entry = BeaconTelemetry(
            timestamp=datetime.now(),
            beacon_mac=MOCK_BEACON_MAC,
            child_id=child_id,
            rssi_threshold=threshold,
            current_rssi=rssi,
            current_status=status,
        )
        db.add(log_entry)
        db.commit()
    except Exception as e:
        print(f"Database write error: {e}")
    finally:
        db.close()


# --- WEBSOCKET CONNECTION MANAGER ---
class ConnectionManager:
    """Manages WebSocket connections for real-time broadcasting"""

    def __init__(self):
        self.active_connections = []

    async def connect(self, websocket: WebSocket):
        """Accept new WebSocket connection"""
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        """Remove disconnected WebSocket"""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients"""
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass


manager = ConnectionManager()


# --- API MODELS ---
class BeaconPayload(BaseModel):
    """Payload from local BLE scanner"""
    child_id: str
    child_name: str
    rssi: int
    status: str  
    message: str  


# --- MODERN LIFESPAN LIFECYCLE MANAGEMENT ---
# ◄ FIXED: Switched from deprecated on_event to modern lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Guarantees table creation and configuration execution prior to processing requests"""
    initialize_and_seed_database()
    print("✅ Cloud API Server started successfully via Lifespan Engine!")
    print("📱 Waiting for telemetry from local BLE scanner...")
    yield
    # Clean up operations go here on shutdown if needed


# --- FastAPI App Setup ---
app = FastAPI(title="Hedge Cloud API", version="1.0", lifespan=lifespan)

# Enable CORS for cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- FRONTEND ROUTES ---
@app.get("/")
def serve_frontend():
    """Serve the dashboard HTML"""
    return FileResponse("index.html")


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "Hedge Cloud API is Running"}


# --- TELEMETRY ENDPOINT ---
@app.post("/api/telemetry")
async def receive_telemetry(payload: BeaconPayload):
    """
    Receives processed telemetry from local BLE scanner and broadcasts downstream.
    """
    status = payload.status 

    # Log telemetry entry straight to SQLite
    log_telemetry_to_db(payload.child_id, payload.rssi, status, STRONG_THRESHOLD)

    # Forward data packet downstream to the browser dashboard
    broadcast_payload = {
        "child_id": payload.child_id,
        "child_name": payload.child_name,
        "rssi": payload.rssi,
        "status": status,
        "message": payload.message, 
    }
    await manager.broadcast(broadcast_payload)

    print(f"📊 Telemetry logged: {payload.child_name} | RSSI: {payload.rssi} dBm | Status: {status}")
    return {"status": "Processed", "child_status": status}


# --- WEBSOCKET ENDPOINT (WITH HISTORICAL BACKFILL) ---
# --- WEBSOCKET ENDPOINT (WITH HISTORICAL BACKFILL) ---
@app.websocket("/ws/monitor")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for frontend real-time dashboard updates.
    """
    await manager.connect(websocket)
    
    db: Session = SessionLocal()
    try:
        last_log = (
            db.query(BeaconTelemetry)
            .filter(BeaconTelemetry.child_id == CHILD_ID)
            .order_by(BeaconTelemetry.id.desc())
            .first()
        )
        
        if last_log:
            await websocket.send_json({
                "child_id": last_log.child_id,
                "child_name": CHILD_NAME,
                "rssi": last_log.current_rssi,
                "status": last_log.current_status,
                "message": f"Last seen at {last_log.timestamp.strftime('%H:%M:%S')} (Restored from log)"
            })
        else:
            await websocket.send_json({
                "child_id": CHILD_ID,
                "child_name": CHILD_NAME,
                "rssi": 0,
                "status": "INIT",
                "message": "System connected, waiting for telemetry scanner...",
            })
    except Exception as e:
        print(f"⚠️ Error fetching initial database state: {e}")
    finally:
        db.close()

    # ◄ FIXED: Robust asynchronous handling loop for Render deployment nodes
    try:
        while True:
            # Using a short timeout ensures the worker thread doesn't hang or close arbitrarily
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=20.0)
            except asyncio.TimeoutError:
                # Send a faint heartbeat pulse to the browser to ensure Render proxy keeps channel alive
                await websocket.send_json({"status": "HEARTBEAT", "message": "Keep-Alive Ping"})
                continue

    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("🔌 WebSocket client disconnected clean.")
    except Exception as e:
        manager.disconnect(websocket)
        print(f"⚠️ WebSocket connection dropped implicitly by host runtime: {e}")
