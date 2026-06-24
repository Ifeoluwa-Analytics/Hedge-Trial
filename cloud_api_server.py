import os
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from sqlalchemy import create_engine, Column, String, Integer, DateTime, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session


BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
DATABASE_PATH = os.path.join(BASE_DIR, "hedge_tracker.db")
DATABASE_URL  = f"sqlite:///{DATABASE_PATH}"

INSTANCE_NAME = os.environ.get("INSTANCE_NAME", "local")

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ─── DATABASE MODELS ──────────────────────────────────────────────────────────
class Guardian(Base):
    __tablename__ = "guardians"
    guardian_id   = Column(String, primary_key=True, index=True)
    name          = Column(String, nullable=False)
    phone_number  = Column(String, nullable=False)
    session_token = Column(String, nullable=True)


class Child(Base):
    __tablename__ = "children"
    child_id    = Column(String, primary_key=True, index=True)
    guardian_id = Column(String, ForeignKey("guardians.guardian_id"), nullable=False)
    name        = Column(String, nullable=False)


class BeaconTelemetry(Base):
    __tablename__ = "beacons"
    id             = Column(Integer, primary_key=True, autoincrement=True)
    timestamp      = Column(DateTime, default=datetime.utcnow)
    beacon_mac     = Column(String, nullable=False)
    child_id       = Column(String, ForeignKey("children.child_id"), nullable=False)
    child_name     = Column(String, nullable=False)          # ← stored for backfill
    rssi_threshold = Column(Integer, nullable=False)
    current_rssi   = Column(Integer, nullable=False)
    current_status = Column(String, nullable=False)
    message        = Column(Text, nullable=True)             # ← human-readable msg


# ─── SEED / MOCK DATA ─────────────────────────────────────────────────────────
MOCK_GUARDIAN_ID = "g-uuid-1111-2222"
MOCK_BEACON_MAC  = "00:1a:7d:da:71:11"
CHILD_ID         = "CH_01"
CHILD_NAME       = "Ifeoluwa Olaloye"

STRONG_THRESHOLD = -70
WEAK_THRESHOLD   = -100


# ─── DATABASE HELPERS ─────────────────────────────────────────────────────────
def initialize_and_seed_database():
    """Creates all tables and inserts seed records (idempotent)."""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        if not db.query(Guardian).filter_by(guardian_id=MOCK_GUARDIAN_ID).first():
            db.add(Guardian(
                guardian_id=MOCK_GUARDIAN_ID,
                name="Aduojo Ilemona",
                phone_number="+2348000000000",
                session_token="session_active_abc123",
            ))
            db.commit()

        if not db.query(Child).filter_by(child_id=CHILD_ID).first():
            db.add(Child(
                child_id=CHILD_ID,
                guardian_id=MOCK_GUARDIAN_ID,
                name=CHILD_NAME,
            ))
            db.commit()

        print(f"💾 [{INSTANCE_NAME}] Database initialised and seeded successfully.")
        print(f"💾 [{INSTANCE_NAME}] DB file location → {DATABASE_PATH}")
    except Exception as exc:
        db.rollback()
        print(f"⚠️  [{INSTANCE_NAME}] Seed warning: {exc}")
    finally:
        db.close()


def log_telemetry_to_db(
    child_id: str,
    child_name: str,
    rssi: int,
    status: str,
    message: str,
    threshold: int,
):
    """
    Persists one telemetry row to THIS instance's SQLite file.
    Local and Render each write to their own separate hedge_tracker.db —
    there is no shared database, by design.
    """
    db = SessionLocal()
    try:
        entry = BeaconTelemetry(
            timestamp      = datetime.utcnow(),
            beacon_mac     = MOCK_BEACON_MAC,
            child_id       = child_id,
            child_name     = child_name,
            rssi_threshold = threshold,
            current_rssi   = rssi,
            current_status = status,
            message        = message,
        )
        db.add(entry)
        db.commit()
        print(f"💾 [{INSTANCE_NAME}] DB write OK — {child_name} | {rssi} dBm | {status}")
    except Exception as exc:
        db.rollback()
        print(f"❌ [{INSTANCE_NAME}] DB write error: {exc}")
    finally:
        db.close()


def fetch_last_telemetry(child_id: str):
    """Returns the most recent BeaconTelemetry row for child_id, or None."""
    db = SessionLocal()
    try:
        return (
            db.query(BeaconTelemetry)
            .filter(BeaconTelemetry.child_id == child_id)
            .order_by(BeaconTelemetry.id.desc())
            .first()
        )
    finally:
        db.close()


# ─── WEBSOCKET CONNECTION MANAGER ─────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active_connections.append(ws)
        print(f"🔌 [{INSTANCE_NAME}] WS client connected — total: {len(self.active_connections)}")

    def disconnect(self, ws: WebSocket):
        if ws in self.active_connections:
            self.active_connections.remove(ws)
            print(f"🔌 [{INSTANCE_NAME}] WS client removed — total: {len(self.active_connections)}")

    async def broadcast(self, payload: dict):
        """Fan-out to all clients connected to THIS instance only; prune stale
        connections on send failure."""
        dead: list[WebSocket] = []
        for ws in self.active_connections:
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


# ─── API MODELS ───────────────────────────────────────────────────────────────
class BeaconPayload(BaseModel):
    child_id:   str
    child_name: str
    rssi:       int
    status:     str
    message:    str


# ─── APP LIFESPAN ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_and_seed_database()
    print(f"✅ [{INSTANCE_NAME}] Hedge Cloud API started — waiting for BLE telemetry...")
    yield
    print(f"🛑 [{INSTANCE_NAME}] Hedge Cloud API shutting down.")


# ─── FASTAPI APP ──────────────────────────────────────────────────────────────
app = FastAPI(title=f"Hedge Cloud API [{INSTANCE_NAME}]", version="1.2", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # lock to your Render domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── ROUTES ───────────────────────────────────────────────────────────────────
@app.get("/")
def serve_frontend():
    # FIX: resolve index.html relative to this script too, so "python
    # cloud_api_server.py" works regardless of your current terminal folder.
    return FileResponse(os.path.join(BASE_DIR, "index.html"))


@app.get("/health")
def health_check():
    return {
        "status": "Hedge Cloud API is Running",
        "instance": INSTANCE_NAME,
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.post("/api/telemetry")
async def receive_telemetry(payload: BeaconPayload):
    """
    Accepts processed RSSI data from local_ble_scanner.py.
    The scanner posts to MULTIPLE instances of this same service (e.g. one
    local, one on Render) — each instance independently persists to its own
    SQLite file and broadcasts only to the WebSocket clients connected to it.
    There is no cross-instance awareness or sync by design.
    """
    # 1. Persist to THIS instance's database
    log_telemetry_to_db(
        child_id   = payload.child_id,
        child_name = payload.child_name,
        rssi       = payload.rssi,
        status     = payload.status,
        message    = payload.message,
        threshold  = STRONG_THRESHOLD,
    )

    # 2. Broadcast downstream to whoever is connected to THIS instance's frontend
    await manager.broadcast({
        "child_id":   payload.child_id,
        "child_name": payload.child_name,
        "rssi":       payload.rssi,
        "status":     payload.status,
        "message":    payload.message,
    })

    print(f"📊 [{INSTANCE_NAME}] Telemetry → {payload.child_name} | {payload.rssi} dBm | {payload.status}")
    return {"status": "Processed", "instance": INSTANCE_NAME, "child_status": payload.status}


@app.websocket("/ws/monitor")
async def websocket_endpoint(websocket: WebSocket):
    """
    Real-time dashboard endpoint, scoped to THIS instance only.
    On connect: replays the last known DB record so the UI is never blank.
    While open: echoes keep-alive heartbeats every 25 s to survive Render's
                proxy idle timeout (~30 s).
    """
    await manager.connect(websocket)

    # ── Historical backfill ──────────────────────────────────────────────────
    try:
        last = fetch_last_telemetry(CHILD_ID)
        if last:
            await websocket.send_json({
                "child_id":   last.child_id,
                "child_name": last.child_name,   # ← from DB row, not hardcoded
                "rssi":       last.current_rssi,
                "status":     last.current_status,
                "message":    f"{last.message or 'Last known state'} "
                              f"(restored {last.timestamp.strftime('%H:%M:%S')})",
            })
        else:
            await websocket.send_json({
                "child_id":   CHILD_ID,
                "child_name": CHILD_NAME,
                "rssi":       0,
                "status":     "INIT",
                "message":    "System online — waiting for scanner telemetry...",
            })
    except Exception as exc:
        print(f"⚠️  [{INSTANCE_NAME}] Backfill error: {exc}")

    # ── Keep-alive loop ──────────────────────────────────────────────────────
    try:
        while True:
            try:
                # Short timeout keeps the worker responsive without burning CPU
                await asyncio.wait_for(websocket.receive_text(), timeout=25.0)
            except asyncio.TimeoutError:
                # Heartbeat prevents Render's proxy from closing an idle socket
                await websocket.send_json({
                    "status":  "HEARTBEAT",
                    "message": "keep-alive",
                })
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print(f"🔌 [{INSTANCE_NAME}] WebSocket client disconnected cleanly.")
    except Exception as exc:
        manager.disconnect(websocket)
        print(f"⚠️  [{INSTANCE_NAME}] WebSocket dropped: {exc}")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))   # Render injects PORT at runtime
    print(f"🚀 Starting Hedge Cloud API as instance: '{INSTANCE_NAME}' on port {port}")
    uvicorn.run("cloud_api_server:app", host="0.0.0.0", port=port, reload=False)
