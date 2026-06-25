import os
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

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
    child_name     = Column(String, nullable=False)
    rssi_threshold = Column(Integer, nullable=False)
    current_rssi   = Column(Integer, nullable=False)   # -999 stored when status=LOST
    current_status = Column(String, nullable=False)
    message        = Column(Text, nullable=True)


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
    rssi: Optional[int],   # None when LOST — stored as -999 (column is NOT NULL)
    status: str,
    message: str,
    threshold: int,
):
    db = SessionLocal()
    try:
        entry = BeaconTelemetry(
            timestamp      = datetime.utcnow(),
            beacon_mac     = MOCK_BEACON_MAC,
            child_id       = child_id,
            child_name     = child_name,
            rssi_threshold = threshold,
            current_rssi   = rssi if rssi is not None else -999,
            current_status = status,
            message        = message,
        )
        db.add(entry)
        db.commit()
        rssi_display = f"{rssi} dBm" if rssi is not None else "-- (LOST)"
        print(f"💾 [{INSTANCE_NAME}] DB write OK — {child_name} | {rssi_display} | {status}")
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
    rssi:       Optional[int] = None   # None when status is LOST (buffer empty)
    status:     str
    message:    str
    trend:      str = "steady"         # 'approaching' | 'departing' | 'steady'


# ─── APP LIFESPAN ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_and_seed_database()
    print(f"✅ [{INSTANCE_NAME}] Hedge Cloud API started — waiting for BLE telemetry...")
    yield
    print(f"🛑 [{INSTANCE_NAME}] Hedge Cloud API shutting down.")


# ─── FASTAPI APP ──────────────────────────────────────────────────────────────
app = FastAPI(title=f"Hedge Cloud API [{INSTANCE_NAME}]", version="1.3", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── ROUTES ───────────────────────────────────────────────────────────────────
@app.get("/")
def serve_frontend():
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
    # 1. Persist — rssi=None (LOST) is stored as -999 to satisfy NOT NULL column
    log_telemetry_to_db(
        child_id   = payload.child_id,
        child_name = payload.child_name,
        rssi       = payload.rssi,
        status     = payload.status,
        message    = payload.message,
        threshold  = STRONG_THRESHOLD,
    )

    # 2. Broadcast — rssi=None becomes JSON null; trend passes straight through
    await manager.broadcast({
        "child_id":   payload.child_id,
        "child_name": payload.child_name,
        "rssi":       payload.rssi,    # null in JSON when LOST → frontend shows '--'
        "status":     payload.status,
        "message":    payload.message,
        "trend":      payload.trend,
    })

    rssi_display = f"{payload.rssi} dBm" if payload.rssi is not None else "-- (LOST)"
    print(f"📊 [{INSTANCE_NAME}] Telemetry → {payload.child_name} | {rssi_display} | {payload.status} | {payload.trend}")
    return {"status": "Processed", "instance": INSTANCE_NAME, "child_status": payload.status}


@app.websocket("/ws/monitor")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)

    # ── Historical backfill ──────────────────────────────────────────────────
    try:
        last = fetch_last_telemetry(CHILD_ID)
        if last:
            # -999 in DB means the row was written during a LOST cycle — send
            # null back to the frontend so it renders '--' correctly.
            rssi_out = last.current_rssi if last.current_rssi != -999 else None
            await websocket.send_json({
                "child_id":   last.child_id,
                "child_name": last.child_name,
                "rssi":       rssi_out,
                "status":     last.current_status,
                "message":    f"{last.message or 'Last known state'} "
                              f"(restored {last.timestamp.strftime('%H:%M:%S')})",
                "trend":      "steady",   # no trend on backfill — direction unknown
            })
        else:
            await websocket.send_json({
                "child_id":   CHILD_ID,
                "child_name": CHILD_NAME,
                "rssi":       None,
                "status":     "INIT",
                "message":    "System online — waiting for scanner telemetry...",
                "trend":      "steady",
            })
    except Exception as exc:
        print(f"⚠️  [{INSTANCE_NAME}] Backfill error: {exc}")

    # ── Keep-alive loop ──────────────────────────────────────────────────────
    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=25.0)
            except asyncio.TimeoutError:
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
    port = int(os.environ.get("PORT", 8000))
    print(f"🚀 Starting Hedge Cloud API as instance: '{INSTANCE_NAME}' on port {port}")
    uvicorn.run("cloud_api_server:app", host="0.0.0.0", port=port, reload=False)
