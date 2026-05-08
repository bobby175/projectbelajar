"""
ESP32 Dashboard - FastAPI Backend v2.0
=======================================
Multi-user dengan role admin dan viewer.

Install:
    pip install fastapi uvicorn paho-mqtt python-dotenv

Jalankan:
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

import asyncio, json, os, time, hashlib
from collections import defaultdict, deque
from datetime import datetime
from typing import Optional

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

# ============================================================
# CONFIG
# ============================================================
MQTT_HOST     = os.getenv("MQTT_HOST", "xxxx.s1.eu.hivemq.cloud")
MQTT_PORT     = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USER     = os.getenv("MQTT_USER", "username_anda")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "password_anda")
API_SECRET    = os.getenv("API_SECRET", "rahasia-ganti-ini")

# ============================================================
# MULTI USER - Tambah/edit user di sini atau via env variable
# Format env: USERS=admin:password:admin,viewer1:pass123:viewer
# ============================================================
def load_users():
    users = {}
    # Default users dari env
    default = os.getenv("USERS", "")
    if default:
        for entry in default.split(","):
            parts = entry.strip().split(":")
            if len(parts) == 3:
                uname, pwd, role = parts
                users[uname.strip()] = {"password": pwd.strip(), "role": role.strip()}

    # Fallback hardcoded jika env USERS kosong
    if not users:
        admin_user = os.getenv("ADMIN_USER", "admin")
        admin_pass = os.getenv("ADMIN_PASS", "admin123")
        viewer_user = os.getenv("VIEWER_USER", "viewer")
        viewer_pass = os.getenv("VIEWER_PASS", "viewer123")
        users = {
            admin_user:  {"password": admin_pass,  "role": "admin"},
            viewer_user: {"password": viewer_pass, "role": "viewer"},
        }
    return users

USERS = load_users()
# ============================================================

app = FastAPI(title="ESP32 Dashboard API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Storage
devices: dict = {}
sensor_history: dict = defaultdict(lambda: deque(maxlen=100))
active_ws: list = []
sessions: dict = {}

security = HTTPBearer(auto_error=False)

# ---- Models ----
class LoginRequest(BaseModel):
    username: str
    password: str

class CommandRequest(BaseModel):
    device_id: str
    action: str
    pin: Optional[int] = None
    state: Optional[bool] = None

class AddUserRequest(BaseModel):
    username: str
    password: str
    role: str = "viewer"

# ---- Auth ----
def create_token(username: str) -> str:
    raw = f"{username}:{time.time()}:{API_SECRET}"
    token = hashlib.sha256(raw.encode()).hexdigest()
    sessions[token] = {
        "username": username,
        "role": USERS[username]["role"],
        "created": time.time()
    }
    return token

def get_session(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Token diperlukan")
    token = credentials.credentials
    if token not in sessions:
        raise HTTPException(status_code=401, detail="Token tidak valid atau expired")
    return sessions[token]

def require_admin(session: dict = Depends(get_session)):
    if session["role"] != "admin":
        raise HTTPException(status_code=403, detail="Akses ditolak — hanya admin yang diizinkan")
    return session

# ---- MQTT ----
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

def on_mqtt_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("✅ MQTT Connected ke HiveMQ Cloud")
        client.subscribe("esp32/+/status")
        client.subscribe("esp32/+/sensor")
        client.subscribe("esp32/+/response")
    else:
        print(f"❌ MQTT gagal: {reason_code}")

def on_mqtt_message(client, userdata, msg):
    try:
        topic   = msg.topic
        payload = json.loads(msg.payload.decode())
        parts   = topic.split("/")
        if len(parts) < 3: return

        device_id = parts[1]
        msg_type  = parts[2]

        if msg_type in ("status", "sensor"):
            devices[device_id] = {
                **devices.get(device_id, {}),
                **payload,
                "last_seen": datetime.now().isoformat(),
                "online": True,
            }
            if msg_type == "sensor":
                sensor_history[device_id].append({
                    **payload, "time": datetime.now().isoformat()
                })

        asyncio.run(broadcast_ws({
            "type": msg_type,
            "device_id": device_id,
            "data": payload,
            "timestamp": datetime.now().isoformat()
        }))
    except Exception as e:
        print(f"MQTT message error: {e}")

async def broadcast_ws(data: dict):
    dead = []
    for ws in active_ws:
        try: await ws.send_json(data)
        except: dead.append(ws)
    for ws in dead:
        active_ws.remove(ws)

def setup_mqtt():
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.tls_set()
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    try:
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.loop_start()
    except Exception as e:
        print(f"MQTT connect error: {e}")

@app.on_event("startup")
async def startup(): setup_mqtt()

@app.on_event("shutdown")
async def shutdown(): mqtt_client.loop_stop(); mqtt_client.disconnect()

# ============================================================
# ENDPOINTS
# ============================================================

@app.post("/api/login")
async def login(req: LoginRequest):
    user = USERS.get(req.username)
    if not user or user["password"] != req.password:
        raise HTTPException(status_code=401, detail="Username atau password salah")
    token = create_token(req.username)
    return {
        "token":    token,
        "username": req.username,
        "role":     user["role"]   # kirim role ke frontend
    }

@app.post("/api/logout")
async def logout(session: dict = Depends(get_session),
                 credentials: HTTPAuthorizationCredentials = Depends(security)):
    sessions.pop(credentials.credentials, None)
    return {"message": "Logged out"}

@app.get("/api/me")
async def me(session: dict = Depends(get_session)):
    return {"username": session["username"], "role": session["role"]}

# ---- Devices ----
@app.get("/api/devices")
async def get_devices(session: dict = Depends(get_session)):
    now = time.time()
    result = []
    for dev_id, dev in devices.items():
        try:
            last_ts = datetime.fromisoformat(dev.get("last_seen","")).timestamp()
            online = (now - last_ts) < 30
        except: online = False
        result.append({**dev, "device_id": dev_id, "online": online})
    return result

@app.get("/api/devices/{device_id}")
async def get_device(device_id: str, session: dict = Depends(get_session)):
    if device_id not in devices:
        raise HTTPException(status_code=404, detail="Device tidak ditemukan")
    return devices[device_id]

@app.get("/api/devices/{device_id}/history")
async def get_history(device_id: str, limit: int = 50, session: dict = Depends(get_session)):
    return list(sensor_history.get(device_id, []))[-limit:]

# ---- Command — semua role bisa ping, hanya admin bisa relay/restart ----
@app.post("/api/command")
async def send_command(req: CommandRequest, session: dict = Depends(get_session)):
    # Viewer hanya boleh ping dan get_sensor
    viewer_allowed = {"ping", "get_sensor"}
    if session["role"] != "admin" and req.action not in viewer_allowed:
        raise HTTPException(
            status_code=403,
            detail=f"Akses ditolak — action '{req.action}' hanya untuk admin"
        )
    topic   = f"esp32/{req.device_id}/command"
    payload = {"action": req.action}
    if req.pin   is not None: payload["pin"]   = req.pin
    if req.state is not None: payload["state"] = req.state
    result = mqtt_client.publish(topic, json.dumps(payload))
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        raise HTTPException(status_code=500, detail="Gagal kirim command ke MQTT")
    return {"message": "Command dikirim", "topic": topic, "payload": payload}

@app.get("/api/stats")
async def get_stats(session: dict = Depends(get_session)):
    total  = len(devices)
    online = sum(1 for d in devices.values() if d.get("online", False))
    temps  = [d["temp"] for d in devices.values() if "temp" in d]
    avg_t  = round(sum(temps)/len(temps), 1) if temps else 0
    return {"total": total, "online": online, "offline": total-online, "avg_temp": avg_t}

# ---- User management — hanya admin ----
@app.get("/api/users")
async def get_users(session: dict = Depends(require_admin)):
    return [
        {"username": u, "role": v["role"]}
        for u, v in USERS.items()
    ]

@app.post("/api/users")
async def add_user(req: AddUserRequest, session: dict = Depends(require_admin)):
    if req.username in USERS:
        raise HTTPException(status_code=400, detail="Username sudah ada")
    if req.role not in ("admin", "viewer"):
        raise HTTPException(status_code=400, detail="Role harus 'admin' atau 'viewer'")
    USERS[req.username] = {"password": req.password, "role": req.role}
    return {"message": f"User '{req.username}' berhasil ditambahkan", "role": req.role}

@app.delete("/api/users/{username}")
async def delete_user(username: str, session: dict = Depends(require_admin)):
    if username not in USERS:
        raise HTTPException(status_code=404, detail="User tidak ditemukan")
    if username == session["username"]:
        raise HTTPException(status_code=400, detail="Tidak bisa hapus akun sendiri")
    del USERS[username]
    # Hapus semua sesi user ini
    to_del = [t for t, s in sessions.items() if s["username"] == username]
    for t in to_del: del sessions[t]
    return {"message": f"User '{username}' dihapus"}

# ---- WebSocket ----
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = ""):
    if token not in sessions:
        await websocket.close(code=4001)
        return
    await websocket.accept()
    active_ws.append(websocket)
    try:
        await websocket.send_json({
            "type": "init",
            "devices": list(devices.values()),
            "role": sessions[token]["role"]   # kirim role via WS juga
        })
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_ws.remove(websocket)

# Serve frontend
try:
    app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
except:
    @app.get("/")
    async def root(): return {"message": "ESP32 Dashboard API v2.0"}
