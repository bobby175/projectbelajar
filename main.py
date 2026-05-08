"""
ESP32 Dashboard - FastAPI Backend
==================================
Menghubungkan HiveMQ Cloud (MQTT) dengan dashboard web via REST API + WebSocket.

Install:
    pip install fastapi uvicorn paho-mqtt python-dotenv

Jalankan:
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload

Deploy ke Railway/Render:
    - Set environment variables (lihat .env.example)
    - Procfile: web: uvicorn main:app --host 0.0.0.0 --port $PORT
"""

import asyncio
import json
import os
import time
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
# CONFIG - Isi via environment variables atau .env
# ============================================================
MQTT_HOST     = os.getenv("MQTT_HOST", "xxxx.s1.eu.hivemq.cloud")
MQTT_PORT     = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USER     = os.getenv("MQTT_USER", "username_anda")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "password_anda")

# JWT sederhana - ganti dengan secret yang kuat di production
API_SECRET    = os.getenv("API_SECRET", "rahasia-super-kuat-ganti-ini")
ADMIN_USER    = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS    = os.getenv("ADMIN_PASS", "admin123")
# ============================================================

app = FastAPI(title="ESP32 Dashboard API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Produksi: ganti dengan domain spesifik
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- In-memory storage (ganti Redis/DB untuk produksi) ----
devices: dict = {}           # device_id -> info terakhir
sensor_history: dict = defaultdict(lambda: deque(maxlen=100))
active_ws: list = []         # WebSocket connections aktif
sessions: dict = {}          # token -> username

# ---- Auth sederhana (token-based) ----
security = HTTPBearer(auto_error=False)

class LoginRequest(BaseModel):
    username: str
    password: str

class CommandRequest(BaseModel):
    device_id: str
    action: str
    pin: Optional[int] = None
    state: Optional[bool] = None

def create_token(username: str) -> str:
    import hashlib
    raw = f"{username}:{time.time()}:{API_SECRET}"
    token = hashlib.sha256(raw.encode()).hexdigest()
    sessions[token] = {"username": username, "created": time.time()}
    return token

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Token diperlukan")
    token = credentials.credentials
    if token not in sessions:
        raise HTTPException(status_code=401, detail="Token tidak valid")
    return sessions[token]["username"]

# ---- MQTT Client ----
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
        topic = msg.topic
        payload = json.loads(msg.payload.decode())
        parts = topic.split("/")
        
        if len(parts) < 3:
            return
        
        device_id = parts[1]
        msg_type  = parts[2]

        if msg_type == "status":
            devices[device_id] = {
                **devices.get(device_id, {}),
                **payload,
                "last_seen": datetime.now().isoformat(),
                "online": True,
            }
            
        elif msg_type == "sensor":
            devices[device_id] = {
                **devices.get(device_id, {}),
                **payload,
                "last_seen": datetime.now().isoformat(),
                "online": True,
            }
            sensor_history[device_id].append({
                **payload,
                "time": datetime.now().isoformat()
            })

        elif msg_type == "response":
            pass  # bisa log atau proses lebih lanjut

        # Broadcast ke semua WebSocket client
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
        try:
            await ws.send_json(data)
        except Exception:
            dead.append(ws)
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
        print(f"🔌 Connecting MQTT: {MQTT_HOST}:{MQTT_PORT}")
    except Exception as e:
        print(f"MQTT connect error: {e}")

@app.on_event("startup")
async def startup():
    setup_mqtt()

@app.on_event("shutdown")
async def shutdown():
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

# ============================================================
# REST API ENDPOINTS
# ============================================================

@app.post("/api/login")
async def login(req: LoginRequest):
    if req.username == ADMIN_USER and req.password == ADMIN_PASS:
        token = create_token(req.username)
        return {"token": token, "username": req.username}
    raise HTTPException(status_code=401, detail="Username atau password salah")

@app.post("/api/logout")
async def logout(username: str = Depends(verify_token), credentials: HTTPAuthorizationCredentials = Depends(security)):
    sessions.pop(credentials.credentials, None)
    return {"message": "Logged out"}

@app.get("/api/devices")
async def get_devices(username: str = Depends(verify_token)):
    now = time.time()
    result = []
    for dev_id, dev in devices.items():
        # Tandai offline jika tidak ada update 30 detik
        last = dev.get("last_seen", "")
        try:
            last_ts = datetime.fromisoformat(last).timestamp()
            online = (now - last_ts) < 30
        except Exception:
            online = False
        result.append({**dev, "device_id": dev_id, "online": online})
    return result

@app.get("/api/devices/{device_id}")
async def get_device(device_id: str, username: str = Depends(verify_token)):
    if device_id not in devices:
        raise HTTPException(status_code=404, detail="Device tidak ditemukan")
    return devices[device_id]

@app.get("/api/devices/{device_id}/history")
async def get_history(device_id: str, limit: int = 50, username: str = Depends(verify_token)):
    history = list(sensor_history.get(device_id, []))
    return history[-limit:]

@app.post("/api/command")
async def send_command(req: CommandRequest, username: str = Depends(verify_token)):
    topic = f"esp32/{req.device_id}/command"
    payload = {"action": req.action}
    if req.pin is not None:
        payload["pin"] = req.pin
    if req.state is not None:
        payload["state"] = req.state
    
    result = mqtt_client.publish(topic, json.dumps(payload))
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        raise HTTPException(status_code=500, detail="Gagal kirim command ke MQTT")
    
    return {"message": "Command dikirim", "topic": topic, "payload": payload}

@app.get("/api/stats")
async def get_stats(username: str = Depends(verify_token)):
    total = len(devices)
    online_count = sum(1 for d in devices.values() if d.get("online", False))
    temps = [d["temp"] for d in devices.values() if "temp" in d]
    avg_temp = round(sum(temps) / len(temps), 1) if temps else 0
    return {
        "total": total,
        "online": online_count,
        "offline": total - online_count,
        "avg_temp": avg_temp,
    }

# ---- WebSocket endpoint untuk update realtime ----
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = ""):
    # Validasi token via query param: ws://host/ws?token=xxx
    if token not in sessions:
        await websocket.close(code=4001)
        return
    
    await websocket.accept()
    active_ws.append(websocket)
    print(f"WebSocket connected, total: {len(active_ws)}")
    
    try:
        # Kirim state awal semua device
        await websocket.send_json({"type": "init", "devices": list(devices.values())})
        while True:
            await websocket.receive_text()  # keep-alive ping
    except WebSocketDisconnect:
        active_ws.remove(websocket)
        print(f"WebSocket disconnected, total: {len(active_ws)}")

# Serve static files (frontend)
try:
    app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
except Exception:
    @app.get("/")
    async def root():
        return {"message": "ESP32 Dashboard API - frontend belum di-mount"}
