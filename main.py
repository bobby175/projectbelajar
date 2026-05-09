"""
ESP32 Dashboard - FastAPI Backend v3.0
========================================
Fitur baru:
- Fix asyncio.run() di thread MQTT → pakai loop.call_soon_threadsafe
- Device pin config (relay/GPIO) editable via API
- GPS tracker support
- LWT / device offline detection via background task
- Auto-expire sessions

Install:
    pip install fastapi uvicorn paho-mqtt python-dotenv

Jalankan:
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

import asyncio, json, os, time, hashlib
from collections import defaultdict, deque
from datetime import datetime
from typing import Optional, List
import threading

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

# ============================================================
MQTT_HOST     = os.getenv("MQTT_HOST", "ea5c1878aaf1441d86f9a6f2094a4e35.s1.eu.hivemq.cloud")
MQTT_PORT     = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USER     = os.getenv("MQTT_USER", "bubby")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "Wulanchan@1")
API_SECRET    = os.getenv("API_SECRET", "4fff9249b670104c526fbb574768b381e3b820a4f6b36a2162910079810cc922")
OFFLINE_TIMEOUT = int(os.getenv("OFFLINE_TIMEOUT", "30"))  # detik
# ============================================================

def load_users():
    users = {}
    default = os.getenv("USERS", "")
    if default:
        for entry in default.split(","):
            parts = entry.strip().split(":")
            if len(parts) == 3:
                uname, pwd, role = parts
                users[uname.strip()] = {"password": pwd.strip(), "role": role.strip()}
    if not users:
        users = {
            os.getenv("ADMIN_USER","admin"):  {"password": os.getenv("ADMIN_PASS","admin123"),  "role": "admin"},
            os.getenv("VIEWER_USER","viewer"): {"password": os.getenv("VIEWER_PASS","viewer123"), "role": "viewer"},
        }
    return users

USERS = load_users()

app = FastAPI(title="ESP32 Dashboard API", version="3.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ---- Storage ----
devices: dict = {}           # device_id → device info
device_pins: dict = {}       # device_id → [{pin, name, type, state}]
sensor_history: dict = defaultdict(lambda: deque(maxlen=200))
gps_history: dict = defaultdict(lambda: deque(maxlen=500))
active_ws: list = []
sessions: dict = {}
main_loop = None             # asyncio event loop utama

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
    value: Optional[int] = None

class AddUserRequest(BaseModel):
    username: str
    password: str
    role: str = "viewer"

class PinConfig(BaseModel):
    pin: int
    name: str
    type: str = "relay"   # relay | pwm | servo | input
    state: bool = False
    value: int = 0

class DevicePinsUpdate(BaseModel):
    pins: List[PinConfig]

# ---- Auth ----
def create_token(username: str) -> str:
    raw = f"{username}:{time.time()}:{API_SECRET}"
    token = hashlib.sha256(raw.encode()).hexdigest()
    sessions[token] = {"username": username, "role": USERS[username]["role"], "created": time.time()}
    return token

def get_session(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Token diperlukan")
    tok = credentials.credentials
    if tok not in sessions:
        raise HTTPException(status_code=401, detail="Token tidak valid atau expired")
    return sessions[tok]

def require_admin(session: dict = Depends(get_session)):
    if session["role"] != "admin":
        raise HTTPException(status_code=403, detail="Akses ditolak — hanya admin")
    return session

# ---- MQTT Broadcast helper (thread-safe) ----
def schedule_broadcast(data: dict):
    """Dipanggil dari thread MQTT, schedule broadcast ke main loop."""
    if main_loop and not main_loop.is_closed():
        asyncio.run_coroutine_threadsafe(broadcast_ws(data), main_loop)

async def broadcast_ws(data: dict):
    dead = []
    for ws in list(active_ws):
        try: await ws.send_json(data)
        except: dead.append(ws)
    for ws in dead:
        if ws in active_ws: active_ws.remove(ws)

# ---- MQTT ----
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

def on_mqtt_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("✅ MQTT Connected ke HiveMQ Cloud")
        client.subscribe("esp32/+/status")
        client.subscribe("esp32/+/sensor")
        client.subscribe("esp32/+/response")
        client.subscribe("esp32/+/gps")
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
        now_iso   = datetime.now().isoformat()

        if msg_type == "status":
            is_online = payload.get("status","") != "offline"
            devices[device_id] = {
                **devices.get(device_id, {}), **payload,
                "last_seen": now_iso, "online": is_online,
            }
            # Init pin config jika belum ada
            if device_id not in device_pins:
                device_pins[device_id] = [
                    {"pin":26,"name":"Relay 1","type":"relay","state":False,"value":0},
                    {"pin":27,"name":"Relay 2","type":"relay","state":False,"value":0},
                ]

        elif msg_type == "sensor":
            devices[device_id] = {
                **devices.get(device_id, {}), **payload,
                "last_seen": now_iso, "online": True,
            }
            sensor_history[device_id].append({**payload, "time": now_iso})

        elif msg_type == "gps":
            # payload: {lat, lng, speed, altitude, satellites, hdop}
            devices[device_id] = {
                **devices.get(device_id, {}),
                "lat": payload.get("lat"), "lng": payload.get("lng"),
                "speed": payload.get("speed",0), "altitude": payload.get("altitude",0),
                "satellites": payload.get("satellites",0),
                "last_seen": now_iso, "online": True,
                "device_type": "gps_tracker",
            }
            gps_history[device_id].append({**payload, "time": now_iso})

        elif msg_type == "response":
            # Update pin state dari response relay
            if payload.get("action") == "relay_updated" and device_id in device_pins:
                pin_num = payload.get("pin")
                state   = payload.get("state", False)
                for p in device_pins[device_id]:
                    if p["pin"] == pin_num:
                        p["state"] = state

        schedule_broadcast({
            "type": msg_type, "device_id": device_id,
            "data": payload, "timestamp": now_iso
        })

    except Exception as e:
        print(f"MQTT message error: {e}")

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

# ---- Background task: cek device offline ----
async def check_device_timeout():
    while True:
        await asyncio.sleep(10)
        now = time.time()
        for dev_id, dev in list(devices.items()):
            try:
                last_ts  = datetime.fromisoformat(dev.get("last_seen","")).timestamp()
                was_online = dev.get("online", False)
                is_online  = (now - last_ts) < OFFLINE_TIMEOUT
                if was_online and not is_online:
                    devices[dev_id]["online"] = False
                    await broadcast_ws({
                        "type": "status", "device_id": dev_id,
                        "data": {**dev, "online": False, "status": "offline"},
                        "timestamp": datetime.now().isoformat()
                    })
                    print(f"⚠ Device offline: {dev_id}")
            except: pass

@app.on_event("startup")
async def startup():
    global main_loop
    main_loop = asyncio.get_event_loop()
    setup_mqtt()
    asyncio.create_task(check_device_timeout())

@app.on_event("shutdown")
async def shutdown():
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

# ============================================================
# ENDPOINTS
# ============================================================

@app.post("/api/login")
async def login(req: LoginRequest):
    user = USERS.get(req.username)
    if not user or user["password"] != req.password:
        raise HTTPException(status_code=401, detail="Username atau password salah")
    tok = create_token(req.username)
    return {"token": tok, "username": req.username, "role": user["role"]}

@app.post("/api/logout")
async def logout(session=Depends(get_session), credentials=Depends(security)):
    sessions.pop(credentials.credentials, None)
    return {"message": "Logged out"}

@app.get("/api/me")
async def me(session=Depends(get_session)):
    return {"username": session["username"], "role": session["role"]}

# ---- Devices ----
@app.get("/api/devices")
async def get_devices(session=Depends(get_session)):
    now = time.time()
    result = []
    for dev_id, dev in devices.items():
        try:
            last_ts = datetime.fromisoformat(dev.get("last_seen","")).timestamp()
            online  = (now - last_ts) < OFFLINE_TIMEOUT
        except: online = False
        result.append({**dev, "device_id": dev_id, "online": online})
    return result

@app.get("/api/devices/{device_id}")
async def get_device(device_id: str, session=Depends(get_session)):
    if device_id not in devices:
        raise HTTPException(status_code=404, detail="Device tidak ditemukan")
    return {**devices[device_id], "device_id": device_id}

@app.get("/api/devices/{device_id}/history")
async def get_history(device_id: str, limit: int = 100, session=Depends(get_session)):
    return list(sensor_history.get(device_id, []))[-limit:]

@app.get("/api/devices/{device_id}/gps")
async def get_gps_history(device_id: str, limit: int = 200, session=Depends(get_session)):
    return list(gps_history.get(device_id, []))[-limit:]

@app.delete("/api/devices/{device_id}")
async def delete_device(device_id: str, session=Depends(require_admin)):
    """Hapus device dari backend secara permanen."""
    if device_id not in devices:
        raise HTTPException(status_code=404, detail="Device tidak ditemukan")
    # Hapus semua data device
    devices.pop(device_id, None)
    device_pins.pop(device_id, None)
    sensor_history.pop(device_id, None)
    gps_history.pop(device_id, None)
    # Broadcast ke semua dashboard agar device card hilang
    await broadcast_ws({
        "type": "device_deleted",
        "device_id": device_id,
        "timestamp": datetime.now().isoformat()
    })
    print(f"🗑 Device dihapus: {device_id}")
    return {"message": f"Device '{device_id}' berhasil dihapus"}

@app.patch("/api/devices/{device_id}")
async def rename_device(device_id: str, body: dict, session=Depends(require_admin)):
    """Ganti nama device di backend."""
    if device_id not in devices:
        raise HTTPException(status_code=404, detail="Device tidak ditemukan")
    new_name = body.get("name","").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="Nama tidak boleh kosong")
    devices[device_id]["name"] = new_name
    await broadcast_ws({
        "type": "device_updated",
        "device_id": device_id,
        "data": devices[device_id],
        "timestamp": datetime.now().isoformat()
    })
    return {"message": f"Device diubah namanya menjadi '{new_name}'"}

# ---- Pin Config ----
@app.get("/api/devices/{device_id}/pins")
async def get_pins(device_id: str, session=Depends(get_session)):
    return device_pins.get(device_id, [])

def push_pin_config(device_id: str):
    """Push pin config ke ESP32 via MQTT topic pinconfig."""
    topic   = f"esp32/{device_id}/pinconfig"
    payload = json.dumps({"pins": device_pins.get(device_id, [])})
    result  = mqtt_client.publish(topic, payload)
    ok = result.rc == mqtt.MQTT_ERR_SUCCESS
    print(f"{'✅' if ok else '❌'} Push pin config → {topic} ({len(device_pins.get(device_id,[]))} pins)")
    return ok

@app.put("/api/devices/{device_id}/pins")
async def update_pins(device_id: str, req: DevicePinsUpdate, session=Depends(require_admin)):
    device_pins[device_id] = [p.dict() for p in req.pins]
    # Langsung push ke ESP32
    pushed = push_pin_config(device_id)
    return {
        "message": "Pin config diperbarui dan dikirim ke ESP32" if pushed else "Pin config disimpan (ESP32 offline, akan diterima saat reconnect)",
        "pins": device_pins[device_id],
        "pushed_to_device": pushed
    }

@app.post("/api/devices/{device_id}/pins")
async def add_pin(device_id: str, pin: PinConfig, session=Depends(require_admin)):
    if device_id not in device_pins:
        device_pins[device_id] = []
    existing = [p for p in device_pins[device_id] if p["pin"] == pin.pin]
    if existing:
        raise HTTPException(status_code=400, detail=f"Pin {pin.pin} sudah ada")
    device_pins[device_id].append(pin.dict())
    pushed = push_pin_config(device_id)
    return {
        "message": f"Pin {pin.pin} ditambahkan",
        "pushed_to_device": pushed
    }

@app.delete("/api/devices/{device_id}/pins/{pin_num}")
async def delete_pin(device_id: str, pin_num: int, session=Depends(require_admin)):
    if device_id not in device_pins:
        raise HTTPException(status_code=404, detail="Device tidak ditemukan")
    device_pins[device_id] = [p for p in device_pins[device_id] if p["pin"] != pin_num]
    pushed = push_pin_config(device_id)
    return {
        "message": f"Pin {pin_num} dihapus",
        "pushed_to_device": pushed
    }

@app.post("/api/devices/{device_id}/pins/push")
async def push_pins(device_id: str, session=Depends(require_admin)):
    """Push ulang pin config ke ESP32 secara manual."""
    if device_id not in device_pins:
        raise HTTPException(status_code=404, detail="Belum ada pin config")
    pushed = push_pin_config(device_id)
    return {
        "message": "Pin config dikirim ke ESP32" if pushed else "Gagal kirim (ESP32 mungkin offline)",
        "pins": device_pins.get(device_id, []),
        "pushed": pushed
    }

@app.get("/api/devices/{device_id}/pins/request")
async def request_pins_from_device(device_id: str, session=Depends(get_session)):
    """Minta ESP32 kirim balik status pin saat ini."""
    topic   = f"esp32/{device_id}/command"
    payload = json.dumps({"action": "get_pins"})
    mqtt_client.publish(topic, payload)
    return {"message": "Request dikirim ke ESP32"}

# ---- Command ----
@app.post("/api/command")
async def send_command(req: CommandRequest, session=Depends(get_session)):
    viewer_allowed = {"ping", "get_sensor"}
    if session["role"] != "admin" and req.action not in viewer_allowed:
        raise HTTPException(status_code=403, detail=f"Action '{req.action}' hanya untuk admin")
    topic   = f"esp32/{req.device_id}/command"
    payload = {"action": req.action}
    if req.pin   is not None: payload["pin"]   = req.pin
    if req.state is not None: payload["state"] = req.state
    if req.value is not None: payload["value"] = req.value
    result = mqtt_client.publish(topic, json.dumps(payload))
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        raise HTTPException(status_code=500, detail="Gagal kirim command ke MQTT")
    return {"message": "Command dikirim", "payload": payload}

@app.get("/api/stats")
async def get_stats(session=Depends(get_session)):
    now    = time.time()
    total  = len(devices)
    online_devs = []
    for dev_id, dev in devices.items():
        try:
            last_ts = datetime.fromisoformat(dev.get("last_seen","")).timestamp()
            if (now - last_ts) < OFFLINE_TIMEOUT:
                online_devs.append(dev)
        except: pass
    online = len(online_devs)
    temps  = [float(d["temp"]) for d in online_devs if "temp" in d]
    avg_t  = round(sum(temps)/len(temps),1) if temps else 0
    gps_count = sum(1 for d in online_devs if d.get("device_type") == "gps_tracker")
    return {"total": total, "online": online, "offline": total-online, "avg_temp": avg_t, "gps_trackers": gps_count}

# ---- Users ----
@app.get("/api/users")
async def get_users(session=Depends(require_admin)):
    return [{"username": u, "role": v["role"]} for u, v in USERS.items()]

@app.post("/api/users")
async def add_user(req: AddUserRequest, session=Depends(require_admin)):
    if req.username in USERS:
        raise HTTPException(status_code=400, detail="Username sudah ada")
    if req.role not in ("admin","viewer"):
        raise HTTPException(status_code=400, detail="Role harus admin atau viewer")
    USERS[req.username] = {"password": req.password, "role": req.role}
    return {"message": f"User '{req.username}' ditambahkan"}

@app.delete("/api/users/{uname}")
async def delete_user(uname: str, session=Depends(require_admin)):
    if uname not in USERS:
        raise HTTPException(status_code=404, detail="User tidak ditemukan")
    if uname == session["username"]:
        raise HTTPException(status_code=400, detail="Tidak bisa hapus akun sendiri")
    del USERS[uname]
    to_del = [t for t, s in sessions.items() if s["username"] == uname]
    for t in to_del: del sessions[t]
    return {"message": f"User '{uname}' dihapus"}

# ---- WebSocket ----
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = ""):
    if token not in sessions:
        await websocket.close(code=4001); return
    await websocket.accept()
    active_ws.append(websocket)
    try:
        await websocket.send_json({
            "type": "init", "devices": list(devices.values()),
            "role": sessions[token]["role"]
        })
        while True:
            await asyncio.wait_for(websocket.receive_text(), timeout=30)
    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    finally:
        if websocket in active_ws: active_ws.remove(websocket)

try:
    app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
except:
    @app.get("/")
    async def root(): return {"message": "ESP32 Dashboard API v3.0"}
