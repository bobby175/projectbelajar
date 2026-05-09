"""
ESP32 Dashboard - FastAPI Backend v4.0
=======================================
Fitur baru v4.0:
- Persistent storage (JSON file) — data tersimpan saat server restart/listrik mati
- Auto restore state relay ke ESP32 saat reconnect
- Auto-save setiap ada perubahan state
- Background auto-save setiap 30 detik

Install:
    pip install fastapi uvicorn paho-mqtt python-dotenv

Jalankan:
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

import asyncio, json, os, time, hashlib
from collections import defaultdict, deque
from datetime import datetime
from typing import Optional, List
from pathlib import Path

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

# ============================================================
# CONFIG
# ============================================================
MQTT_HOST     = os.getenv("MQTT_HOST", "ea5c1878aaf1441d86f9a6f2094a4e35.s1.eu.hivemq.cloud")
MQTT_PORT     = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USER     = os.getenv("MQTT_USER", "bubby")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "Wulanchan@1")
API_SECRET    = os.getenv("API_SECRET", "4fff9249b670104c526fbb574768b381e3b820a4f6b36a2162910079810cc922")
OFFLINE_TIMEOUT = int(os.getenv("OFFLINE_TIMEOUT", "30"))  # detik
# File penyimpanan data — di /tmp agar bisa ditulis di semua platform
STORAGE_FILE    = os.getenv("STORAGE_FILE", "/tmp/esp32_data.json")
AUTOSAVE_INTERVAL = 30  # detik
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
            os.getenv("ADMIN_USER",  "admin"):  {"password": os.getenv("ADMIN_PASS",  "admin123"),  "role": "admin"},
            os.getenv("VIEWER_USER", "viewer"): {"password": os.getenv("VIEWER_PASS", "viewer123"), "role": "viewer"},
        }
    return users

USERS = load_users()

app = FastAPI(title="ESP32 Dashboard API", version="4.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

# ============================================================
# PERSISTENT STORAGE
# ============================================================
devices:          dict = {}
device_pins:      dict = {}   # {device_id: [{pin, name, type, state, value}]}
gps_last_location:dict = {}   # {device_id: {lat, lng, speed, altitude, satellites, time}}
sensor_history:   dict = defaultdict(lambda: deque(maxlen=200))
gps_history:      dict = defaultdict(lambda: deque(maxlen=500))
active_ws:        list = []
sessions:         dict = {}
main_loop                = None
_dirty                   = False   # flag — ada perubahan yang belum disimpan

def load_persistent_data():
    """Load data dari file JSON saat startup."""
    global devices, device_pins
    if not Path(STORAGE_FILE).exists():
        print(f"📂 Storage file tidak ada, mulai dari kosong: {STORAGE_FILE}")
        return
    try:
        with open(STORAGE_FILE, "r") as f:
            data = json.load(f)
        # Restore devices (tanpa sensor history — terlalu besar)
        saved_devices = data.get("devices", {})
        for dev_id, dev in saved_devices.items():
            # Tandai semua device sebagai offline saat pertama load
            devices[dev_id] = {**dev, "online": False, "status": "offline"}
        # Restore pin config + state terakhir
        device_pins = data.get("device_pins", {})
        # Restore lokasi GPS terakhir
        global gps_last_location
        gps_last_location = data.get("gps_last_location", {})
        # Pasang lokasi terakhir ke devices agar langsung tampil di peta
        for dev_id, loc in gps_last_location.items():
            if dev_id in devices:
                devices[dev_id].update({
                    "lat": loc.get("lat"), "lng": loc.get("lng"),
                    "speed": loc.get("speed", 0), "altitude": loc.get("altitude", 0),
                    "satellites": loc.get("satellites", 0),
                    "device_type": "gps_tracker",
                    "last_gps_time": loc.get("time", ""),
                })
        print(f"✅ Data restored: {len(devices)} device(s), {len(device_pins)} pin config(s), {len(gps_last_location)} GPS location(s)")
        # Print state relay yang tersimpan
        for dev_id, pins in device_pins.items():
            relays = [p for p in pins if p.get("type") == "relay"]
            if relays:
                states = ", ".join([f"GPIO{p['pin']}={'ON' if p['state'] else 'OFF'}" for p in relays])
                print(f"   📌 {dev_id}: {states}")
        # Print lokasi GPS terakhir
        for dev_id, loc in gps_last_location.items():
            if loc.get("lat") and loc.get("lng"):
                print(f"   📍 {dev_id}: lat={loc['lat']:.6f}, lng={loc['lng']:.6f} ({loc.get('time','')})")
    except Exception as e:
        print(f"⚠ Gagal load storage: {e} — mulai dari kosong")

def save_persistent_data(reason: str = ""):
    """Simpan data ke file JSON."""
    global _dirty
    try:
        # Bersihkan data sebelum simpan — hanya simpan yang penting
        clean_devices = {}
        for dev_id, dev in devices.items():
            clean_devices[dev_id] = {
                k: v for k, v in dev.items()
                if k not in ("last_seen",)  # tidak simpan last_seen agar tidak stale
            }
        data = {
            "devices":          clean_devices,
            "device_pins":      device_pins,
            "gps_last_location": gps_last_location,
            "saved_at":         datetime.now().isoformat(),
            "reason":           reason,
        }
        # Tulis ke file sementara dulu, lalu rename (atomic write)
        tmp_file = STORAGE_FILE + ".tmp"
        with open(tmp_file, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_file, STORAGE_FILE)
        _dirty = False
        if reason:
            print(f"💾 Data disimpan ({reason}): {len(devices)} device(s)")
    except Exception as e:
        print(f"⚠ Gagal simpan storage: {e}")

def mark_dirty():
    """Tandai ada perubahan yang perlu disimpan."""
    global _dirty
    _dirty = True

def push_saved_state_to_device(device_id: str):
    """Kirim state relay tersimpan ke ESP32 saat reconnect."""
    pins = device_pins.get(device_id, [])
    if not pins:
        return
    # Push semua pin config sekaligus
    topic   = f"esp32/{device_id}/pinconfig"
    payload = json.dumps({"pins": pins})
    result  = mqtt_client.publish(topic, payload)
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        relays = [p for p in pins if p.get("type") == "relay"]
        states = ", ".join([f"GPIO{p['pin']}={'ON' if p['state'] else 'OFF'}" for p in relays])
        print(f"🔄 State restored → {device_id}: {states}")
    else:
        print(f"⚠ Gagal kirim state ke {device_id}")

# ============================================================
# AUTH
# ============================================================
security = HTTPBearer(auto_error=False)

class LoginRequest(BaseModel):
    username: str
    password: str

class CommandRequest(BaseModel):
    device_id: str
    action: str
    pin:   Optional[int]  = None
    state: Optional[bool] = None
    value: Optional[int]  = None

class AddUserRequest(BaseModel):
    username: str
    password: str
    role: str = "viewer"

class PinConfig(BaseModel):
    pin:   int
    name:  str
    type:  str  = "relay"
    state: bool = False
    value: int  = 0

class DevicePinsUpdate(BaseModel):
    pins: List[PinConfig]

def create_token(username: str) -> str:
    raw   = f"{username}:{time.time()}:{API_SECRET}"
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

# ============================================================
# MQTT
# ============================================================
def schedule_broadcast(data: dict):
    if main_loop and not main_loop.is_closed():
        asyncio.run_coroutine_threadsafe(broadcast_ws(data), main_loop)

async def broadcast_ws(data: dict):
    dead = []
    for ws in list(active_ws):
        try: await ws.send_json(data)
        except: dead.append(ws)
    for ws in dead:
        if ws in active_ws: active_ws.remove(ws)

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
        topic     = msg.topic
        payload   = json.loads(msg.payload.decode())
        parts     = topic.split("/")
        if len(parts) < 3: return

        device_id = parts[1]
        msg_type  = parts[2]
        now_iso   = datetime.now().isoformat()

        if msg_type == "status":
            is_online = payload.get("status", "") != "offline"
            was_offline = not devices.get(device_id, {}).get("online", False)

            devices[device_id] = {
                **devices.get(device_id, {}), **payload,
                "last_seen": now_iso, "online": is_online,
            }

            if is_online:
                # ── DEVICE BARU ONLINE ──
                if device_id not in device_pins:
                    # Init default pin config untuk device baru
                    device_pins[device_id] = [
                        {"pin": 4,  "name": "Relay 1", "type": "relay", "state": False, "value": 0},
                        {"pin": 5,  "name": "Relay 2", "type": "relay", "state": False, "value": 0},
                    ]

                # Kirim state tersimpan ke ESP32 saat reconnect
                # (termasuk setelah listrik mati)
                if was_offline:
                    print(f"⚡ Device reconnect: {device_id} — restoring state...")
                    push_saved_state_to_device(device_id)

            mark_dirty()  # tandai perlu disimpan

        elif msg_type == "sensor":
            devices[device_id] = {
                **devices.get(device_id, {}), **payload,
                "last_seen": now_iso, "online": True,
            }
            sensor_history[device_id].append({**payload, "time": now_iso})
            # Tidak mark_dirty di sini — sensor datang terlalu sering

        elif msg_type == "gps":
            lat = payload.get("lat")
            lng = payload.get("lng")
            devices[device_id] = {
                **devices.get(device_id, {}),
                "lat": lat, "lng": lng,
                "speed":      payload.get("speed", 0),
                "altitude":   payload.get("altitude", 0),
                "satellites": payload.get("satellites", 0),
                "last_seen":  now_iso, "online": True,
                "device_type": "gps_tracker",
                "last_gps_time": now_iso,
            }
            gps_history[device_id].append({**payload, "time": now_iso})
            # Simpan lokasi terakhir ke persistent storage
            if lat and lng:
                gps_last_location[device_id] = {
                    "lat":        lat,
                    "lng":        lng,
                    "speed":      payload.get("speed", 0),
                    "altitude":   payload.get("altitude", 0),
                    "satellites": payload.get("satellites", 0),
                    "hdop":       payload.get("hdop", 0),
                    "time":       now_iso,
                }
                mark_dirty()  # tandai perlu auto-save

        elif msg_type == "response":
            if payload.get("action") == "relay_updated" and device_id in device_pins:
                pin_num = payload.get("pin")
                state   = payload.get("state", False)
                changed = False
                for p in device_pins[device_id]:
                    if p["pin"] == pin_num:
                        if p["state"] != state:
                            p["state"] = state
                            changed = True
                if changed:
                    # Simpan langsung saat ada perubahan state relay
                    save_persistent_data(f"relay GPIO{pin_num}={'ON' if state else 'OFF'} on {device_id}")

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

# ============================================================
# BACKGROUND TASKS
# ============================================================
async def check_device_timeout():
    """Tandai device offline jika tidak ada data dalam OFFLINE_TIMEOUT detik."""
    while True:
        await asyncio.sleep(10)
        now = time.time()
        for dev_id, dev in list(devices.items()):
            try:
                last_ts    = datetime.fromisoformat(dev.get("last_seen", "")).timestamp()
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

async def auto_save_task():
    """Auto-save ke file setiap AUTOSAVE_INTERVAL detik jika ada perubahan."""
    while True:
        await asyncio.sleep(AUTOSAVE_INTERVAL)
        if _dirty:
            save_persistent_data("auto-save")

@app.on_event("startup")
async def startup():
    global main_loop
    main_loop = asyncio.get_event_loop()
    # Load data tersimpan terlebih dahulu
    load_persistent_data()
    setup_mqtt()
    asyncio.create_task(check_device_timeout())
    asyncio.create_task(auto_save_task())
    print("🚀 ESP32 Dashboard API v4.0 ready")

@app.on_event("shutdown")
async def shutdown():
    # Simpan data sebelum shutdown
    save_persistent_data("shutdown")
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
            last_ts = datetime.fromisoformat(dev.get("last_seen", "")).timestamp()
            online  = (now - last_ts) < OFFLINE_TIMEOUT
        except: online = False
        result.append({**dev, "device_id": dev_id, "online": online})
    return result

@app.get("/api/devices/{device_id}")
async def get_device(device_id: str, session=Depends(get_session)):
    if device_id not in devices:
        raise HTTPException(status_code=404, detail="Device tidak ditemukan")
    return {**devices[device_id], "device_id": device_id}

@app.delete("/api/devices/{device_id}")
async def delete_device(device_id: str, session=Depends(require_admin)):
    if device_id not in devices:
        raise HTTPException(status_code=404, detail="Device tidak ditemukan")
    devices.pop(device_id, None)
    device_pins.pop(device_id, None)
    sensor_history.pop(device_id, None)
    gps_history.pop(device_id, None)
    save_persistent_data(f"delete device {device_id}")
    await broadcast_ws({"type": "device_deleted", "device_id": device_id, "timestamp": datetime.now().isoformat()})
    return {"message": f"Device '{device_id}' dihapus"}

@app.patch("/api/devices/{device_id}")
async def rename_device(device_id: str, body: dict, session=Depends(require_admin)):
    if device_id not in devices:
        raise HTTPException(status_code=404, detail="Device tidak ditemukan")
    new_name = body.get("name", "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="Nama tidak boleh kosong")
    devices[device_id]["name"] = new_name
    save_persistent_data(f"rename {device_id} → {new_name}")
    await broadcast_ws({"type": "device_updated", "device_id": device_id,
                        "data": devices[device_id], "timestamp": datetime.now().isoformat()})
    return {"message": f"Device diubah namanya menjadi '{new_name}'"}

@app.get("/api/devices/{device_id}/history")
async def get_history(device_id: str, limit: int = 100, session=Depends(get_session)):
    return list(sensor_history.get(device_id, []))[-limit:]

@app.get("/api/devices/{device_id}/gps")
async def get_gps_history(device_id: str, limit: int = 200, session=Depends(get_session)):
    return list(gps_history.get(device_id, []))[-limit:]

@app.get("/api/devices/{device_id}/gps/last")
async def get_last_gps(device_id: str, session=Depends(get_session)):
    """Ambil koordinat GPS terakhir yang tersimpan (persisten)."""
    loc = gps_last_location.get(device_id)
    if not loc:
        raise HTTPException(status_code=404, detail="Belum ada data GPS tersimpan untuk device ini")
    return {"device_id": device_id, **loc}

@app.get("/api/gps/all")
async def get_all_last_gps(session=Depends(get_session)):
    """Ambil lokasi GPS terakhir semua tracker."""
    result = []
    for dev_id, loc in gps_last_location.items():
        name = devices.get(dev_id, {}).get("name", dev_id)
        online = devices.get(dev_id, {}).get("online", False)
        result.append({"device_id": dev_id, "name": name, "online": online, **loc})
    return result

# ---- Pin Config ----
@app.get("/api/devices/{device_id}/pins")
async def get_pins(device_id: str, session=Depends(get_session)):
    return device_pins.get(device_id, [])

def push_pin_config(device_id: str) -> bool:
    topic   = f"esp32/{device_id}/pinconfig"
    payload = json.dumps({"pins": device_pins.get(device_id, [])})
    result  = mqtt_client.publish(topic, payload)
    ok = result.rc == mqtt.MQTT_ERR_SUCCESS
    print(f"{'✅' if ok else '❌'} Push pin config → {topic}")
    return ok

@app.put("/api/devices/{device_id}/pins")
async def update_pins(device_id: str, req: DevicePinsUpdate, session=Depends(require_admin)):
    device_pins[device_id] = [p.dict() for p in req.pins]
    save_persistent_data(f"update pins {device_id}")   # simpan langsung
    pushed = push_pin_config(device_id)
    return {
        "message": "Pin config disimpan & dikirim ke ESP32" if pushed else "Pin config disimpan (ESP32 offline)",
        "pins": device_pins[device_id], "pushed": pushed
    }

@app.post("/api/devices/{device_id}/pins")
async def add_pin(device_id: str, pin: PinConfig, session=Depends(require_admin)):
    if device_id not in device_pins: device_pins[device_id] = []
    if any(p["pin"] == pin.pin for p in device_pins[device_id]):
        raise HTTPException(status_code=400, detail=f"Pin {pin.pin} sudah ada")
    device_pins[device_id].append(pin.dict())
    save_persistent_data(f"add pin GPIO{pin.pin} to {device_id}")
    pushed = push_pin_config(device_id)
    return {"message": f"Pin {pin.pin} ditambahkan", "pushed": pushed}

@app.delete("/api/devices/{device_id}/pins/{pin_num}")
async def delete_pin(device_id: str, pin_num: int, session=Depends(require_admin)):
    if device_id not in device_pins:
        raise HTTPException(status_code=404, detail="Device tidak ditemukan")
    device_pins[device_id] = [p for p in device_pins[device_id] if p["pin"] != pin_num]
    save_persistent_data(f"delete pin GPIO{pin_num} from {device_id}")
    pushed = push_pin_config(device_id)
    return {"message": f"Pin {pin_num} dihapus", "pushed": pushed}

@app.post("/api/devices/{device_id}/pins/push")
async def push_pins(device_id: str, session=Depends(require_admin)):
    """Push ulang state tersimpan ke ESP32 secara manual."""
    if device_id not in device_pins:
        raise HTTPException(status_code=404, detail="Belum ada pin config")
    pushed = push_pin_config(device_id)
    return {"message": "State dikirim ke ESP32" if pushed else "Gagal (ESP32 offline)", "pushed": pushed}

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

    # Update state di memory + simpan langsung untuk relay
    if req.action == "set_relay" and req.pin is not None and req.state is not None:
        pins = device_pins.get(req.device_id, [])
        for p in pins:
            if p["pin"] == req.pin:
                p["state"] = req.state
        save_persistent_data(f"command relay GPIO{req.pin}={'ON' if req.state else 'OFF'} on {req.device_id}")

    result = mqtt_client.publish(topic, json.dumps(payload))
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        raise HTTPException(status_code=500, detail="Gagal kirim command ke MQTT")
    return {"message": "Command dikirim", "payload": payload}

@app.get("/api/stats")
async def get_stats(session=Depends(get_session)):
    now = time.time()
    online_devs = []
    for dev_id, dev in devices.items():
        try:
            last_ts = datetime.fromisoformat(dev.get("last_seen", "")).timestamp()
            if (now - last_ts) < OFFLINE_TIMEOUT: online_devs.append(dev)
        except: pass
    total  = len(devices)
    online = len(online_devs)
    temps  = [float(d["temp"]) for d in online_devs if "temp" in d]
    avg_t  = round(sum(temps)/len(temps), 1) if temps else 0
    gps_c  = sum(1 for d in online_devs if d.get("device_type") == "gps_tracker")
    return {"total": total, "online": online, "offline": total-online, "avg_temp": avg_t, "gps_trackers": gps_c}

# ---- Storage info endpoint ----
@app.get("/api/storage")
async def get_storage_info(session=Depends(require_admin)):
    """Info tentang file storage yang tersimpan."""
    info = {"file": STORAGE_FILE, "exists": Path(STORAGE_FILE).exists()}
    if info["exists"]:
        stat = Path(STORAGE_FILE).stat()
        info["size_kb"]       = round(stat.st_size / 1024, 1)
        info["modified"]      = datetime.fromtimestamp(stat.st_mtime).isoformat()
        info["devices"]       = len(devices)
        info["pin_configs"]   = len(device_pins)
        info["gps_locations"] = len(gps_last_location)
        info["gps_trackers"]  = [
            {"device_id": d, "name": devices.get(d,{}).get("name",d),
             "lat": loc.get("lat"), "lng": loc.get("lng"), "time": loc.get("time","")}
            for d, loc in gps_last_location.items()
        ]
    return info

@app.post("/api/storage/save")
async def manual_save(session=Depends(require_admin)):
    """Simpan data sekarang secara manual."""
    save_persistent_data("manual save")
    return {"message": "Data berhasil disimpan", "file": STORAGE_FILE}

# ---- Users ----
@app.get("/api/users")
async def get_users(session=Depends(require_admin)):
    return [{"username": u, "role": v["role"]} for u, v in USERS.items()]

@app.post("/api/users")
async def add_user(req: AddUserRequest, session=Depends(require_admin)):
    if req.username in USERS:
        raise HTTPException(status_code=400, detail="Username sudah ada")
    if req.role not in ("admin", "viewer"):
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
    async def root(): return {"message": "ESP32 Dashboard API v4.0"}
