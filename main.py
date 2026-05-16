"""
ESP32 Dashboard - FastAPI Backend v14.0 PWA
=======================================
Fitur lengkap:
- Persistent storage semua device + GPS
- Multi user admin/viewer
- Rate limiting + login lockout
- Token expire
- Security headers
- Alert/notifikasi suhu
- Jadwal relay otomatis (scheduler)
- Notifikasi Telegram
- Export CSV sensor history
- Geofencing GPS
- Ganti password
- Filter log per device
"""

import asyncio, json, os, time, hashlib, secrets, csv, io, random
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from pathlib import Path
from math import radians, sin, cos, sqrt, atan2

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends, Request, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, StreamingResponse, RedirectResponse
from pydantic import BaseModel

load_dotenv()

# ============================================================
MQTT_HOST       = os.getenv("MQTT_HOST",       "")
MQTT_PORT       = int(os.getenv("MQTT_PORT",   "8883"))
MQTT_USER       = os.getenv("MQTT_USER",       "")
MQTT_PASSWORD   = os.getenv("MQTT_PASSWORD",   "")
API_SECRET      = os.getenv("API_SECRET",      "")
OFFLINE_TIMEOUT = int(os.getenv("OFFLINE_TIMEOUT", "30"))
STORAGE_FILE    = os.getenv("STORAGE_FILE",    "/tmp/esp32_data.json")
AUTOSAVE_INTERVAL  = 30
ALLOWED_ORIGINS    = os.getenv("ALLOWED_ORIGINS",    "*").split(",")
TOKEN_EXPIRE_HOURS = int(os.getenv("TOKEN_EXPIRE_HOURS", "24"))
MAX_LOGIN_ATTEMPTS = int(os.getenv("MAX_LOGIN_ATTEMPTS", "5"))
LOGIN_LOCKOUT_SEC  = int(os.getenv("LOGIN_LOCKOUT_SEC",  "300"))
RATE_LIMIT_RPM     = int(os.getenv("RATE_LIMIT_RPM",     "120"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "")

# Instagram DM notification (Meta Instagram Messaging API)
# Syarat resmi Meta: akun Instagram harus Professional, memakai access token yang benar,
# dan recipient_id adalah IG Scoped ID / PSID dari user yang sudah pernah DM akun Anda.
INSTAGRAM_ACCESS_TOKEN = os.getenv("INSTAGRAM_ACCESS_TOKEN", "")
INSTAGRAM_RECIPIENT_ID = os.getenv("INSTAGRAM_RECIPIENT_ID", "")
INSTAGRAM_ACCOUNT_ID   = os.getenv("INSTAGRAM_ACCOUNT_ID",   "")
INSTAGRAM_API_VERSION  = os.getenv("INSTAGRAM_API_VERSION",  "v24.0")
# Opsional jika endpoint Anda berbeda, misalnya lewat graph.instagram.com atau third-party gateway.
INSTAGRAM_MESSAGES_URL = os.getenv("INSTAGRAM_MESSAGES_URL", "")
# ============================================================

def load_users():
    users = {}
    default = os.getenv("USERS","")
    if default:
        for entry in default.split(","):
            parts = entry.strip().split(":")
            if len(parts)==3:
                u,p,r = parts
                users[u.strip()] = {"password":p.strip(),"role":r.strip()}
    if not users:
        users = {
            os.getenv("ADMIN_USER","admin"):  {"password":os.getenv("ADMIN_PASS","admin123"), "role":"admin"},
            os.getenv("VIEWER_USER","viewer"):{"password":os.getenv("VIEWER_PASS","viewer123"),"role":"viewer"},
        }
    return users

USERS = load_users()

app = FastAPI(title="ESP32 Dashboard API", version="18.0.0",
              docs_url=None, redoc_url=None, openapi_url=None)

app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS,
                   allow_credentials=True, allow_methods=["*"],
                   allow_headers=["Authorization","Content-Type","X-Requested-With"],
                   expose_headers=["X-RateLimit-Remaining"])

# ============================================================
# STORAGE
# ============================================================
devices:           dict = {}
device_pins:       dict = {}
gps_last_location: dict = {}
sensor_history:    dict = defaultdict(lambda: deque(maxlen=500))
gps_history:       dict = defaultdict(lambda: deque(maxlen=500))
alert_config:      dict = {}   # {device_id: {temp_max, temp_min, humid_max, humid_min}}
schedules:         dict = {}   # {schedule_id: {device_id, pin, action, cron_hour, cron_min, enabled}}
geofences:         dict = {}   # {device_id: {lat, lng, radius_m, name, alert_on_exit}}
active_ws:         list = []
sessions:          dict = {}
system_logs:       list = []   # [{time, device, msg, level}]
main_loop                = None
_dirty                   = False
rate_limit_store:  dict = {}
login_attempts:    dict = {}

# Simulator sensor untuk testing dashboard tanpa ESP fisik.
simulator_state: dict = {
    "running": False,
    "device_id": "sim-sensor-01",
    "name": "Simulasi Suhu & Humid",
    "temp_base": 29.0,
    "temp_variation": 3.0,
    "humid_base": 62.0,
    "humid_variation": 12.0,
    "interval_sec": 2.0,
    "started_at": None,
    "uptime": 0,
}

def haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371000.0
    dLat = radians(float(lat2)-float(lat1))
    dLon = radians(float(lon2)-float(lon1))
    a = sin(dLat/2)**2 + cos(radians(float(lat1)))*cos(radians(float(lat2)))*sin(dLon/2)**2
    return R*2*atan2(sqrt(a),sqrt(1-a))

# ============================================================
# PERSISTENT STORAGE
# ============================================================
def load_persistent_data():
    global devices, device_pins, gps_last_location, alert_config, schedules, geofences, sensor_history, gps_history
    if not Path(STORAGE_FILE).exists():
        print(f"📂 Storage baru: {STORAGE_FILE}"); return
    try:
        with open(STORAGE_FILE,"r") as f: data = json.load(f)
        for dev_id,dev in data.get("devices",{}).items():
            devices[dev_id] = {**dev,"online":False,"status":"offline"}
        device_pins       = data.get("device_pins",{})
        gps_last_location = data.get("gps_last_location",{})
        alert_config      = data.get("alert_config",{})
        schedules         = data.get("schedules",{})
        geofences         = data.get("geofences",{})
        # Restore riwayat sensor/GPS agar grafik tidak hilang setelah server restart.
        for dev_id, rows in data.get("sensor_history", {}).items():
            if isinstance(rows, list):
                sensor_history[dev_id] = deque(rows[-500:], maxlen=500)
        for dev_id, rows in data.get("gps_history", {}).items():
            if isinstance(rows, list):
                gps_history[dev_id] = deque(rows[-500:], maxlen=500)
        # Pasang GPS terakhir ke devices
        for dev_id,loc in gps_last_location.items():
            if dev_id in devices:
                devices[dev_id].update({"lat":loc.get("lat"),"lng":loc.get("lng"),
                    "speed":loc.get("speed",0),"altitude":loc.get("altitude",0),
                    "device_type":"gps_tracker","last_gps_time":loc.get("time","")})
        print(f"✅ Restored: {len(devices)} devices, {len(schedules)} schedules, {len(geofences)} geofences, {sum(len(v) for v in sensor_history.values())} sensor points")
        for dev_id,pins in device_pins.items():
            relays=[p for p in pins if p.get("type")=="relay"]
            if relays:
                st=", ".join([f"GPIO{p['pin']}={'ON' if p['state'] else 'OFF'}" for p in relays])
                print(f"   📌 {dev_id}: {st}")
        for dev_id,loc in gps_last_location.items():
            if loc.get("lat"): print(f"   📍 {dev_id}: {loc['lat']:.6f},{loc['lng']:.6f}")
    except Exception as e:
        print(f"⚠ Load error: {e}")

def save_persistent_data(reason=""):
    global _dirty
    try:
        clean = {}
        for dev_id,dev in devices.items():
            clean[dev_id]={k:v for k,v in dev.items() if k!="last_seen"}
        clean_geofences = {k:{kk:vv for kk,vv in v.items() if not kk.startswith("_")} for k,v in geofences.items()}
        clean_schedules = {k:{kk:vv for kk,vv in v.items() if not kk.startswith("_")} for k,v in schedules.items()}
        clean_sensor_history = {k:list(v)[-500:] for k,v in sensor_history.items() if v}
        clean_gps_history    = {k:list(v)[-500:] for k,v in gps_history.items() if v}
        data = {"devices":clean,"device_pins":device_pins,
                "gps_last_location":gps_last_location,"alert_config":alert_config,
                "schedules":clean_schedules,"geofences":clean_geofences,
                "sensor_history":clean_sensor_history,"gps_history":clean_gps_history,
                "saved_at":datetime.now().isoformat(),"reason":reason}
        tmp = STORAGE_FILE+".tmp"
        with open(tmp,"w") as f: json.dump(data,f,indent=2)
        os.replace(tmp,STORAGE_FILE)
        _dirty = False
        if reason: print(f"💾 Saved ({reason})")
    except Exception as e:
        print(f"⚠ Save error: {e}")

def mark_dirty(): global _dirty; _dirty=True

def push_pin_config_to_device(device_id: str) -> bool:
    pins  = device_pins.get(device_id,[])
    topic = f"esp32/{device_id}/pinconfig"
    result= mqtt_client.publish(topic, json.dumps({"pins":pins}))
    ok    = result.rc == mqtt.MQTT_ERR_SUCCESS
    print(f"{'✅' if ok else '❌'} Push pins → {device_id}")
    return ok

# ============================================================
# NOTIFICATIONS: TELEGRAM + INSTAGRAM
# ============================================================
def strip_html(text: str) -> str:
    """Ubah pesan HTML sederhana dari Telegram menjadi teks biasa untuk Instagram."""
    import re
    text = text.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()

async def send_telegram(msg: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {"ok": False, "detail": "Telegram belum dikonfigurasi"}
    try:
        import urllib.request
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = json.dumps({"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML"}).encode()
        req  = urllib.request.Request(url,data=data,headers={"Content-Type":"application/json"})
        with urllib.request.urlopen(req,timeout=10) as r:
            body = r.read().decode("utf-8", "ignore")
        print(f"📱 Telegram: {msg[:50]}")
        return {"ok": True, "response": body}
    except Exception as e:
        print(f"⚠ Telegram error: {e}")
        return {"ok": False, "detail": str(e)}

def instagram_endpoint() -> str:
    if INSTAGRAM_MESSAGES_URL.strip():
        return INSTAGRAM_MESSAGES_URL.strip()
    if INSTAGRAM_ACCOUNT_ID.strip():
        return f"https://graph.facebook.com/{INSTAGRAM_API_VERSION}/{INSTAGRAM_ACCOUNT_ID}/messages"
    return f"https://graph.instagram.com/{INSTAGRAM_API_VERSION}/me/messages"

async def send_instagram(msg: str):
    """Kirim DM Instagram via Meta Messaging API.

    Catatan: Instagram tidak bisa kirim DM bebas ke username biasa. Recipient harus berupa
    IG Scoped ID/PSID dari user yang sudah pernah mengirim DM ke akun IG Professional Anda.
    """
    if not INSTAGRAM_ACCESS_TOKEN or not INSTAGRAM_RECIPIENT_ID:
        return {"ok": False, "detail": "Instagram belum dikonfigurasi"}
    try:
        import urllib.request, urllib.error
        text = strip_html(msg)[:950]
        payload = {"recipient": {"id": INSTAGRAM_RECIPIENT_ID}, "message": {"text": text}}
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            instagram_endpoint(),
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {INSTAGRAM_ACCESS_TOKEN}",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", "ignore")
        print(f"📸 Instagram: {text[:50]}")
        return {"ok": True, "response": body}
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "ignore")
        print(f"⚠ Instagram HTTP error: {detail}")
        return {"ok": False, "detail": detail or str(e)}
    except Exception as e:
        print(f"⚠ Instagram error: {e}")
        return {"ok": False, "detail": str(e)}

async def send_notifications(msg: str):
    """Kirim ke semua channel yang dikonfigurasi. Tidak menggagalkan proses jika salah satu channel error."""
    tg = await send_telegram(msg)
    ig = await send_instagram(msg)
    return {"telegram": tg, "instagram": ig}

# ============================================================
# ALERTS
# ============================================================
alert_sent: dict = {}   # {key: last_sent_time} — anti-spam

async def check_alerts(device_id: str, payload: dict):
    cfg = alert_config.get(device_id,{})
    if not cfg: return
    now  = time.time()
    temp = float(payload.get("temp",0) or 0)
    hum  = float(payload.get("humidity",0) or 0)
    name = devices.get(device_id,{}).get("name", device_id)
    alerts_to_send = []

    def should_send(key):
        last = alert_sent.get(key,0)
        if now-last > 300:  # max 1 alert per 5 menit per kondisi
            alert_sent[key] = now; return True
        return False

    if cfg.get("temp_max") and temp > cfg["temp_max"] and should_send(f"{device_id}_tmax"):
        alerts_to_send.append(f"🌡 <b>SUHU TINGGI</b>\n{name}: {temp}°C (batas: {cfg['temp_max']}°C)")
    if cfg.get("temp_min") and temp < cfg["temp_min"] and should_send(f"{device_id}_tmin"):
        alerts_to_send.append(f"❄️ <b>SUHU RENDAH</b>\n{name}: {temp}°C (batas: {cfg['temp_min']}°C)")
    if cfg.get("humid_max") and hum > cfg["humid_max"] and should_send(f"{device_id}_hmax"):
        alerts_to_send.append(f"💧 <b>KELEMBABAN TINGGI</b>\n{name}: {hum}% (batas: {cfg['humid_max']}%)")
    if cfg.get("humid_min") and hum < cfg["humid_min"] and should_send(f"{device_id}_hmin"):
        alerts_to_send.append(f"🏜 <b>KELEMBABAN RENDAH</b>\n{name}: {hum}% (batas: {cfg['humid_min']}%)")

    for msg in alerts_to_send:
        add_system_log(device_id, msg.replace("<b>","").replace("</b>",""), "warn")
        await send_notifications(msg)
        await broadcast_ws({"type":"alert","device_id":device_id,"message":msg,"timestamp":datetime.now().isoformat()})

async def check_geofence(device_id: str, lat, lng):
    gf = geofences.get(device_id)
    if not gf or not lat or not lng: return
    dist = haversine(gf["lat"], gf["lng"], lat, lng)
    name = devices.get(device_id,{}).get("name", device_id)
    inside = dist <= gf["radius_m"]
    was_inside = gf.get("_inside", True)
    gf["_inside"] = inside
    now = time.time()
    key = f"{device_id}_geo"

    if gf.get("alert_on_exit") and was_inside and not inside:
        if time.time()-alert_sent.get(key,0) > 60:
            alert_sent[key] = now
            msg = f"🚨 <b>GEOFENCE EXIT</b>\n{name} keluar area {gf.get('name','')}\nJarak: {dist:.0f}m dari pusat"
            add_system_log(device_id, f"Keluar geofence {gf.get('name','')} — jarak {dist:.0f}m", "warn")
            await send_notifications(msg)
            await broadcast_ws({"type":"geofence_exit","device_id":device_id,"distance":dist,"timestamp":datetime.now().isoformat()})

    if not gf.get("alert_on_exit") and not was_inside and inside:
        if time.time()-alert_sent.get(key+"_in",0) > 60:
            alert_sent[key+"_in"] = now
            msg = f"✅ <b>GEOFENCE ENTER</b>\n{name} masuk area {gf.get('name','')}"
            await send_notifications(msg)

# ============================================================
# SYSTEM LOG
# ============================================================
def add_system_log(device: str, msg: str, level: str="info"):
    system_logs.insert(0,{"time":datetime.now().isoformat(),"device":device,"msg":msg,"level":level})
    if len(system_logs)>500: system_logs.pop()

# ============================================================
# SCHEDULER
# ============================================================
async def scheduler_task():
    """Jalankan jadwal relay otomatis setiap menit."""
    while True:
        await asyncio.sleep(30)
        now = datetime.now()
        for sch_id, sch in schedules.items():
            if not sch.get("enabled",False): continue
            if now.weekday() not in sch.get("days", [0,1,2,3,4,5,6]): continue
            if sch.get("cron_hour") != now.hour: continue
            if sch.get("cron_min")  != now.minute: continue
            # Cek apakah sudah dijalankan menit ini
            last_run = sch.get("_last_run","")
            current_slot = now.strftime("%Y-%m-%d %H:%M")
            if last_run == current_slot: continue
            sch["_last_run"] = current_slot
            # Kirim command
            device_id = sch["device_id"]
            pin       = sch["pin"]
            state     = sch["action"] == "on"
            topic     = f"esp32/{device_id}/command"
            payload   = {"action":"set_relay","pin":pin,"state":state}
            mqtt_client.publish(topic, json.dumps(payload))
            # Update state di pin config
            for p in device_pins.get(device_id,[]):
                if p["pin"]==pin: p["state"]=state
            save_persistent_data(f"scheduler: {device_id} GPIO{pin} {'ON' if state else 'OFF'}")
            add_system_log(device_id, f"⏰ Jadwal: GPIO{pin} → {'ON' if state else 'OFF'}", "ok")
            await broadcast_ws({"type":"scheduler","device_id":device_id,"pin":pin,"state":state,"schedule_id":sch_id,"timestamp":datetime.now().isoformat()})
            print(f"⏰ Scheduler: {device_id} GPIO{pin} → {'ON' if state else 'OFF'}")

# ============================================================
# AUTH
# ============================================================
security = HTTPBearer(auto_error=False)

class LoginRequest(BaseModel):
    username: str
    password: str

class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str

class CommandRequest(BaseModel):
    device_id: str
    action:    str
    pin:       Optional[int]  = None
    state:     Optional[bool] = None
    value:     Optional[int]  = None

class AddUserRequest(BaseModel):
    username: str
    password: str
    role:     str = "viewer"

class DeviceCreateRequest(BaseModel):
    device_id:   str
    name:        str
    type:        str = "custom"
    device_type: Optional[str] = None
    chip:        Optional[str] = None

class PinConfig(BaseModel):
    pin:   int
    name:  str
    type:  str  = "relay"
    state: bool = False
    value: int  = 0

class DevicePinsUpdate(BaseModel):
    pins: List[PinConfig]

class AlertConfig(BaseModel):
    temp_max:  Optional[float] = None
    temp_min:  Optional[float] = None
    humid_max: Optional[float] = None
    humid_min: Optional[float] = None
    enabled:   bool = True

class ScheduleConfig(BaseModel):
    device_id:  str
    pin:        int
    action:     str   # "on" | "off"
    cron_hour:  int   # 0-23
    cron_min:   int   # 0-59
    label:      str = ""
    enabled:    bool = True
    days:       List[int] = [0,1,2,3,4,5,6]  # 0=Senin

class SimulatorConfig(BaseModel):
    device_id:       str = "sim-sensor-01"
    name:            str = "Simulasi Suhu & Humid"
    temp_base:       float = 29.0
    temp_variation:  float = 3.0
    humid_base:      float = 62.0
    humid_variation: float = 12.0
    interval_sec:    float = 2.0

class PhoneGPSPush(BaseModel):
    device_id: str = "phone-gps-01"
    name:      str = "GPS HP Saya"
    lat:       float
    lng:       float
    speed:     Optional[float] = 0.0      # km/h dari frontend
    altitude:  Optional[float] = 0.0
    accuracy:  Optional[float] = None
    heading:   Optional[float] = None
    time:      Optional[str] = None

class PhoneGPSStop(BaseModel):
    device_id: str = "phone-gps-01"

class GeofenceConfig(BaseModel):
    lat:          float
    lng:          float
    radius_m:     float = 100.0
    name:         str   = "Area"
    alert_on_exit:bool  = True

def create_token(username: str) -> str:
    nonce  = secrets.token_hex(16)
    raw    = f"{username}:{time.time()}:{nonce}:{API_SECRET}"
    token  = hashlib.sha256(raw.encode()).hexdigest()
    expire = time.time()+(TOKEN_EXPIRE_HOURS*3600)
    sessions[token] = {"username":username,"role":USERS[username]["role"],
                       "created":time.time(),"expires":expire,"nonce":nonce}
    return token

def cleanup_expired_sessions():
    now  = time.time()
    dead = [t for t,s in sessions.items() if s.get("expires",0)<now]
    for t in dead: del sessions[t]

def get_session(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Token diperlukan")
    tok = credentials.credentials
    if tok not in sessions:
        raise HTTPException(status_code=401, detail="Token tidak valid atau expired")
    sess = sessions[tok]
    if sess.get("expires",0) < time.time():
        del sessions[tok]
        raise HTTPException(status_code=401, detail="Token expired — silakan login ulang")
    return sess

def require_admin(session=Depends(get_session)):
    if session["role"]!="admin":
        raise HTTPException(status_code=403, detail="Akses ditolak — hanya admin")
    return session

def get_client_ip(request: Request) -> str:
    fwd = request.headers.get("X-Forwarded-For")
    if fwd: return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

def check_rate_limit(ip: str) -> bool:
    now=time.time(); window=60
    if ip not in rate_limit_store: rate_limit_store[ip]=[]
    rate_limit_store[ip]=[t for t in rate_limit_store[ip] if now-t<window]
    if len(rate_limit_store[ip])>=RATE_LIMIT_RPM: return False
    rate_limit_store[ip].append(now); return True

def check_login_lockout(ip: str):
    now=time.time()
    if ip not in login_attempts: return False,0
    d=login_attempts[ip]
    if d.get("locked_until",0)>now: return True,int(d["locked_until"]-now)
    return False,0

def record_login_failure(ip: str):
    now=time.time()
    if ip not in login_attempts: login_attempts[ip]={"count":0,"locked_until":0}
    login_attempts[ip]["count"]+=1
    if login_attempts[ip]["count"]>=MAX_LOGIN_ATTEMPTS:
        login_attempts[ip]["locked_until"]=now+LOGIN_LOCKOUT_SEC
        login_attempts[ip]["count"]=0

def reset_login_attempts(ip: str): login_attempts.pop(ip,None)

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if request.url.path in ("/","/health") or request.url.path.startswith("/static"):
        return await call_next(request)
    ip=get_client_ip(request)
    if not check_rate_limit(ip):
        return JSONResponse(status_code=429,content={"detail":"Rate limit exceeded"},headers={"Retry-After":"60"})
    resp=await call_next(request)
    remaining=RATE_LIMIT_RPM-len(rate_limit_store.get(ip,[]))
    resp.headers["X-RateLimit-Remaining"]=str(max(0,remaining))
    return resp

@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    resp=await call_next(request)
    resp.headers["X-Content-Type-Options"]="nosniff"
    resp.headers["X-Frame-Options"]="DENY"
    resp.headers["X-XSS-Protection"]="1; mode=block"
    resp.headers["Referrer-Policy"]="strict-origin-when-cross-origin"
    resp.headers["Cache-Control"]="no-store"
    try: del resp.headers["Server"]
    except: pass
    return resp

# ============================================================
# MQTT
# ============================================================
def schedule_broadcast(data: dict):
    if main_loop and not main_loop.is_closed():
        asyncio.run_coroutine_threadsafe(broadcast_ws(data), main_loop)

def schedule_coroutine(coro):
    if main_loop and not main_loop.is_closed():
        asyncio.run_coroutine_threadsafe(coro, main_loop)

async def broadcast_ws(data: dict):
    dead=[]
    for ws in list(active_ws):
        try: await ws.send_json(data)
        except: dead.append(ws)
    for ws in dead:
        if ws in active_ws: active_ws.remove(ws)

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

def on_mqtt_connect(client, userdata, flags, reason_code, properties):
    if reason_code==0:
        print("✅ MQTT Connected")
        for topic in ["esp32/+/status","esp32/+/sensor","esp32/+/response","esp32/+/gps"]:
            client.subscribe(topic)
    else:
        print(f"❌ MQTT gagal: {reason_code}")

def on_mqtt_message(client, userdata, msg):
    try:
        payload  = json.loads(msg.payload.decode())
        parts    = msg.topic.split("/")
        if len(parts)<3: return
        device_id= parts[1]
        msg_type = parts[2]
        now_iso  = datetime.now().isoformat()

        if msg_type=="status":
            is_online  = payload.get("status","")!="offline"
            was_offline= not devices.get(device_id,{}).get("online",False)
            devices[device_id]={**devices.get(device_id,{}),**payload,"last_seen":now_iso,"online":is_online}
            if is_online:
                if device_id not in device_pins:
                    device_pins[device_id]=[
                        {"pin":4,"name":"Relay 1","type":"relay","state":False,"value":0},
                        {"pin":5,"name":"Relay 2","type":"relay","state":False,"value":0},
                    ]
                if was_offline:
                    print(f"⚡ Reconnect: {device_id}")
                    push_pin_config_to_device(device_id)
                    name=devices[device_id].get("name",device_id)
                    schedule_coroutine(send_notifications(f"✅ <b>Device Online</b>\n{name} terhubung kembali"))
                    add_system_log(device_id,"Device online","ok")
            else:
                name=devices.get(device_id,{}).get("name",device_id)
                schedule_coroutine(send_notifications(f"⚠️ <b>Device Offline</b>\n{name} tidak merespons"))
                add_system_log(device_id,"Device offline","warn")
            mark_dirty()

        elif msg_type=="sensor":
            devices[device_id]={**devices.get(device_id,{}),**payload,"last_seen":now_iso,"online":True}
            sensor_history[device_id].append({**payload,"time":now_iso})
            mark_dirty()
            schedule_coroutine(check_alerts(device_id, payload))

        elif msg_type=="gps":
            lat=payload.get("lat"); lng=payload.get("lng")
            devices[device_id]={**devices.get(device_id,{}),
                "lat":lat,"lng":lng,"speed":payload.get("speed",0),
                "altitude":payload.get("altitude",0),"satellites":payload.get("satellites",0),
                "last_seen":now_iso,"online":True,"device_type":"gps_tracker","last_gps_time":now_iso}
            gps_history[device_id].append({**payload,"time":now_iso})
            if lat and lng:
                gps_last_location[device_id]={**payload,"time":now_iso}
                mark_dirty()
                schedule_coroutine(check_geofence(device_id,lat,lng))

        elif msg_type=="response":
            if payload.get("action")=="relay_updated" and device_id in device_pins:
                pin_num=payload.get("pin"); state=payload.get("state",False)
                changed=False
                for p in device_pins[device_id]:
                    if p["pin"]==pin_num and p["state"]!=state:
                        p["state"]=state; changed=True
                if changed:
                    save_persistent_data(f"relay GPIO{pin_num}={'ON' if state else 'OFF'} on {device_id}")

        schedule_broadcast({"type":msg_type,"device_id":device_id,"data":payload,"timestamp":now_iso})

    except Exception as e:
        print(f"MQTT error: {e}")

def setup_mqtt():
    mqtt_client.username_pw_set(MQTT_USER,MQTT_PASSWORD)
    mqtt_client.tls_set()
    mqtt_client.on_connect=on_mqtt_connect
    mqtt_client.on_message=on_mqtt_message
    try:
        mqtt_client.connect(MQTT_HOST,MQTT_PORT,60)
        mqtt_client.loop_start()
    except Exception as e:
        print(f"MQTT error: {e}")

# ============================================================
# BACKGROUND TASKS
# ============================================================
async def check_device_timeout():
    while True:
        await asyncio.sleep(10)
        now=time.time()
        for dev_id,dev in list(devices.items()):
            try:
                last_ts=datetime.fromisoformat(dev.get("last_seen","")).timestamp()
                was_online=dev.get("online",False)
                is_online=(now-last_ts)<OFFLINE_TIMEOUT
                if was_online and not is_online:
                    devices[dev_id]["online"]=False
                    await broadcast_ws({"type":"status","device_id":dev_id,
                        "data":{**dev,"online":False,"status":"offline"},"timestamp":datetime.now().isoformat()})
            except: pass

async def auto_save_task():
    while True:
        await asyncio.sleep(AUTOSAVE_INTERVAL)
        if _dirty: save_persistent_data("auto-save")
        cleanup_expired_sessions()
        now=time.time()
        for ip in list(rate_limit_store.keys()):
            rate_limit_store[ip]=[t for t in rate_limit_store[ip] if now-t<60]
            if not rate_limit_store[ip]: del rate_limit_store[ip]

async def simulator_task():
    """Generate suhu dan humidity palsu untuk mengetes grafik dashboard tanpa ESP."""
    phase = 0.0
    while True:
        interval = max(1.0, float(simulator_state.get("interval_sec", 2.0) or 2.0))
        await asyncio.sleep(interval)
        if not simulator_state.get("running"):
            continue

        phase += interval / 8.0
        device_id = simulator_state.get("device_id") or "sim-sensor-01"
        now_iso = datetime.now().isoformat()
        simulator_state["uptime"] = int(time.time() - (simulator_state.get("started_at") or time.time()))

        temp_base = float(simulator_state.get("temp_base", 29.0))
        temp_var  = abs(float(simulator_state.get("temp_variation", 3.0)))
        hum_base  = float(simulator_state.get("humid_base", 62.0))
        hum_var   = abs(float(simulator_state.get("humid_variation", 12.0)))

        temp = round(temp_base + sin(phase) * temp_var + random.uniform(-0.35, 0.35), 1)
        hum  = round(max(0, min(100, hum_base + cos(phase * 0.7) * hum_var + random.uniform(-1.5, 1.5))), 1)
        rssi = random.randint(-70, -42)
        payload = {"temp": temp, "humidity": hum, "rssi": rssi, "uptime": simulator_state["uptime"]}

        devices[device_id] = {
            **devices.get(device_id, {}),
            **payload,
            "device_id": device_id,
            "name": simulator_state.get("name") or "Simulasi Suhu & Humid",
            "type": "sensor",
            "device_type": "sensor",
            "chip": "SIMULATOR",
            "online": True,
            "status": "online",
            "simulated": True,
            "last_seen": now_iso,
        }
        sensor_history[device_id].append({**payload, "time": now_iso})
        mark_dirty()
        await check_alerts(device_id, payload)
        await broadcast_ws({"type":"sensor","device_id":device_id,"data":payload,"timestamp":now_iso,"simulated":True})

@app.on_event("startup")
async def startup():
    global main_loop
    main_loop=asyncio.get_event_loop()
    load_persistent_data()
    setup_mqtt()
    asyncio.create_task(check_device_timeout())
    asyncio.create_task(auto_save_task())
    asyncio.create_task(scheduler_task())
    asyncio.create_task(simulator_task())
    print("🚀 ESP32 Dashboard API v8.0 ready")

@app.on_event("shutdown")
async def shutdown():
    save_persistent_data("shutdown")
    mqtt_client.loop_stop(); mqtt_client.disconnect()

# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/health")
async def health(): return {"status":"ok","version":"18.0.0"}

# ---- AUTH ----
@app.post("/api/login")
async def login(req: LoginRequest, request: Request):
    ip=get_client_ip(request)
    locked,remaining=check_login_lockout(ip)
    if locked:
        raise HTTPException(status_code=429,detail=f"Terlalu banyak percobaan. Tunggu {remaining} detik.")
    user=USERS.get(req.username)
    if not user or user["password"]!=req.password:
        record_login_failure(ip)
        raise HTTPException(status_code=401,detail="Kredensial tidak valid")
    reset_login_attempts(ip)
    cleanup_expired_sessions()
    tok=create_token(req.username)
    expires_at=datetime.fromtimestamp(sessions[tok]["expires"]).isoformat()
    add_system_log("System",f"Login: {req.username} dari {ip}","ok")
    return {"token":tok,"username":req.username,"role":user["role"],"expires_at":expires_at}

@app.post("/api/logout")
async def logout(session=Depends(get_session),credentials=Depends(security)):
    sessions.pop(credentials.credentials,None)
    return {"message":"Logged out"}

@app.get("/api/me")
async def me(session=Depends(get_session)):
    return {"username":session["username"],"role":session["role"]}

@app.post("/api/change-password")
async def change_password(req: ChangePasswordRequest, session=Depends(get_session)):
    username=session["username"]
    user=USERS.get(username)
    if not user or user["password"]!=req.old_password:
        raise HTTPException(status_code=401,detail="Password lama salah")
    if len(req.new_password)<6:
        raise HTTPException(status_code=400,detail="Password minimal 6 karakter")
    USERS[username]["password"]=req.new_password
    # Logout semua sesi user ini kecuali yang sekarang
    add_system_log("System",f"Password diubah: {username}","ok")
    return {"message":"Password berhasil diubah"}

# ---- DEVICES ----
@app.get("/api/devices")
async def get_devices(session=Depends(get_session)):
    now=time.time(); result=[]
    for dev_id,dev in devices.items():
        try:
            last_ts=datetime.fromisoformat(dev.get("last_seen","")).timestamp()
            online=(now-last_ts)<OFFLINE_TIMEOUT
        except: online=False
        result.append({**dev,"device_id":dev_id,"online":online})
    return result

@app.post("/api/devices")
async def create_device(req: DeviceCreateRequest, session=Depends(require_admin)):
    device_id = req.device_id.strip()
    if not device_id:
        raise HTTPException(status_code=400, detail="Device ID wajib diisi")
    if device_id in devices:
        raise HTTPException(status_code=400, detail="Device ID sudah ada")
    dtype = req.device_type or req.type
    devices[device_id] = {
        "name": req.name.strip() or device_id,
        "type": req.type,
        "device_type": dtype,
        "chip": req.chip or "manual",
        "status": "offline",
        "online": False,
        "created_at": datetime.now().isoformat(),
    }
    if device_id not in device_pins:
        device_pins[device_id] = []
    save_persistent_data(f"create device {device_id}")
    await broadcast_ws({"type":"device_updated","device_id":device_id,"data":devices[device_id],"timestamp":datetime.now().isoformat()})
    return {"message":"Device ditambahkan","device_id":device_id,"device":devices[device_id]}

@app.get("/api/devices/{device_id}")
async def get_device(device_id: str, session=Depends(get_session)):
    if device_id not in devices:
        raise HTTPException(status_code=404,detail="Device tidak ditemukan")
    return {**devices[device_id],"device_id":device_id}

@app.delete("/api/devices/{device_id}")
async def delete_device(device_id: str, session=Depends(require_admin)):
    if device_id not in devices:
        raise HTTPException(status_code=404,detail="Device tidak ditemukan")
    for d in [devices,device_pins,gps_last_location,alert_config,geofences]:
        d.pop(device_id,None)
    sensor_history.pop(device_id,None); gps_history.pop(device_id,None)
    save_persistent_data(f"delete {device_id}")
    await broadcast_ws({"type":"device_deleted","device_id":device_id,"timestamp":datetime.now().isoformat()})
    return {"message":f"Device '{device_id}' dihapus"}

@app.patch("/api/devices/{device_id}")
async def update_device(device_id: str, body: dict, session=Depends(require_admin)):
    if device_id not in devices:
        raise HTTPException(status_code=404,detail="Device tidak ditemukan")
    if "name" in body and body["name"].strip():
        devices[device_id]["name"]=body["name"].strip()
    save_persistent_data(f"update {device_id}")
    await broadcast_ws({"type":"device_updated","device_id":device_id,"data":devices[device_id],"timestamp":datetime.now().isoformat()})
    return {"message":"Device diperbarui"}

# ---- HISTORY ----
@app.get("/api/devices/{device_id}/history")
async def get_history(device_id: str, limit: int=100, session=Depends(get_session)):
    return list(sensor_history.get(device_id,[]))[-limit:]

@app.get("/api/devices/{device_id}/history/export")
async def export_history_csv(device_id: str, session=Depends(get_session)):
    """Export riwayat sensor ke CSV."""
    hist=list(sensor_history.get(device_id,[]))
    if not hist:
        raise HTTPException(status_code=404,detail="Tidak ada data")
    output=io.StringIO()
    writer=csv.DictWriter(output,fieldnames=["time","temp","humidity","rssi","uptime"])
    writer.writeheader()
    for row in hist:
        writer.writerow({"time":row.get("time",""),"temp":row.get("temp",""),
            "humidity":row.get("humidity",""),"rssi":row.get("rssi",""),"uptime":row.get("uptime","")})
    output.seek(0)
    name=devices.get(device_id,{}).get("name",device_id).replace(" ","_")
    filename=f"{name}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    return StreamingResponse(iter([output.getvalue()]),media_type="text/csv",
        headers={"Content-Disposition":f"attachment; filename={filename}"})

# ---- GPS ----
@app.get("/api/devices/{device_id}/gps")
async def get_gps_history(device_id: str, limit: int=200, session=Depends(get_session)):
    return list(gps_history.get(device_id,[]))[-limit:]

@app.get("/api/devices/{device_id}/gps/last")
async def get_last_gps(device_id: str, session=Depends(get_session)):
    loc=gps_last_location.get(device_id)
    if not loc: raise HTTPException(status_code=404,detail="Belum ada GPS data")
    return {"device_id":device_id,**loc}

@app.get("/api/gps/all")
async def get_all_last_gps(session=Depends(get_session)):
    result=[]
    for dev_id,loc in gps_last_location.items():
        name=devices.get(dev_id,{}).get("name",dev_id)
        online=devices.get(dev_id,{}).get("online",False)
        result.append({"device_id":dev_id,"name":name,"online":online,**loc})
    return result

# ---- PINS ----
@app.get("/api/devices/{device_id}/pins")
async def get_pins(device_id: str, session=Depends(get_session)):
    return device_pins.get(device_id,[])

@app.put("/api/devices/{device_id}/pins")
async def update_pins(device_id: str, req: DevicePinsUpdate, session=Depends(require_admin)):
    if len(req.pins) > 20:
        raise HTTPException(status_code=400, detail="Maksimal 20 pin per device")
    if device_id not in devices:
        devices[device_id] = {"name":device_id,"type":"custom","device_type":"custom","status":"offline","online":False,"created_at":datetime.now().isoformat()}
    device_pins[device_id]=[p.dict() for p in req.pins]
    save_persistent_data(f"update pins {device_id}")
    pushed=push_pin_config_to_device(device_id)
    return {"message":"Pin config disimpan","pins":device_pins[device_id],"pushed":pushed}

@app.post("/api/devices/{device_id}/pins")
async def add_pin(device_id: str, pin: PinConfig, session=Depends(require_admin)):
    if device_id not in device_pins: device_pins[device_id]=[]
    if any(p["pin"]==pin.pin for p in device_pins[device_id]):
        raise HTTPException(status_code=400,detail=f"Pin {pin.pin} sudah ada")
    device_pins[device_id].append(pin.dict())
    save_persistent_data(f"add pin GPIO{pin.pin} to {device_id}")
    pushed=push_pin_config_to_device(device_id)
    return {"message":f"Pin {pin.pin} ditambahkan","pushed":pushed}

@app.delete("/api/devices/{device_id}/pins/{pin_num}")
async def delete_pin(device_id: str, pin_num: int, session=Depends(require_admin)):
    if device_id not in device_pins:
        raise HTTPException(status_code=404,detail="Device tidak ditemukan")
    device_pins[device_id]=[p for p in device_pins[device_id] if p["pin"]!=pin_num]
    save_persistent_data(f"delete pin GPIO{pin_num} from {device_id}")
    pushed=push_pin_config_to_device(device_id)
    return {"message":f"Pin {pin_num} dihapus","pushed":pushed}

@app.post("/api/devices/{device_id}/pins/push")
async def push_pins(device_id: str, session=Depends(require_admin)):
    if device_id not in device_pins:
        raise HTTPException(status_code=404,detail="Belum ada pin config")
    pushed=push_pin_config_to_device(device_id)
    return {"message":"Config dikirim" if pushed else "Gagal (offline)","pushed":pushed}

# ---- COMMAND ----
@app.post("/api/command")
async def send_command(req: CommandRequest, session=Depends(get_session)):
    viewer_allowed={"ping","get_sensor"}
    if session["role"]!="admin" and req.action not in viewer_allowed:
        raise HTTPException(status_code=403,detail=f"Action '{req.action}' hanya untuk admin")
    topic=f"esp32/{req.device_id}/command"
    payload={"action":req.action}
    if req.pin   is not None: payload["pin"]=req.pin
    if req.state is not None: payload["state"]=req.state
    if req.value is not None: payload["value"]=req.value
    if req.action=="set_relay" and req.pin is not None and req.state is not None:
        for p in device_pins.get(req.device_id,[]):
            if p["pin"]==req.pin: p["state"]=req.state
        save_persistent_data(f"cmd relay GPIO{req.pin}={'ON' if req.state else 'OFF'} on {req.device_id}")
    if req.action=="set_pwm" and req.pin is not None and req.value is not None:
        for p in device_pins.get(req.device_id,[]):
            if p["pin"]==req.pin: p["value"]=req.value
        save_persistent_data(f"cmd value GPIO{req.pin}={req.value} on {req.device_id}")
    result=mqtt_client.publish(topic,json.dumps(payload))
    if result.rc!=mqtt.MQTT_ERR_SUCCESS:
        raise HTTPException(status_code=500,detail="Gagal kirim MQTT")
    return {"message":"Command dikirim","payload":payload}

# ---- STATS ----
@app.get("/api/stats")
async def get_stats(session=Depends(get_session)):
    now=time.time(); online_devs=[]
    for dev_id,dev in devices.items():
        try:
            last_ts=datetime.fromisoformat(dev.get("last_seen","")).timestamp()
            if (now-last_ts)<OFFLINE_TIMEOUT: online_devs.append(dev)
        except: pass
    total=len(devices); online=len(online_devs)
    temps=[float(d["temp"]) for d in online_devs if "temp" in d]
    avg_t=round(sum(temps)/len(temps),1) if temps else 0
    gps_c=sum(1 for d in online_devs if d.get("device_type")=="gps_tracker")
    return {"total":total,"online":online,"offline":total-online,"avg_temp":avg_t,"gps_trackers":gps_c}

# ---- SENSOR SIMULATOR ----
@app.get("/api/simulator/status")
async def simulator_status(session=Depends(get_session)):
    return {**simulator_state}

@app.post("/api/simulator/start")
async def start_simulator(cfg: SimulatorConfig, session=Depends(require_admin)):
    if not cfg.device_id.strip():
        raise HTTPException(status_code=400, detail="Device ID simulasi wajib diisi")
    if cfg.interval_sec < 1 or cfg.interval_sec > 60:
        raise HTTPException(status_code=400, detail="Interval simulasi harus 1-60 detik")
    simulator_state.update({
        "running": True,
        "device_id": cfg.device_id.strip(),
        "name": cfg.name.strip() or "Simulasi Suhu & Humid",
        "temp_base": cfg.temp_base,
        "temp_variation": cfg.temp_variation,
        "humid_base": cfg.humid_base,
        "humid_variation": cfg.humid_variation,
        "interval_sec": cfg.interval_sec,
        "started_at": time.time(),
        "uptime": 0,
    })
    device_id = simulator_state["device_id"]
    now_iso = datetime.now().isoformat()
    devices[device_id] = {
        **devices.get(device_id, {}),
        "device_id": device_id,
        "name": simulator_state["name"],
        "type": "sensor",
        "device_type": "sensor",
        "chip": "SIMULATOR",
        "online": True,
        "status": "online",
        "simulated": True,
        "last_seen": now_iso,
        "temp": simulator_state["temp_base"],
        "humidity": simulator_state["humid_base"],
        "rssi": -55,
        "uptime": 0,
    }
    add_system_log(device_id, "Simulator sensor dimulai", "ok")
    await broadcast_ws({"type":"status","device_id":device_id,"data":devices[device_id],"timestamp":now_iso})
    return {"message":"Simulator dimulai", **simulator_state}

@app.post("/api/simulator/stop")
async def stop_simulator(session=Depends(require_admin)):
    simulator_state["running"] = False
    device_id = simulator_state.get("device_id") or "sim-sensor-01"
    if device_id in devices:
        devices[device_id]["online"] = False
        devices[device_id]["status"] = "offline"
        devices[device_id]["last_seen"] = datetime.now().isoformat()
    add_system_log(device_id, "Simulator sensor dihentikan", "warn")
    await broadcast_ws({"type":"status","device_id":device_id,"data":devices.get(device_id,{"status":"offline"}),"timestamp":datetime.now().isoformat()})
    return {"message":"Simulator dihentikan", **simulator_state}

# ---- PHONE GPS SIMULATOR ----
@app.post("/api/simulator/gps/push")
async def push_phone_gps(req: PhoneGPSPush, session=Depends(require_admin)):
    """Terima lokasi GPS dari browser HP dan tampilkan sebagai tracker GPS virtual."""
    device_id = (req.device_id or "phone-gps-01").strip()
    name = (req.name or "GPS HP Saya").strip()
    if not device_id:
        raise HTTPException(status_code=400, detail="Device ID GPS HP wajib diisi")
    if req.lat < -90 or req.lat > 90 or req.lng < -180 or req.lng > 180:
        raise HTTPException(status_code=400, detail="Koordinat GPS tidak valid")
    now_iso = datetime.now().isoformat()
    gps_time = req.time or now_iso
    payload = {
        "lat": round(float(req.lat), 7),
        "lng": round(float(req.lng), 7),
        "speed": round(float(req.speed or 0), 2),
        "altitude": round(float(req.altitude or 0), 2),
        "accuracy": None if req.accuracy is None else round(float(req.accuracy), 1),
        "heading": req.heading,
        "time": gps_time,
        "source": "phone_gps",
    }
    devices[device_id] = {
        **devices.get(device_id, {}),
        **payload,
        "device_id": device_id,
        "name": name,
        "type": "gps_tracker",
        "device_type": "gps_tracker",
        "chip": "PHONE-GPS",
        "online": True,
        "status": "online",
        "simulated": True,
        "last_seen": now_iso,
        "last_gps_time": gps_time,
    }
    gps_last_location[device_id] = {**payload, "time": gps_time}
    gps_history[device_id].append({**payload, "time": gps_time})
    mark_dirty()
    await check_geofence(device_id, payload["lat"], payload["lng"])
    await broadcast_ws({"type":"gps","device_id":device_id,"data":payload,"timestamp":now_iso,"simulated":True})
    return {"message":"GPS HP diterima", "device":devices[device_id]}

@app.post("/api/simulator/gps/stop")
async def stop_phone_gps(req: PhoneGPSStop, session=Depends(require_admin)):
    device_id=(req.device_id or "phone-gps-01").strip()
    now_iso=datetime.now().isoformat()
    if device_id in devices:
        devices[device_id]["online"] = False
        devices[device_id]["status"] = "offline"
        devices[device_id]["last_seen"] = now_iso
    add_system_log(device_id, "GPS HP dihentikan", "warn")
    await broadcast_ws({"type":"status","device_id":device_id,"data":devices.get(device_id,{"status":"offline"}),"timestamp":now_iso})
    return {"message":"GPS HP dihentikan", "device_id":device_id}

# ---- ALERTS ----
@app.get("/api/devices/{device_id}/alert")
async def get_alert(device_id: str, session=Depends(get_session)):
    return alert_config.get(device_id,{})

@app.put("/api/devices/{device_id}/alert")
async def set_alert(device_id: str, cfg: AlertConfig, session=Depends(require_admin)):
    alert_config[device_id]=cfg.dict()
    save_persistent_data(f"alert config {device_id}")
    return {"message":"Alert config disimpan","config":cfg.dict()}

@app.delete("/api/devices/{device_id}/alert")
async def delete_alert(device_id: str, session=Depends(require_admin)):
    alert_config.pop(device_id,None)
    save_persistent_data(f"delete alert {device_id}")
    return {"message":"Alert dihapus"}

# ---- SCHEDULES ----
@app.get("/api/schedules")
async def get_schedules(session=Depends(get_session)):
    return [{"id":k,**{kk:vv for kk,vv in v.items() if not kk.startswith("_")}} for k,v in schedules.items()]

def validate_schedule(cfg: ScheduleConfig):
    if cfg.action not in ("on", "off"):
        raise HTTPException(status_code=400, detail="Action jadwal harus 'on' atau 'off'")
    if not (0 <= cfg.cron_hour <= 23) or not (0 <= cfg.cron_min <= 59):
        raise HTTPException(status_code=400, detail="Jam/menit jadwal tidak valid")
    if cfg.pin < 0 or cfg.pin > 39:
        raise HTTPException(status_code=400, detail="GPIO pin tidak valid")
    if not cfg.device_id:
        raise HTTPException(status_code=400, detail="Device wajib dipilih")
    if cfg.days and any((d < 0 or d > 6) for d in cfg.days):
        raise HTTPException(status_code=400, detail="Hari aktif tidak valid")

@app.post("/api/schedules")
async def add_schedule(cfg: ScheduleConfig, session=Depends(require_admin)):
    validate_schedule(cfg)
    sch_id=f"sch_{int(time.time())}_{secrets.token_hex(4)}"
    schedules[sch_id]={**cfg.dict(),"_last_run":""}
    save_persistent_data("add schedule")
    await broadcast_ws({"type":"schedule_updated","schedule_id":sch_id,"timestamp":datetime.now().isoformat()})
    return {"message":"Jadwal ditambahkan","id":sch_id}

@app.put("/api/schedules/{sch_id}")
async def update_schedule(sch_id: str, cfg: ScheduleConfig, session=Depends(require_admin)):
    if sch_id not in schedules:
        raise HTTPException(status_code=404,detail="Jadwal tidak ditemukan")
    validate_schedule(cfg)
    schedules[sch_id]={**cfg.dict(),"_last_run":schedules[sch_id].get("_last_run","")}
    save_persistent_data("update schedule")
    await broadcast_ws({"type":"schedule_updated","schedule_id":sch_id,"timestamp":datetime.now().isoformat()})
    return {"message":"Jadwal diperbarui"}

@app.delete("/api/schedules/{sch_id}")
async def delete_schedule(sch_id: str, session=Depends(require_admin)):
    schedules.pop(sch_id,None)
    save_persistent_data("delete schedule")
    await broadcast_ws({"type":"schedule_deleted","schedule_id":sch_id,"timestamp":datetime.now().isoformat()})
    return {"message":"Jadwal dihapus"}

# ---- GEOFENCE ----
@app.get("/api/devices/{device_id}/geofence")
async def get_geofence(device_id: str, session=Depends(get_session)):
    return geofences.get(device_id,{})

@app.put("/api/devices/{device_id}/geofence")
async def set_geofence(device_id: str, cfg: GeofenceConfig, session=Depends(require_admin)):
    if not (-90 <= cfg.lat <= 90):
        raise HTTPException(status_code=400, detail="Latitude harus antara -90 sampai 90")
    if not (-180 <= cfg.lng <= 180):
        raise HTTPException(status_code=400, detail="Longitude harus antara -180 sampai 180")
    if cfg.radius_m < 10 or cfg.radius_m > 10000:
        raise HTTPException(status_code=400, detail="Radius harus 10 sampai 10000 meter")
    data = cfg.dict()
    # Status awal inside dihitung dari lokasi GPS terakhir jika ada, agar alert exit/enter lebih akurat.
    loc = gps_last_location.get(device_id) or devices.get(device_id, {})
    inside = True
    if loc.get("lat") and loc.get("lng"):
        try:
            inside = haversine(cfg.lat, cfg.lng, float(loc.get("lat")), float(loc.get("lng"))) <= cfg.radius_m
        except Exception:
            inside = True
    geofences[device_id] = {**data, "_inside": inside}
    save_persistent_data(f"geofence {device_id}")
    await broadcast_ws({"type":"geofence_updated","device_id":device_id,"data":data,"timestamp":datetime.now().isoformat()})
    return {"message":"Geofence disimpan","config":data}

@app.delete("/api/devices/{device_id}/geofence")
async def delete_geofence(device_id: str, session=Depends(require_admin)):
    geofences.pop(device_id,None)
    save_persistent_data(f"delete geofence {device_id}")
    await broadcast_ws({"type":"geofence_deleted","device_id":device_id,"timestamp":datetime.now().isoformat()})
    return {"message":"Geofence dihapus"}

# ---- LOGS ----
@app.get("/api/logs")
async def get_logs(device_id: Optional[str]=None, limit: int=100, session=Depends(get_session)):
    logs=system_logs
    if device_id: logs=[l for l in logs if l.get("device")==device_id]
    return logs[:limit]

# ---- NOTIFICATION TEST ----
@app.post("/api/telegram/test")
async def test_telegram(session=Depends(require_admin)):
    result = await send_telegram("🔔 <b>Test Notifikasi</b>\nESP32 Dashboard berfungsi dengan baik!")
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("detail", "Telegram gagal"))
    return {"message":"Pesan tes dikirim ke Telegram", "result": result}

@app.post("/api/instagram/test")
async def test_instagram(session=Depends(require_admin)):
    result = await send_instagram("🔔 Test Notifikasi\nESP32 Dashboard berfungsi dengan baik!")
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("detail", "Instagram gagal"))
    return {"message":"Pesan tes dikirim ke Instagram", "result": result}

@app.post("/api/notify/test")
async def test_all_notifications(session=Depends(require_admin)):
    result = await send_notifications("🔔 <b>Test Notifikasi</b>\nESP32 Dashboard berfungsi dengan baik!")
    ok = any(v.get("ok") for v in result.values())
    if not ok:
        raise HTTPException(status_code=400, detail=result)
    return {"message":"Test notifikasi diproses", "result": result}

# ---- USERS ----
@app.get("/api/users")
async def get_users(session=Depends(require_admin)):
    return [{"username":u,"role":v["role"]} for u,v in USERS.items()]

@app.post("/api/users")
async def add_user(req: AddUserRequest, session=Depends(require_admin)):
    if req.username in USERS:
        raise HTTPException(status_code=400,detail="Username sudah ada")
    if req.role not in ("admin","viewer"):
        raise HTTPException(status_code=400,detail="Role harus admin atau viewer")
    USERS[req.username]={"password":req.password,"role":req.role}
    return {"message":f"User '{req.username}' ditambahkan"}

@app.delete("/api/users/{uname}")
async def delete_user(uname: str, session=Depends(require_admin)):
    if uname not in USERS: raise HTTPException(status_code=404,detail="User tidak ditemukan")
    if uname==session["username"]: raise HTTPException(status_code=400,detail="Tidak bisa hapus akun sendiri")
    del USERS[uname]
    to_del=[t for t,s in sessions.items() if s["username"]==uname]
    for t in to_del: del sessions[t]
    return {"message":f"User '{uname}' dihapus"}

# ---- STORAGE ----
@app.get("/api/storage")
async def get_storage_info(session=Depends(require_admin)):
    info={"file":STORAGE_FILE,"exists":Path(STORAGE_FILE).exists()}
    if info["exists"]:
        stat=Path(STORAGE_FILE).stat()
        info.update({"size_kb":round(stat.st_size/1024,1),
            "modified":datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "devices":len(devices),"pin_configs":len(device_pins),
            "gps_locations":len(gps_last_location),"schedules":len(schedules),
            "sensor_points":sum(len(v) for v in sensor_history.values()),"gps_points":sum(len(v) for v in gps_history.values())})
    return info

@app.post("/api/storage/save")
async def manual_save(session=Depends(require_admin)):
    save_persistent_data("manual save")
    return {"message":"Data disimpan","file":STORAGE_FILE}

# ---- WEBSOCKET ----
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str=""):
    if token not in sessions:
        await websocket.close(code=4001); return
    await websocket.accept()
    active_ws.append(websocket)
    try:
        await websocket.send_json({"type":"init","devices":list(devices.values()),
            "role":sessions[token]["role"],"schedules":len(schedules),"geofences":len(geofences)})
        while True:
            await asyncio.wait_for(websocket.receive_text(),timeout=30)
    except (WebSocketDisconnect,asyncio.TimeoutError): pass
    finally:
        if websocket in active_ws: active_ws.remove(websocket)

@app.get("/", include_in_schema=False)
async def root_redirect():
    return RedirectResponse(url="/index.html", status_code=307)

try:
    app.mount("/",StaticFiles(directory="frontend",html=True),name="frontend")
except Exception:
    @app.get("/fallback", include_in_schema=False)
    async def fallback_root(): return {"status":"ok","version":"18.0.0","pwa":True}
