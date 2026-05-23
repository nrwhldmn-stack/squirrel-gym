import os, json, asyncio, logging
from datetime import date, datetime
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import psycopg2.pool
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
AUTHORIZED_USER_ID = int(os.environ.get("AUTHORIZED_USER_ID", "7955194359"))

db_pool = None
executor = ThreadPoolExecutor(max_workers=4)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = psycopg2.pool.ThreadedConnectionPool(2, 10, DATABASE_URL)
    await run_sync(init_db)
    asyncio.create_task(setup_bot())
    yield
    db_pool.closeall()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

async def run_sync(fn, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, fn, *args)

def db_exec(fn):
    conn = db_pool.getconn()
    try:
        result = fn(conn)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def init_db():
    def _run(conn):
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS workout_sessions (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    day_label TEXT NOT NULL,
                    session_date DATE NOT NULL,
                    exercises JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS body_metrics (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    metric_date DATE NOT NULL,
                    weight_lbs FLOAT,
                    body_fat_pct FLOAT,
                    muscle_mass_pct FLOAT,
                    chest_cm FLOAT,
                    waist_cm FLOAT,
                    arm_cm FLOAT,
                    notes TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
    db_exec(_run)
    logger.info("DB initialized")

# --- THE PROGRAM ---
PROGRAM = {
    "A": {
        "name": "Chest + Triceps + Shoulders",
        "day": "Monday",
        "exercises": [
            {"id": "bench_press", "name": "DB Bench Press", "sets": 4, "reps": "8-10", "notes": "Full range, controlled"},
            {"id": "incline_press", "name": "Incline DB Press", "sets": 3, "reps": "10-12", "notes": "45° angle"},
            {"id": "db_flyes", "name": "DB Flyes", "sets": 3, "reps": "12-15", "notes": "Feel the stretch"},
            {"id": "lateral_raises", "name": "Lateral Raises", "sets": 4, "reps": "15-20", "notes": "Controlled, no swinging"},
            {"id": "overhead_ext", "name": "Overhead Tricep Extension", "sets": 3, "reps": "12", "notes": ""},
            {"id": "tricep_pushdown", "name": "Tricep Pushdown", "sets": 3, "reps": "15", "notes": ""},
        ]
    },
    "B": {
        "name": "Back + Biceps + Rear Delts",
        "day": "Wednesday",
        "exercises": [
            {"id": "pullups", "name": "Pull-Ups / Lat Pulldown", "sets": 4, "reps": "8-10", "notes": "Full ROM"},
            {"id": "single_row", "name": "Single-Arm DB Row", "sets": 4, "reps": "10-12", "notes": "Each side"},
            {"id": "bent_row", "name": "Bent-Over DB Row", "sets": 3, "reps": "10-12", "notes": "Squeeze at top"},
            {"id": "rear_delt_fly", "name": "Rear Delt Flyes", "sets": 3, "reps": "15-20", "notes": ""},
            {"id": "barbell_curl", "name": "DB Bicep Curl", "sets": 3, "reps": "10-12", "notes": "Alternate arms"},
            {"id": "hammer_curl", "name": "Hammer Curls", "sets": 3, "reps": "12-15", "notes": ""},
        ]
    },
    "C": {
        "name": "Shoulders + Arms",
        "day": "Friday",
        "exercises": [
            {"id": "shoulder_press", "name": "Seated DB Shoulder Press", "sets": 4, "reps": "8-10", "notes": ""},
            {"id": "arnold_press", "name": "Arnold Press", "sets": 3, "reps": "10-12", "notes": ""},
            {"id": "lateral_raises_heavy", "name": "Lateral Raises (heavy)", "sets": 4, "reps": "15-20", "notes": "Push the weight"},
            {"id": "db_shrugs", "name": "DB Shrugs", "sets": 3, "reps": "15", "notes": "Hold 1s at top"},
            {"id": "incline_curl", "name": "Incline DB Curl", "sets": 3, "reps": "12", "notes": ""},
            {"id": "close_grip_press", "name": "Close-Grip DB Press", "sets": 3, "reps": "10-12", "notes": ""},
        ]
    }
}

SCHEDULE = {0: "A", 2: "B", 4: "C"}  # Mon=0, Wed=2, Fri=4

def get_today_label():
    return SCHEDULE.get(date.today().weekday())

def _get_last_session(user_id, day_label):
    def _run(conn):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT exercises, session_date FROM workout_sessions
                WHERE user_id=%s AND day_label=%s
                ORDER BY session_date DESC LIMIT 1
            """, (user_id, day_label))
            row = cur.fetchone()
            if row:
                return {"exercises": row[0], "session_date": row[1]}
    return db_exec(_run)

# --- ROUTES ---

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/api/program")
async def get_program():
    return PROGRAM

@app.get("/api/today")
async def get_today(user_id: int = AUTHORIZED_USER_ID):
    day_label = get_today_label()
    if not day_label:
        return {"rest_day": True, "message": "Rest day or Lagree day 🧘"}
    workout = {**PROGRAM[day_label]}
    workout["exercises"] = [dict(e) for e in workout["exercises"]]
    last = await run_sync(_get_last_session, user_id, day_label)
    if last:
        last_map = {ex["exercise_id"]: ex for ex in last["exercises"]}
        for ex in workout["exercises"]:
            prev = last_map.get(ex["id"])
            if prev:
                ex["last_weight"] = prev.get("weight")
                ex["last_reps"] = prev.get("reps")
                ex["last_sets"] = prev.get("sets")
                ex["last_date"] = str(last["session_date"])
    return {"day_label": day_label, "workout": workout, "date": str(date.today())}

@app.get("/api/workout/{day_label}")
async def get_workout(day_label: str, user_id: int = AUTHORIZED_USER_ID):
    if day_label not in PROGRAM:
        raise HTTPException(404, "Invalid day")
    workout = {**PROGRAM[day_label]}
    workout["exercises"] = [dict(e) for e in workout["exercises"]]
    last = await run_sync(_get_last_session, user_id, day_label)
    if last:
        last_map = {ex["exercise_id"]: ex for ex in last["exercises"]}
        for ex in workout["exercises"]:
            prev = last_map.get(ex["id"])
            if prev:
                ex["last_weight"] = prev.get("weight")
                ex["last_reps"] = prev.get("reps")
                ex["last_sets"] = prev.get("sets")
    return {"day_label": day_label, "workout": workout}

class WorkoutLog(BaseModel):
    user_id: int = AUTHORIZED_USER_ID
    day_label: str
    exercises: List[dict]

@app.post("/api/workout/log")
async def log_workout(data: WorkoutLog):
    def _run(conn):
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO workout_sessions (user_id, day_label, session_date, exercises)
                VALUES (%s, %s, %s, %s)
            """, (data.user_id, data.day_label, date.today(), json.dumps(data.exercises)))
    await run_sync(db_exec, _run)
    return {"ok": True}

@app.get("/api/sessions")
async def get_sessions(user_id: int = AUTHORIZED_USER_ID, limit: int = 50):
    def _run(conn):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, day_label, session_date, exercises FROM workout_sessions
                WHERE user_id=%s ORDER BY session_date DESC LIMIT %s
            """, (user_id, limit))
            cols = ["id", "day_label", "session_date", "exercises"]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    rows = await run_sync(db_exec, _run)
    for r in rows:
        r["session_date"] = str(r["session_date"])
    return rows

class MetricsLog(BaseModel):
    user_id: int = AUTHORIZED_USER_ID
    metric_date: Optional[str] = None
    weight_lbs: Optional[float] = None
    body_fat_pct: Optional[float] = None
    muscle_mass_pct: Optional[float] = None
    chest_cm: Optional[float] = None
    waist_cm: Optional[float] = None
    arm_cm: Optional[float] = None
    notes: Optional[str] = None

@app.post("/api/metrics")
async def log_metrics(data: MetricsLog):
    d = date.fromisoformat(data.metric_date) if data.metric_date else date.today()
    def _run(conn):
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO body_metrics (user_id, metric_date, weight_lbs, body_fat_pct, muscle_mass_pct, chest_cm, waist_cm, arm_cm, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (data.user_id, d, data.weight_lbs, data.body_fat_pct, data.muscle_mass_pct,
                  data.chest_cm, data.waist_cm, data.arm_cm, data.notes))
    await run_sync(db_exec, _run)
    return {"ok": True}

@app.get("/api/metrics")
async def get_metrics(user_id: int = AUTHORIZED_USER_ID, limit: int = 30):
    def _run(conn):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, metric_date, weight_lbs, body_fat_pct, muscle_mass_pct,
                       chest_cm, waist_cm, arm_cm, notes
                FROM body_metrics WHERE user_id=%s
                ORDER BY metric_date DESC LIMIT %s
            """, (user_id, limit))
            cols = ["id","metric_date","weight_lbs","body_fat_pct","muscle_mass_pct","chest_cm","waist_cm","arm_cm","notes"]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    rows = await run_sync(db_exec, _run)
    for r in rows:
        r["metric_date"] = str(r["metric_date"])
    return rows

# --- BOT ---
async def setup_bot():
    await asyncio.sleep(5)
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
    if domain:
        webhook_url = f"https://{domain}/bot/webhook"
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
                json={"url": webhook_url}
            )
            logger.info(f"Webhook: {r.json()}")

@app.post("/bot/webhook")
async def bot_webhook(request: Request):
    data = await request.json()
    msg = data.get("message")
    if not msg:
        return {"ok": True}
    chat_id = msg["chat"]["id"]
    text = msg.get("text", "")
    if text.startswith("/start"):
        domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
        app_url = f"https://{domain}"
        await send_tg(chat_id, "🐿️ *Squirrel Gym* — Progressive overload tracker\n\nTap below to open:", {
            "inline_keyboard": [[{"text": "🏋️ Open Squirrel Gym", "web_app": {"url": app_url}}]]
        })
    return {"ok": True}

async def send_tg(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    async with httpx.AsyncClient() as client:
        await client.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json=payload)

# Serve frontend
app.mount("/", StaticFiles(directory="static", html=True), name="static")
