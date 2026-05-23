import os, json, asyncio, logging
from datetime import date
from contextlib import asynccontextmanager

import asyncpg
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    for attempt in range(10):
        try:
            db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
            await init_db()
            logger.info("DB pool created")
            break
        except Exception as e:
            logger.error(f"DB pool attempt {attempt+1} failed: {e}")
            await asyncio.sleep(5)
    asyncio.create_task(setup_bot())
    yield
    if db_pool:
        await db_pool.close()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

async def init_db():
    async with db_pool.acquire() as conn:
        await conn.execute("""
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
    logger.info("DB initialized")

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

SCHEDULE = {0: "A", 2: "B", 4: "C"}

def get_today_label():
    return SCHEDULE.get(date.today().weekday())

async def get_last_session(user_id: int, day_label: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT exercises, session_date FROM workout_sessions
            WHERE user_id=$1 AND day_label=$2
            ORDER BY session_date DESC LIMIT 1
        """, user_id, day_label)
        return dict(row) if row else None

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
    workout = {**PROGRAM[day_label], "exercises": [dict(e) for e in PROGRAM[day_label]["exercises"]]}
    last = await get_last_session(user_id, day_label)
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
    workout = {**PROGRAM[day_label], "exercises": [dict(e) for e in PROGRAM[day_label]["exercises"]]}
    last = await get_last_session(user_id, day_label)
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
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO workout_sessions (user_id, day_label, session_date, exercises)
            VALUES ($1, $2, $3, $4)
        """, data.user_id, data.day_label, date.today(), json.dumps(data.exercises))
    return {"ok": True}

@app.get("/api/sessions")
async def get_sessions(user_id: int = AUTHORIZED_USER_ID, limit: int = 50):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, day_label, session_date, exercises FROM workout_sessions
            WHERE user_id=$1 ORDER BY session_date DESC LIMIT $2
        """, user_id, limit)
    return [{"id": r["id"], "day_label": r["day_label"], "session_date": str(r["session_date"]), "exercises": r["exercises"]} for r in rows]

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
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO body_metrics (user_id, metric_date, weight_lbs, body_fat_pct, muscle_mass_pct, chest_cm, waist_cm, arm_cm, notes)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        """, data.user_id, d, data.weight_lbs, data.body_fat_pct, data.muscle_mass_pct,
             data.chest_cm, data.waist_cm, data.arm_cm, data.notes)
    return {"ok": True}

@app.get("/api/metrics")
async def get_metrics(user_id: int = AUTHORIZED_USER_ID, limit: int = 30):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, metric_date, weight_lbs, body_fat_pct, muscle_mass_pct,
                   chest_cm, waist_cm, arm_cm, notes
            FROM body_metrics WHERE user_id=$1
            ORDER BY metric_date DESC LIMIT $2
        """, user_id, limit)
    return [{"id": r["id"], "metric_date": str(r["metric_date"]), "weight_lbs": r["weight_lbs"],
             "body_fat_pct": r["body_fat_pct"], "muscle_mass_pct": r["muscle_mass_pct"],
             "chest_cm": r["chest_cm"], "waist_cm": r["waist_cm"], "arm_cm": r["arm_cm"],
             "notes": r["notes"]} for r in rows]

async def setup_bot():
    await asyncio.sleep(5)
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
    if domain:
        webhook_url = f"https://{domain}/bot/webhook"
        async with httpx.AsyncClient() as client:
            r = await client.post(f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook", json={"url": webhook_url})
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

app.mount("/", StaticFiles(directory="static", html=True), name="static")
