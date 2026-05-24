import os, json, asyncio, logging, base64
from datetime import date, datetime
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
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
APP_URL = f"https://{os.environ.get('RAILWAY_PUBLIC_DOMAIN', '')}"

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
            CREATE TABLE IF NOT EXISTS meals (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                meal_date DATE NOT NULL,
                meal_name TEXT NOT NULL,
                description TEXT,
                protein_g FLOAT NOT NULL DEFAULT 0,
                carbs_g FLOAT NOT NULL DEFAULT 0,
                fat_g FLOAT NOT NULL DEFAULT 0,
                calories INT NOT NULL DEFAULT 0,
                logged_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS nutrition_targets (
                user_id BIGINT PRIMARY KEY,
                protein_g INT NOT NULL DEFAULT 160,
                carbs_g INT NOT NULL DEFAULT 180,
                fat_g INT NOT NULL DEFAULT 65,
                calories INT NOT NULL DEFAULT 1900,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS chat_history (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
    logger.info("DB initialized")

# ─── WORKOUT PROGRAM ──────────────────────────────────────────────────────────

PROGRAM = {
    "A": {
        "name": "Chest + Triceps + Shoulders", "day": "Monday",
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
        "name": "Back + Biceps + Rear Delts", "day": "Wednesday",
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
        "name": "Shoulders + Arms", "day": "Friday",
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

# ─── WORKOUT API ──────────────────────────────────────────────────────────────

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

# ─── BODY METRICS API ─────────────────────────────────────────────────────────

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

# ─── NUTRITION API ────────────────────────────────────────────────────────────

@app.get("/api/nutrition/targets")
async def get_targets(user_id: int = AUTHORIZED_USER_ID):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM nutrition_targets WHERE user_id=$1", user_id)
        if row:
            return dict(row)
        return {"user_id": user_id, "protein_g": None, "carbs_g": None, "fat_g": None, "calories": None}

class TargetsUpdate(BaseModel):
    user_id: int = AUTHORIZED_USER_ID
    protein_g: int
    carbs_g: int
    fat_g: int
    calories: int

@app.post("/api/nutrition/targets")
async def set_targets(data: TargetsUpdate):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO nutrition_targets (user_id, protein_g, carbs_g, fat_g, calories)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (user_id) DO UPDATE SET
                protein_g=EXCLUDED.protein_g, carbs_g=EXCLUDED.carbs_g,
                fat_g=EXCLUDED.fat_g, calories=EXCLUDED.calories,
                updated_at=NOW()
        """, data.user_id, data.protein_g, data.carbs_g, data.fat_g, data.calories)
    return {"ok": True}

@app.get("/api/nutrition/today")
async def get_today_nutrition(user_id: int = AUTHORIZED_USER_ID):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, meal_name, description, protein_g, carbs_g, fat_g, calories, logged_at
            FROM meals WHERE user_id=$1 AND meal_date=$2
            ORDER BY logged_at ASC
        """, user_id, date.today())
        targets = await conn.fetchrow("SELECT * FROM nutrition_targets WHERE user_id=$1", user_id)
    
    meals = [{"id": r["id"], "meal_name": r["meal_name"], "description": r["description"],
              "protein_g": r["protein_g"], "carbs_g": r["carbs_g"], "fat_g": r["fat_g"],
              "calories": r["calories"], "logged_at": r["logged_at"].isoformat()} for r in rows]
    
    totals = {
        "protein_g": sum(m["protein_g"] for m in meals),
        "carbs_g": sum(m["carbs_g"] for m in meals),
        "fat_g": sum(m["fat_g"] for m in meals),
        "calories": sum(m["calories"] for m in meals),
    }
    
    return {
        "date": str(date.today()),
        "meals": meals,
        "totals": totals,
        "targets": dict(targets) if targets else None
    }

@app.get("/api/nutrition/history")
async def get_nutrition_history(user_id: int = AUTHORIZED_USER_ID, days: int = 30):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT meal_date,
                   SUM(protein_g) as protein_g,
                   SUM(carbs_g) as carbs_g,
                   SUM(fat_g) as fat_g,
                   SUM(calories) as calories,
                   COUNT(*) as meal_count
            FROM meals WHERE user_id=$1
            GROUP BY meal_date ORDER BY meal_date DESC LIMIT $2
        """, user_id, days)
    return [{"date": str(r["meal_date"]), "protein_g": float(r["protein_g"]),
             "carbs_g": float(r["carbs_g"]), "fat_g": float(r["fat_g"]),
             "calories": float(r["calories"]), "meal_count": r["meal_count"]} for r in rows]

class MealLog(BaseModel):
    user_id: int = AUTHORIZED_USER_ID
    meal_name: str
    description: Optional[str] = None
    protein_g: float
    carbs_g: float
    fat_g: float
    calories: int
    meal_date: Optional[str] = None

@app.post("/api/nutrition/log")
async def log_meal(data: MealLog):
    d = date.fromisoformat(data.meal_date) if data.meal_date else date.today()
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO meals (user_id, meal_date, meal_name, description, protein_g, carbs_g, fat_g, calories)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        """, data.user_id, d, data.meal_name, data.description,
             data.protein_g, data.carbs_g, data.fat_g, data.calories)
    return {"ok": True}

@app.delete("/api/nutrition/meal/{meal_id}")
async def delete_meal(meal_id: int, user_id: int = AUTHORIZED_USER_ID):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM meals WHERE id=$1 AND user_id=$2", meal_id, user_id)
    return {"ok": True}

# ─── AI MACRO ESTIMATION ──────────────────────────────────────────────────────

class EstimateRequest(BaseModel):
    description: Optional[str] = None
    image_base64: Optional[str] = None
    image_media_type: Optional[str] = "image/jpeg"

@app.post("/api/nutrition/estimate")
async def estimate_macros(data: EstimateRequest):
    messages = []
    
    if data.image_base64:
        messages.append({
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": data.image_media_type, "data": data.image_base64}},
                {"type": "text", "text": f"Estimate the macros for this meal.{' Additional context: ' + data.description if data.description else ''} Reply with ONLY a JSON object: {{\"meal_name\": \"...\", \"protein_g\": X, \"carbs_g\": X, \"fat_g\": X, \"calories\": X, \"notes\": \"...\"}}"}
            ]
        })
    else:
        messages.append({
            "role": "user",
            "content": f"Estimate the macros for this meal: {data.description}. Reply with ONLY a JSON object: {{\"meal_name\": \"...\", \"protein_g\": X, \"carbs_g\": X, \"fat_g\": X, \"calories\": X, \"notes\": \"...\"}}"
        })
    
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-opus-4-5", "max_tokens": 300, "messages": messages}
        )
        result = r.json()
    
    text = result["content"][0]["text"].strip()
    # Parse JSON from response
    start = text.find("{")
    end = text.rfind("}") + 1
    macro_data = json.loads(text[start:end])
    return macro_data

# ─── AI COACH ─────────────────────────────────────────────────────────────────

async def get_coach_context(user_id: int) -> str:
    async with db_pool.acquire() as conn:
        # Last 14 days of workouts
        workout_rows = await conn.fetch("""
            SELECT day_label, session_date, exercises FROM workout_sessions
            WHERE user_id=$1 AND session_date >= CURRENT_DATE - 14
            ORDER BY session_date DESC
        """, user_id)
        
        # Today's meals
        meal_rows = await conn.fetch("""
            SELECT meal_name, protein_g, carbs_g, fat_g, calories FROM meals
            WHERE user_id=$1 AND meal_date=CURRENT_DATE
            ORDER BY logged_at ASC
        """, user_id)
        
        # Last 30 days nutrition averages
        nutrition_avg = await conn.fetchrow("""
            SELECT AVG(protein_g) as avg_protein, AVG(calories) as avg_calories
            FROM (
                SELECT meal_date, SUM(protein_g) as protein_g, SUM(calories) as calories
                FROM meals WHERE user_id=$1 AND meal_date >= CURRENT_DATE - 30
                GROUP BY meal_date
            ) daily
        """, user_id)
        
        # Latest metrics
        metrics = await conn.fetchrow("""
            SELECT weight_lbs, body_fat_pct, metric_date FROM body_metrics
            WHERE user_id=$1 ORDER BY metric_date DESC LIMIT 1
        """, user_id)
        
        # Targets
        targets = await conn.fetchrow("SELECT * FROM nutrition_targets WHERE user_id=$1", user_id)
    
    today = date.today().strftime("%A, %B %d")
    ctx = f"Today is {today}.\n\n"
    
    if metrics:
        ctx += f"BODY METRICS (as of {metrics['metric_date']}):\n"
        ctx += f"- Weight: {metrics['weight_lbs']}lbs, Body fat: {metrics['body_fat_pct']}%\n\n"
    
    if targets:
        ctx += f"NUTRITION TARGETS: {targets['calories']} cal, {targets['protein_g']}g protein, {targets['carbs_g']}g carbs, {targets['fat_g']}g fat\n\n"
    
    if meal_rows:
        total_p = sum(r['protein_g'] for r in meal_rows)
        total_cal = sum(r['calories'] for r in meal_rows)
        ctx += f"TODAY'S MEALS ({len(meal_rows)} logged, {total_p:.0f}g protein, {total_cal} cal):\n"
        for m in meal_rows:
            ctx += f"- {m['meal_name']}: {m['protein_g']:.0f}g P / {m['carbs_g']:.0f}g C / {m['fat_g']:.0f}g F / {m['calories']} cal\n"
        ctx += "\n"
    else:
        ctx += "TODAY'S MEALS: None logged yet\n\n"
    
    if nutrition_avg and nutrition_avg['avg_protein']:
        ctx += f"30-DAY AVERAGES: {nutrition_avg['avg_protein']:.0f}g protein/day, {nutrition_avg['avg_calories']:.0f} cal/day\n\n"
    
    if workout_rows:
        ctx += "RECENT WORKOUTS (last 14 days):\n"
        for w in workout_rows:
            exs = [f"{e.get('name','?')} {e.get('weight','')}lbs×{e.get('reps','')}×{e.get('sets','')}s" for e in w['exercises'][:3]]
            ctx += f"- {w['session_date']} Day {w['day_label']}: {', '.join(exs)}...\n"
    
    return ctx

async def get_chat_history(user_id: int, limit: int = 10):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT role, content FROM chat_history
            WHERE user_id=$1 ORDER BY created_at DESC LIMIT $2
        """, user_id, limit)
    return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]

async def save_chat_message(user_id: int, role: str, content: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO chat_history (user_id, role, content) VALUES ($1,$2,$3)
        """, user_id, role, content)
        # Keep last 50 messages only
        await conn.execute("""
            DELETE FROM chat_history WHERE user_id=$1 AND id NOT IN (
                SELECT id FROM chat_history WHERE user_id=$1 ORDER BY created_at DESC LIMIT 50
            )
        """, user_id)

class ChatMessage(BaseModel):
    user_id: int = AUTHORIZED_USER_ID
    message: str
    image_base64: Optional[str] = None
    image_media_type: Optional[str] = "image/jpeg"

@app.post("/api/coach/chat")
async def coach_chat(data: ChatMessage):
    context = await get_coach_context(data.user_id)
    history = await get_chat_history(data.user_id)
    
    system = f"""You are a personal fitness and nutrition coach. You are direct, practical, and data-driven.
You have full context on the user's workouts, nutrition, and body metrics. Use it.

{context}

Keep responses concise — 2-4 sentences unless a detailed explanation is genuinely needed.
Be specific with numbers. Don't hedge excessively. Give clear recommendations."""

    # Build message content
    if data.image_base64:
        user_content = [
            {"type": "image", "source": {"type": "base64", "media_type": data.image_media_type, "data": data.image_base64}},
            {"type": "text", "text": data.message or "What are the macros in this meal?"}
        ]
    else:
        user_content = data.message
    
    messages = history + [{"role": "user", "content": user_content}]
    
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-opus-4-5", "max_tokens": 500, "system": system, "messages": messages}
        )
        result = r.json()
    
    reply = result["content"][0]["text"]
    await save_chat_message(data.user_id, "user", data.message or "sent a photo")
    await save_chat_message(data.user_id, "assistant", reply)
    return {"reply": reply}

# ─── SETUP ────────────────────────────────────────────────────────────────────

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
    photo = msg.get("photo")
    
    if text.startswith("/start"):
        domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
        await send_tg(chat_id, f"🐿️ *Squirrel Gym* — Your fitness \\& nutrition coach\n\nApp: https://{domain}\n\nOr just ask me anything — workouts, nutrition, what to eat, when to train. I have full context on your data.", parse_mode="Markdown")
        return {"ok": True}
    
    # Handle photo (meal logging from TG)
    if photo:
        caption = msg.get("caption", "What are the macros in this meal?")
        await send_tg(chat_id, "📸 Analyzing your meal...")
        
        # Get the largest photo
        file_id = photo[-1]["file_id"]
        async with httpx.AsyncClient() as client:
            file_info = await client.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={file_id}")
            file_path = file_info.json()["result"]["file_path"]
            img_response = await client.get(f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}")
            img_b64 = base64.b64encode(img_response.content).decode()
        
        # Estimate macros
        est = await estimate_macros(EstimateRequest(description=caption, image_base64=img_b64))
        
        confirm_text = (f"🍽️ *{est['meal_name']}*\n"
                       f"Protein: {est['protein_g']}g | Carbs: {est['carbs_g']}g | Fat: {est['fat_g']}g | Cal: {est['calories']}\n"
                       f"_{est.get('notes', '')}_\n\nLogging this meal?")
        
        # Store pending meal in chat history for confirmation
        await save_chat_message(AUTHORIZED_USER_ID, "system", f"PENDING_MEAL:{json.dumps(est)}")
        await send_tg(chat_id, confirm_text, parse_mode="Markdown", reply_markup={
            "inline_keyboard": [[
                {"text": "✅ Log it", "callback_data": f"log_meal"},
                {"text": "❌ Cancel", "callback_data": "cancel_meal"}
            ]]
        })
        return {"ok": True}
    
    # Handle callback queries (button presses)
    if data.get("callback_query"):
        cq = data["callback_query"]
        cq_data = cq.get("data", "")
        cq_chat_id = cq["message"]["chat"]["id"]
        
        if cq_data == "log_meal":
            # Get pending meal from history
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT content FROM chat_history WHERE user_id=$1 AND content LIKE 'PENDING_MEAL:%'
                    ORDER BY created_at DESC LIMIT 1
                """, AUTHORIZED_USER_ID)
            
            if row:
                meal_data = json.loads(row["content"].replace("PENDING_MEAL:", ""))
                await log_meal(MealLog(
                    meal_name=meal_data["meal_name"],
                    description=meal_data.get("notes"),
                    protein_g=meal_data["protein_g"],
                    carbs_g=meal_data["carbs_g"],
                    fat_g=meal_data["fat_g"],
                    calories=meal_data["calories"]
                ))
                summary = await get_today_nutrition(AUTHORIZED_USER_ID)
                t = summary["totals"]
                tgt = summary["targets"]
                tgt_str = f"/{tgt['protein_g']}g" if tgt else ""
                await send_tg(cq_chat_id, f"✅ Logged!\n\n*Today so far:*\nProtein: {t['protein_g']:.0f}g{tgt_str}\nCalories: {t['calories']}", parse_mode="Markdown")
            
        elif cq_data == "cancel_meal":
            await send_tg(cq_chat_id, "Cancelled.")
        
        async with httpx.AsyncClient() as client:
            await client.post(f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery",
                json={"callback_query_id": cq["id"]})
        return {"ok": True}
    
    # Regular text → AI coach
    if text and not text.startswith("/"):
        chat_data = ChatMessage(message=text)
        result = await coach_chat(chat_data)
        await send_tg(chat_id, result["reply"])
    
    return {"ok": True}

async def send_tg(chat_id, text, parse_mode=None, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup
    async with httpx.AsyncClient() as client:
        await client.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json=payload)

from fastapi.responses import HTMLResponse

@app.get("/")
async def serve_index():
    return HTMLResponse(content=HTML_CONTENT, headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0"
    })

HTML_CONTENT = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
<title>Squirrel 🐿️ v2</title>
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
<meta http-equiv="Pragma" content="no-cache">
<meta http-equiv="Expires" content="0">
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg: #0f0f0f; --card: #1a1a1a; --border: #2a2a2a;
    --accent: #6ee7b7; --accent2: #3b82f6; --text: #f0f0f0;
    --muted: #888; --danger: #ef4444; --success: #22c55e;
    --orange: #f97316;
  }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; min-height: 100vh; padding-bottom: 70px; }
  .nav { display: flex; background: var(--card); border-bottom: 1px solid var(--border); position: sticky; top: 0; z-index: 100; }
  .nav-btn { flex: 1; padding: 10px 4px; border: none; background: none; color: var(--muted); font-size: 10px; cursor: pointer; display: flex; flex-direction: column; align-items: center; gap: 2px; transition: color .2s; }
  .nav-btn.active { color: var(--accent); }
  .nav-btn span { font-size: 18px; }
  .screen { display: none; padding: 16px; }
  .screen.active { display: block; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px; margin-bottom: 12px; }
  .card-title { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .5px; margin-bottom: 10px; }
  h1 { font-size: 20px; font-weight: 700; margin-bottom: 4px; }
  h2 { font-size: 17px; font-weight: 600; margin-bottom: 12px; }
  h3 { font-size: 14px; font-weight: 600; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 20px; font-size: 11px; font-weight: 600; }
  .badge-green { background: rgba(110,231,183,.15); color: var(--accent); }
  .badge-blue { background: rgba(59,130,246,.15); color: var(--accent2); }
  .stat-row { display: flex; gap: 8px; margin-bottom: 12px; }
  .stat { flex: 1; background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 10px; text-align: center; }
  .stat-val { font-size: 20px; font-weight: 700; }
  .stat-lbl { font-size: 10px; color: var(--muted); margin-top: 2px; }
  .stat-trend { font-size: 11px; margin-top: 2px; }
  .up { color: var(--success); } .down { color: var(--danger); }
  .workout-day { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px; margin-bottom: 10px; cursor: pointer; display: flex; align-items: center; justify-content: space-between; }
  .workout-day:hover, .workout-day.today { border-color: var(--accent); }
  .exercise-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px; margin-bottom: 10px; }
  .ex-name { font-weight: 600; font-size: 14px; }
  .ex-target { font-size: 11px; color: var(--muted); }
  .last-week { background: rgba(59,130,246,.08); border: 1px solid rgba(59,130,246,.2); border-radius: 8px; padding: 8px 10px; margin: 8px 0; font-size: 12px; color: var(--muted); }
  .last-week strong { color: var(--accent2); }
  .set-inputs { display: flex; gap: 8px; }
  .set-input-group { flex: 1; }
  .set-input-group label { font-size: 10px; color: var(--muted); display: block; margin-bottom: 4px; }
  .set-input-group input { width: 100%; background: #111; border: 1px solid var(--border); border-radius: 8px; padding: 10px; color: var(--text); font-size: 16px; text-align: center; }
  .set-input-group input:focus { outline: none; border-color: var(--accent); }
  .notes-small { font-size: 11px; color: var(--muted); margin-top: 6px; font-style: italic; }
  .btn { width: 100%; padding: 13px; border: none; border-radius: 10px; font-size: 15px; font-weight: 600; cursor: pointer; margin-top: 8px; transition: opacity .2s; }
  .btn:active { opacity: .8; }
  .btn-primary { background: var(--accent); color: #000; }
  .btn-secondary { background: var(--card); border: 1px solid var(--border); color: var(--text); }
  .btn-sm { padding: 8px 14px; font-size: 13px; width: auto; }
  .form-group { margin-bottom: 12px; }
  .form-group label { font-size: 11px; color: var(--muted); display: block; margin-bottom: 5px; text-transform: uppercase; letter-spacing: .4px; }
  .form-group input, .form-group textarea { width: 100%; background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 11px; color: var(--text); font-size: 15px; }
  .form-group input:focus, .form-group textarea:focus { outline: none; border-color: var(--accent); }
  .form-group textarea { resize: none; height: 80px; font-family: inherit; }
  .progress-bar-wrap { background: var(--border); border-radius: 4px; height: 8px; margin-top: 6px; }
  .progress-bar { border-radius: 4px; height: 8px; transition: width .5s; }
  .empty-state { text-align: center; padding: 40px 20px; color: var(--muted); }
  .empty-state .icon { font-size: 40px; margin-bottom: 12px; }
  .toast { position: fixed; bottom: 80px; left: 50%; transform: translateX(-50%); background: var(--success); color: #000; padding: 10px 20px; border-radius: 20px; font-weight: 600; font-size: 13px; z-index: 999; opacity: 0; transition: opacity .3s; pointer-events: none; white-space: nowrap; }
  .toast.show { opacity: 1; }
  .back-btn { background: none; border: none; color: var(--accent); font-size: 14px; cursor: pointer; padding: 0 0 14px 0; display: flex; align-items: center; gap: 5px; }
  .metric-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
  .macro-bar { margin-bottom: 14px; }
  .macro-bar-header { display: flex; justify-content: space-between; font-size: 13px; margin-bottom: 4px; }
  .macro-bar-label { font-weight: 600; }
  .macro-bar-val { color: var(--muted); font-size: 12px; }
  .meal-item { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 12px; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: flex-start; }
  .meal-item-info { flex: 1; }
  .meal-item-name { font-weight: 600; font-size: 14px; }
  .meal-item-macros { font-size: 11px; color: var(--muted); margin-top: 3px; }
  .meal-item-del { background: none; border: none; color: var(--muted); font-size: 16px; cursor: pointer; padding: 0 0 0 8px; }
  .chat-wrap { display: flex; flex-direction: column; height: calc(100vh - 140px); }
  .chat-messages { flex: 1; overflow-y: auto; padding: 8px 0; }
  .chat-msg { margin-bottom: 12px; max-width: 85%; }
  .chat-msg.user { margin-left: auto; }
  .chat-msg-bubble { padding: 10px 14px; border-radius: 16px; font-size: 14px; line-height: 1.45; }
  .chat-msg.user .chat-msg-bubble { background: var(--accent2); color: #fff; border-bottom-right-radius: 4px; }
  .chat-msg.assistant .chat-msg-bubble { background: var(--card); border: 1px solid var(--border); border-bottom-left-radius: 4px; }
  .chat-input-row { display: flex; gap: 8px; padding-top: 10px; border-top: 1px solid var(--border); align-items: flex-end; }
  .chat-input { flex: 1; background: var(--card); border: 1px solid var(--border); border-radius: 20px; padding: 10px 16px; color: var(--text); font-size: 14px; font-family: inherit; resize: none; max-height: 100px; }
  .chat-input:focus { outline: none; border-color: var(--accent); }
  .chat-send-btn { background: var(--accent); border: none; border-radius: 50%; width: 40px; height: 40px; font-size: 18px; cursor: pointer; flex-shrink: 0; }
  .photo-btn { background: var(--card); border: 1px solid var(--border); border-radius: 50%; width: 40px; height: 40px; font-size: 18px; cursor: pointer; flex-shrink: 0; }
  .typing { display: inline-flex; gap: 4px; padding: 10px 14px; background: var(--card); border: 1px solid var(--border); border-radius: 16px; border-bottom-left-radius: 4px; }
  .typing span { width: 6px; height: 6px; border-radius: 50%; background: var(--muted); animation: bounce 1s infinite; }
  .typing span:nth-child(2) { animation-delay: .15s; }
  .typing span:nth-child(3) { animation-delay: .3s; }
  @keyframes bounce { 0%,60%,100%{transform:translateY(0)} 30%{transform:translateY(-6px)} }
  .modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,.8); z-index: 200; display: flex; align-items: flex-end; }
  .modal { background: var(--card); border-radius: 20px 20px 0 0; padding: 20px; width: 100%; max-height: 90vh; overflow-y: auto; }
  .modal-title { font-size: 16px; font-weight: 700; margin-bottom: 16px; }
  .modal-close { float: right; background: none; border: none; color: var(--muted); font-size: 20px; cursor: pointer; }
  .tabs { display: flex; border-bottom: 1px solid var(--border); margin-bottom: 16px; }
  .tab { flex: 1; padding: 10px; background: none; border: none; color: var(--muted); font-size: 13px; cursor: pointer; border-bottom: 2px solid transparent; }
  .tab.active { color: var(--accent); border-bottom-color: var(--accent); }
  .tab-content { display: none; }
  .tab-content.active { display: block; }
  .history-item { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 11px; margin-bottom: 8px; }
  .setup-card { background: linear-gradient(135deg, rgba(110,231,183,.1), rgba(59,130,246,.1)); border: 1px solid rgba(110,231,183,.3); border-radius: 14px; padding: 20px; text-align: center; margin-bottom: 16px; }
</style>
</head>
<body>

<div id="toast" class="toast">✅ Saved!</div>

<!-- HOME -->
<div id="screen-home" class="screen active">
  <div style="padding-top:8px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
      <div><h1>Squirrel 🐿️</h1><div style="font-size:12px;color:var(--muted)" id="home-date"></div></div>
    </div>
    <div class="stat-row" id="home-stats">
      <div class="stat"><div class="stat-val" id="stat-bf">—</div><div class="stat-lbl">Body Fat %</div><div class="stat-trend" id="stat-bf-trend"></div></div>
      <div class="stat"><div class="stat-val" id="stat-weight">—</div><div class="stat-lbl">Weight lbs</div></div>
      <div class="stat"><div class="stat-val" id="stat-protein">—</div><div class="stat-lbl">Protein Today</div></div>
    </div>
    <div class="card" id="today-nutrition-home"></div>
    <div class="card"><div class="card-title">This Week</div><div id="weekly-schedule"></div></div>
    <div id="today-cta"></div>
  </div>
</div>

<!-- WORKOUT -->
<div id="screen-workout" class="screen">
  <div id="workout-list-view">
    <h2>Workouts</h2>
    <div id="workout-days-list"></div>
  </div>
  <div id="workout-session-view" style="display:none">
    <button class="back-btn" onclick="showWorkoutList()">← Back</button>
    <div id="session-content"></div>
  </div>
</div>

<!-- NUTRITION -->
<div id="screen-nutrition" class="screen">
  <div id="nutrition-main-view">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
      <h2 style="margin:0">Nutrition</h2>
      <button class="btn btn-primary btn-sm" onclick="showLogMeal()">+ Log Meal</button>
    </div>
    <div id="nutrition-setup-prompt"></div>
    <div id="daily-macros-card"></div>
    <div class="tabs">
      <button class="tab active" onclick="switchNutriTab('today')">Today</button>
      <button class="tab" onclick="switchNutriTab('history')">History</button>
    </div>
    <div id="nutri-tab-today" class="tab-content active">
      <div id="meals-today-list"></div>
    </div>
    <div id="nutri-tab-history" class="tab-content">
      <div id="nutrition-history-list"></div>
    </div>
  </div>
  <div id="log-meal-view" style="display:none">
    <button class="back-btn" onclick="showNutritionMain()">← Back</button>
    <h2>Log Meal</h2>
    <div class="card">
      <div class="form-group">
        <label>Describe your meal</label>
        <textarea id="meal-desc" placeholder="e.g. grilled salmon fillet, 1 cup brown rice, steamed broccoli with olive oil..."></textarea>
      </div>
      <div style="text-align:center;color:var(--muted);font-size:12px;margin-bottom:12px">or upload a photo</div>
      <input type="file" id="meal-photo" accept="image/*" capture="environment" style="display:none" onchange="handleMealPhoto(event)">
      <button class="btn btn-secondary" onclick="document.getElementById('meal-photo').click()">📷 Take / Upload Photo</button>
      <div id="meal-photo-preview" style="margin-top:10px"></div>
      <button class="btn btn-primary" onclick="estimateMeal()" id="estimate-btn">Estimate Macros →</button>
    </div>
    <div id="meal-estimate-result" style="display:none">
      <div class="card">
        <div class="card-title">AI Estimate — Review & Adjust</div>
        <div class="form-group"><label>Meal Name</label><input type="text" id="est-name"></div>
        <div class="metric-grid">
          <div class="form-group"><label>Protein (g)</label><input type="number" id="est-protein" step="1"></div>
          <div class="form-group"><label>Carbs (g)</label><input type="number" id="est-carbs" step="1"></div>
          <div class="form-group"><label>Fat (g)</label><input type="number" id="est-fat" step="1"></div>
          <div class="form-group"><label>Calories</label><input type="number" id="est-cal" step="1"></div>
        </div>
        <div id="est-notes" style="font-size:12px;color:var(--muted);margin-bottom:10px;font-style:italic"></div>
        <button class="btn btn-primary" onclick="confirmLogMeal()">✅ Log This Meal</button>
      </div>
    </div>
  </div>
  <div id="setup-targets-view" style="display:none">
    <button class="back-btn" onclick="showNutritionMain()">← Back</button>
    <h2>Set Your Targets</h2>
    <div class="card">
      <p style="font-size:13px;color:var(--muted);margin-bottom:16px">Set your daily targets. Your AI coach can help you figure out the right numbers — just ask in the Coach tab.</p>
      <div class="metric-grid">
        <div class="form-group"><label>Calories</label><input type="number" id="tgt-cal" placeholder="e.g. 1900"></div>
        <div class="form-group"><label>Protein (g)</label><input type="number" id="tgt-protein" placeholder="e.g. 160"></div>
        <div class="form-group"><label>Carbs (g)</label><input type="number" id="tgt-carbs" placeholder="e.g. 180"></div>
        <div class="form-group"><label>Fat (g)</label><input type="number" id="tgt-fat" placeholder="e.g. 65"></div>
      </div>
      <button class="btn btn-primary" onclick="saveTargets()">Save Targets</button>
    </div>
  </div>
</div>

<!-- COACH -->
<div id="screen-coach" class="screen" style="padding:0">
  <div class="chat-wrap" style="padding:16px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
      <h2 style="margin:0">Coach 🏆</h2>
      <button onclick="clearChat()" style="background:none;border:none;color:var(--muted);font-size:12px;cursor:pointer">Clear</button>
    </div>
    <div class="chat-messages" id="chat-messages">
      <div class="chat-msg assistant">
        <div class="chat-msg-bubble">Hey — I'm your coach. I can see your workouts, nutrition, and body metrics. Ask me anything or send a photo of your meal to log it.</div>
      </div>
    </div>
    <div class="chat-input-row">
      <button class="photo-btn" onclick="triggerChatPhoto()" title="Send meal photo">📷</button>
      <input type="file" id="chat-photo-input" accept="image/*" capture="environment" style="display:none" onchange="handleChatPhoto(event)">
      <textarea class="chat-input" id="chat-input" placeholder="Ask your coach..." rows="1" onkeydown="chatKeyDown(event)" oninput="autoResize(this)"></textarea>
      <button class="chat-send-btn" onclick="sendChat()">↑</button>
    </div>
  </div>
</div>

<!-- METRICS -->
<div id="screen-metrics" class="screen">
  <h2>Body Metrics</h2>
  <div class="card">
    <div class="card-title">Log Today</div>
    <div class="metric-grid">
      <div class="form-group"><label>Weight (lbs)</label><input type="number" id="m-weight" placeholder="186" step="0.1"></div>
      <div class="form-group"><label>Body Fat %</label><input type="number" id="m-bf" placeholder="15.0" step="0.1"></div>
      <div class="form-group"><label>Muscle Mass %</label><input type="number" id="m-muscle" placeholder="42" step="0.1"></div>
      <div class="form-group"><label>Waist (cm)</label><input type="number" id="m-waist" placeholder="85" step="0.5"></div>
      <div class="form-group"><label>Chest (cm)</label><input type="number" id="m-chest" placeholder="100" step="0.5"></div>
      <div class="form-group"><label>Arm (cm)</label><input type="number" id="m-arm" placeholder="35" step="0.5"></div>
    </div>
    <button class="btn btn-primary" onclick="logMetrics()">Save Metrics</button>
  </div>
  <div id="metrics-history"></div>
</div>

<!-- NAV -->
<nav class="nav" style="position:fixed;bottom:0;left:0;right:0;border-top:1px solid var(--border);border-bottom:none">
  <button class="nav-btn active" onclick="showScreen('home')" id="nav-home"><span>🏠</span>Home</button>
  <button class="nav-btn" onclick="showScreen('workout')" id="nav-workout"><span>🏋️</span>Gym</button>
  <button class="nav-btn" onclick="showScreen('nutrition')" id="nav-nutrition"><span>🥗</span>Nutrition</button>
  <button class="nav-btn" onclick="showScreen('coach')" id="nav-coach"><span>🏆</span>Coach</button>
  <button class="nav-btn" onclick="showScreen('metrics')" id="nav-metrics"><span>📊</span>Metrics</button>
</nav>

<script>
const API = 'https://squirrel-gym-production.up.railway.app';
const USER_ID = 7955194359;

// Catch all errors and show them
window.onerror = function(msg, src, line, col, err) {
  const el = document.getElementById('screen-home');
  if (el) el.innerHTML = '<div style="padding:20px;color:red;font-size:12px;word-break:break-all">JS Error: '+msg+' ('+src+':'+line+')</div>';
};
window.addEventListener('unhandledrejection', function(e) {
  const el = document.getElementById('screen-home');
  if (el) el.innerHTML = '<div style="padding:20px;color:red;font-size:12px;word-break:break-all">Promise Error: '+e.reason+'</div>';
});
const DAY_LABELS = {0:'A', 2:'B', 4:'C'};
let program = null;
let currentDayLabel = null;
let sessionData = {};
let pendingMealPhoto = null;
let chatPhotoData = null;

async function api(path, method='GET', body=null) {
  const opts = { method, headers: {'Content-Type':'application/json'} };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(API + path, opts);
  return r.json();
}

function showToast(msg='✅ Saved!') {
  const t = document.getElementById('toast');
  t.textContent = msg; t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2500);
}

function showScreen(name) {
  document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('screen-'+name).classList.add('active');
  document.getElementById('nav-'+name).classList.add('active');
  if (name === 'metrics') loadMetricsHistory();
  if (name === 'nutrition') loadNutrition();
}

// ── HOME ──────────────────────────────────────────────────────────────────────
async function loadHome() {
  const today = new Date();
  document.getElementById('home-date').textContent = today.toLocaleDateString('en-US', {weekday:'long', month:'long', day:'numeric'});
  
  const [todayData, sessions, metrics, nutrition] = await Promise.all([
    api('/api/today?user_id='+USER_ID),
    api('/api/sessions?user_id='+USER_ID+'&limit=50'),
    api('/api/metrics?user_id='+USER_ID+'&limit=2'),
    api('/api/nutrition/today?user_id='+USER_ID)
  ]);

  if (metrics.length > 0) {
    const m = metrics[0];
    document.getElementById('stat-bf').textContent = m.body_fat_pct ? m.body_fat_pct.toFixed(1)+'%' : '—';
    document.getElementById('stat-weight').textContent = m.weight_lbs ? m.weight_lbs.toFixed(1) : '—';
    if (metrics.length > 1 && m.body_fat_pct && metrics[1].body_fat_pct) {
      const diff = (m.body_fat_pct - metrics[1].body_fat_pct).toFixed(1);
      const el = document.getElementById('stat-bf-trend');
      el.textContent = (diff > 0 ? '▲' : '▼') + Math.abs(diff); el.className = 'stat-trend '+(diff > 0 ? 'up':'down');
    }
  }
  
  const p = nutrition.totals?.protein_g || 0;
  const tgt = nutrition.targets?.protein_g;
  document.getElementById('stat-protein').textContent = Math.round(p)+'g'+(tgt?'/'+tgt:'');

  // Today's nutrition summary
  const nc = document.getElementById('today-nutrition-home');
  if (nutrition.totals) {
    const t = nutrition.totals, tg = nutrition.targets;
    const pPct = tg ? Math.min(100, t.protein_g/tg.protein_g*100) : 0;
    const cPct = tg ? Math.min(100, t.calories/tg.calories*100) : 0;
    nc.innerHTML = `
      <div class="card-title">Today's Nutrition</div>
      <div class="macro-bar">
        <div class="macro-bar-header"><span class="macro-bar-label">Protein</span><span class="macro-bar-val">${Math.round(t.protein_g)}g${tg?' / '+tg.protein_g+'g':''}</span></div>
        <div class="progress-bar-wrap"><div class="progress-bar" style="width:${pPct}%;background:var(--accent)"></div></div>
      </div>
      <div class="macro-bar">
        <div class="macro-bar-header"><span class="macro-bar-label">Calories</span><span class="macro-bar-val">${Math.round(t.calories)}${tg?' / '+tg.calories:''}</span></div>
        <div class="progress-bar-wrap"><div class="progress-bar" style="width:${cPct}%;background:var(--accent2)"></div></div>
      </div>`;
  }

  // Schedule
  const todayDow = today.getDay() === 0 ? 6 : today.getDay() - 1;
  const weekDays = [{dow:0,label:'A',name:'Chest + Tris',day:'Mon'},{dow:1,label:null,name:'Lagree 🧘',day:'Tue'},{dow:2,label:'B',name:'Back + Bis',day:'Wed'},{dow:3,label:null,name:'Lagree 🧘',day:'Thu'},{dow:4,label:'C',name:'Shoulders + Arms',day:'Fri'},{dow:5,label:null,name:'Lagree / Rest',day:'Sat'},{dow:6,label:null,name:'Rest',day:'Sun'}];
  document.getElementById('weekly-schedule').innerHTML = weekDays.map(d => `
    <div style="display:flex;align-items:center;padding:7px 0;${d.dow<6?'border-bottom:1px solid var(--border)':''};opacity:${d.dow<todayDow?.6:1}">
      <div style="width:32px;font-size:11px;color:var(--muted)">${d.day}</div>
      <div style="flex:1;font-size:12px${d.dow===todayDow?';font-weight:700;color:var(--accent)':''}">${d.name}</div>
      ${d.dow===todayDow?'<div style="font-size:10px;color:var(--accent)">TODAY</div>':''}
    </div>`).join('');

  const ctaEl = document.getElementById('today-cta');
  if (todayData.rest_day) {
    ctaEl.innerHTML = `<div style="text-align:center;color:var(--muted);padding:16px;font-size:13px">🧘 ${todayData.message}</div>`;
  } else {
    ctaEl.innerHTML = `<button class="btn btn-primary" style="margin-top:4px" onclick="openWorkout('${todayData.day_label}')">🏋️ Start Today — Day ${todayData.day_label}</button>`;
  }
}

// ── WORKOUT ───────────────────────────────────────────────────────────────────
function showWorkoutList() {
  document.getElementById('workout-list-view').style.display = 'block';
  document.getElementById('workout-session-view').style.display = 'none';
}

async function loadWorkoutList() {
  if (!program) program = await api('/api/program');
  const todayDow = new Date().getDay() === 0 ? 6 : new Date().getDay() - 1;
  const todayLabel = DAY_LABELS[todayDow];
  document.getElementById('workout-days-list').innerHTML = Object.entries(program).map(([label, w]) => `
    <div class="workout-day${label===todayLabel?' today':''}" onclick="openWorkout('${label}')">
      <div style="flex:1">
        <div style="font-weight:600;font-size:14px">Day ${label} — ${w.day}</div>
        <div style="font-size:12px;color:var(--muted)">${w.name}</div>
      </div>
      <div style="color:var(--muted);font-size:18px">›</div>
    </div>`).join('');
}

async function openWorkout(dayLabel) {
  showScreen('workout');
  currentDayLabel = dayLabel; sessionData = {};
  document.getElementById('workout-list-view').style.display = 'none';
  document.getElementById('workout-session-view').style.display = 'block';
  const data = await api(`/api/workout/${dayLabel}?user_id=${USER_ID}`);
  const w = data.workout;
  document.getElementById('session-content').innerHTML = `
    <h2>Day ${dayLabel} — ${w.name}</h2>
    ${w.exercises.map((ex, i) => `
      <div class="exercise-card">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:6px">
          <div><div class="ex-name">${ex.name}</div><div class="ex-target">${ex.sets} sets × ${ex.reps} reps</div></div>
        </div>
        ${ex.last_weight ? `<div class="last-week">Last: <strong>${ex.last_weight}lbs × ${ex.last_reps}r × ${ex.last_sets}s</strong></div>` : '<div class="last-week" style="color:var(--muted)">First session — set your baseline</div>'}
        <div class="set-inputs">
          <div class="set-input-group"><label>Weight</label><input type="number" id="weight-${i}" placeholder="${ex.last_weight||0}" step="2.5" oninput="updateSession(${i},'${ex.id}','${ex.name}')" value="${ex.last_weight||''}"></div>
          <div class="set-input-group"><label>Reps</label><input type="number" id="reps-${i}" placeholder="${ex.last_reps||8}" oninput="updateSession(${i},'${ex.id}','${ex.name}')" value="${ex.last_reps||''}"></div>
          <div class="set-input-group"><label>Sets</label><input type="number" id="sets-${i}" placeholder="${ex.sets}" oninput="updateSession(${i},'${ex.id}','${ex.name}')" value="${ex.sets}"></div>
        </div>
        ${ex.notes?`<div class="notes-small">💡 ${ex.notes}</div>`:''}
      </div>`).join('')}
    <button class="btn btn-primary" onclick="saveSession()">✅ Complete Workout</button>
    <div style="height:20px"></div>`;
}

function updateSession(idx, exId, exName) {
  const w = parseFloat(document.getElementById('weight-'+idx)?.value)||0;
  const r = parseFloat(document.getElementById('reps-'+idx)?.value)||0;
  const s = parseFloat(document.getElementById('sets-'+idx)?.value)||0;
  sessionData[idx] = {exercise_id:exId, name:exName, weight:w, reps:r, sets:s};
}

async function saveSession() {
  const exercises = Object.values(sessionData).filter(e => e.weight > 0);
  if (!exercises.length) { showToast('⚠️ Log at least one exercise'); return; }
  await api('/api/workout/log', 'POST', {user_id:USER_ID, day_label:currentDayLabel, exercises});
  showToast('🏋️ Workout saved!');
  setTimeout(() => { showWorkoutList(); showScreen('home'); loadHome(); }, 1200);
}

// ── NUTRITION ─────────────────────────────────────────────────────────────────
let nutritionTargets = null;

async function loadNutrition() {
  const [nutrition, targets] = await Promise.all([
    api('/api/nutrition/today?user_id='+USER_ID),
    api('/api/nutrition/targets?user_id='+USER_ID)
  ]);
  nutritionTargets = targets;

  // Setup prompt if no targets
  const prompt = document.getElementById('nutrition-setup-prompt');
  if (!targets.calories) {
    prompt.innerHTML = `<div class="setup-card">
      <div style="font-size:28px;margin-bottom:8px">🎯</div>
      <div style="font-weight:600;margin-bottom:6px">Set Your Targets</div>
      <div style="font-size:13px;color:var(--muted);margin-bottom:12px">Set daily macro targets to track your progress</div>
      <button class="btn btn-primary btn-sm" onclick="showSetupTargets()">Set Targets</button>
    </div>`;
  } else {
    prompt.innerHTML = `<div style="text-align:right;margin-bottom:8px"><button onclick="showSetupTargets()" style="background:none;border:none;color:var(--muted);font-size:12px;cursor:pointer">Edit targets ⚙️</button></div>`;
  }

  // Daily macros card
  const t = nutrition.totals, tg = targets;
  const card = document.getElementById('daily-macros-card');
  if (tg.calories) {
    const macros = [
      {label:'Protein', val:Math.round(t.protein_g), tgt:tg.protein_g, unit:'g', color:'var(--accent)'},
      {label:'Carbs', val:Math.round(t.carbs_g), tgt:tg.carbs_g, unit:'g', color:'var(--accent2)'},
      {label:'Fat', val:Math.round(t.fat_g), tgt:tg.fat_g, unit:'g', color:'var(--orange)'},
      {label:'Calories', val:Math.round(t.calories), tgt:tg.calories, unit:'', color:'#a78bfa'},
    ];
    card.innerHTML = `<div class="card-title">Today's Macros</div>` + macros.map(m => `
      <div class="macro-bar">
        <div class="macro-bar-header">
          <span class="macro-bar-label">${m.label}</span>
          <span class="macro-bar-val">${m.val}${m.unit} / ${m.tgt}${m.unit}</span>
        </div>
        <div class="progress-bar-wrap"><div class="progress-bar" style="width:${Math.min(100,m.val/m.tgt*100)}%;background:${m.color}"></div></div>
      </div>`).join('');
  } else {
    card.innerHTML = `<div class="card-title">Today's Macros</div>
      <div style="display:flex;gap:12px;flex-wrap:wrap">
        <span style="font-size:13px">🥩 ${Math.round(t.protein_g)}g protein</span>
        <span style="font-size:13px">🍞 ${Math.round(t.carbs_g)}g carbs</span>
        <span style="font-size:13px">🥑 ${Math.round(t.fat_g)}g fat</span>
        <span style="font-size:13px">🔥 ${Math.round(t.calories)} cal</span>
      </div>`;
  }

  // Meals list
  const ml = document.getElementById('meals-today-list');
  if (!nutrition.meals.length) {
    ml.innerHTML = `<div class="empty-state"><div class="icon">🍽️</div><p>No meals logged yet today</p></div>`;
  } else {
    ml.innerHTML = nutrition.meals.map(m => `
      <div class="meal-item">
        <div class="meal-item-info">
          <div class="meal-item-name">${m.meal_name}</div>
          <div class="meal-item-macros">${Math.round(m.protein_g)}g P · ${Math.round(m.carbs_g)}g C · ${Math.round(m.fat_g)}g F · ${m.calories} cal</div>
        </div>
        <button class="meal-item-del" onclick="deleteMeal(${m.id})">×</button>
      </div>`).join('');
  }
}

async function deleteMeal(id) {
  await api(`/api/nutrition/meal/${id}?user_id=${USER_ID}`, 'DELETE');
  showToast('Deleted'); loadNutrition();
}

function showLogMeal() {
  document.getElementById('nutrition-main-view').style.display = 'none';
  document.getElementById('log-meal-view').style.display = 'block';
  document.getElementById('setup-targets-view').style.display = 'none';
  document.getElementById('meal-estimate-result').style.display = 'none';
  document.getElementById('meal-desc').value = '';
  document.getElementById('meal-photo-preview').innerHTML = '';
  pendingMealPhoto = null;
}

function showNutritionMain() {
  document.getElementById('nutrition-main-view').style.display = 'block';
  document.getElementById('log-meal-view').style.display = 'none';
  document.getElementById('setup-targets-view').style.display = 'none';
}

function showSetupTargets() {
  document.getElementById('nutrition-main-view').style.display = 'none';
  document.getElementById('log-meal-view').style.display = 'none';
  document.getElementById('setup-targets-view').style.display = 'block';
  if (nutritionTargets) {
    document.getElementById('tgt-cal').value = nutritionTargets.calories || '';
    document.getElementById('tgt-protein').value = nutritionTargets.protein_g || '';
    document.getElementById('tgt-carbs').value = nutritionTargets.carbs_g || '';
    document.getElementById('tgt-fat').value = nutritionTargets.fat_g || '';
  }
}

function switchNutriTab(tab) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  event.target.classList.add('active');
  document.getElementById('nutri-tab-'+tab).classList.add('active');
  if (tab === 'history') loadNutritionHistory();
}

async function loadNutritionHistory() {
  const data = await api('/api/nutrition/history?user_id='+USER_ID+'&days=30');
  const el = document.getElementById('nutrition-history-list');
  if (!data.length) { el.innerHTML = '<div class="empty-state"><div class="icon">📈</div><p>No history yet</p></div>'; return; }
  const tgt = nutritionTargets;
  el.innerHTML = data.map(d => {
    const pPct = tgt?.protein_g ? Math.round(d.protein_g/tgt.protein_g*100) : null;
    return `<div class="history-item">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <div>
          <div style="font-weight:600;font-size:13px">${new Date(d.date+'T12:00:00').toLocaleDateString('en-US',{weekday:'short',month:'short',day:'numeric'})}</div>
          <div style="font-size:11px;color:var(--muted);margin-top:2px">${Math.round(d.protein_g)}g protein · ${Math.round(d.calories)} cal · ${d.meal_count} meals</div>
        </div>
        ${pPct !== null ? `<span class="badge ${pPct>=90?'badge-green':'badge-blue'}">${pPct}% protein</span>` : ''}
      </div>
    </div>`;
  }).join('');
}

async function handleMealPhoto(event) {
  const file = event.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = e => {
    pendingMealPhoto = { base64: e.target.result.split(',')[1], type: file.type };
    document.getElementById('meal-photo-preview').innerHTML = `<img src="${e.target.result}" style="width:100%;border-radius:8px;max-height:200px;object-fit:cover">`;
  };
  reader.readAsDataURL(file);
}

async function estimateMeal() {
  const desc = document.getElementById('meal-desc').value.trim();
  if (!desc && !pendingMealPhoto) { showToast('⚠️ Describe the meal or upload a photo'); return; }
  
  const btn = document.getElementById('estimate-btn');
  btn.textContent = 'Estimating...'; btn.disabled = true;
  
  try {
    const payload = { description: desc || null };
    if (pendingMealPhoto) { payload.image_base64 = pendingMealPhoto.base64; payload.image_media_type = pendingMealPhoto.type; }
    
    const est = await api('/api/nutrition/estimate', 'POST', payload);
    document.getElementById('est-name').value = est.meal_name || '';
    document.getElementById('est-protein').value = est.protein_g || 0;
    document.getElementById('est-carbs').value = est.carbs_g || 0;
    document.getElementById('est-fat').value = est.fat_g || 0;
    document.getElementById('est-cal').value = est.calories || 0;
    document.getElementById('est-notes').textContent = est.notes || '';
    document.getElementById('meal-estimate-result').style.display = 'block';
  } catch(e) {
    showToast('⚠️ Estimation failed — try again');
  }
  btn.textContent = 'Estimate Macros →'; btn.disabled = false;
}

async function confirmLogMeal() {
  const data = {
    user_id: USER_ID,
    meal_name: document.getElementById('est-name').value,
    protein_g: parseFloat(document.getElementById('est-protein').value)||0,
    carbs_g: parseFloat(document.getElementById('est-carbs').value)||0,
    fat_g: parseFloat(document.getElementById('est-fat').value)||0,
    calories: parseInt(document.getElementById('est-cal').value)||0,
    description: document.getElementById('meal-desc').value || null
  };
  await api('/api/nutrition/log', 'POST', data);
  showToast('🥗 Meal logged!');
  showNutritionMain();
  loadNutrition();
  loadHome();
}

async function saveTargets() {
  const data = {
    user_id: USER_ID,
    calories: parseInt(document.getElementById('tgt-cal').value)||0,
    protein_g: parseInt(document.getElementById('tgt-protein').value)||0,
    carbs_g: parseInt(document.getElementById('tgt-carbs').value)||0,
    fat_g: parseInt(document.getElementById('tgt-fat').value)||0
  };
  if (!data.calories || !data.protein_g) { showToast('⚠️ Fill in calories and protein at minimum'); return; }
  await api('/api/nutrition/targets', 'POST', data);
  showToast('🎯 Targets saved!');
  showNutritionMain();
  loadNutrition();
}

// ── COACH ─────────────────────────────────────────────────────────────────────
function autoResize(el) {
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 100) + 'px';
}

function chatKeyDown(e) {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendChat(); }
}

function triggerChatPhoto() { document.getElementById('chat-photo-input').click(); }

async function handleChatPhoto(event) {
  const file = event.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = async e => {
    chatPhotoData = { base64: e.target.result.split(',')[1], type: file.type };
    addChatMsg('user', '📷 [Photo sent]');
    await sendChatRequest('What are the macros in this meal? If I should log it, tell me the macros.', chatPhotoData);
    chatPhotoData = null;
    event.target.value = '';
  };
  reader.readAsDataURL(file);
}

async function sendChat() {
  const input = document.getElementById('chat-input');
  const msg = input.value.trim();
  if (!msg && !chatPhotoData) return;
  input.value = ''; input.style.height = 'auto';
  if (msg) addChatMsg('user', msg);
  await sendChatRequest(msg, chatPhotoData);
}

async function sendChatRequest(message, photoData=null) {
  const typingEl = addTyping();
  try {
    const payload = { user_id: USER_ID, message: message || '' };
    if (photoData) { payload.image_base64 = photoData.base64; payload.image_media_type = photoData.type; }
    const r = await api('/api/coach/chat', 'POST', payload);
    typingEl.remove();
    addChatMsg('assistant', r.reply);
  } catch(e) {
    typingEl.remove();
    addChatMsg('assistant', 'Sorry, something went wrong. Try again.');
  }
}

function addChatMsg(role, text) {
  const el = document.createElement('div');
  el.className = 'chat-msg ' + role;
  el.innerHTML = `<div class="chat-msg-bubble">${text.replace(/\\n/g,'<br>')}</div>`;
  const msgs = document.getElementById('chat-messages');
  msgs.appendChild(el);
  msgs.scrollTop = msgs.scrollHeight;
  return el;
}

function addTyping() {
  const el = document.createElement('div');
  el.className = 'chat-msg assistant';
  el.innerHTML = '<div class="typing"><span></span><span></span><span></span></div>';
  const msgs = document.getElementById('chat-messages');
  msgs.appendChild(el);
  msgs.scrollTop = msgs.scrollHeight;
  return el;
}

function clearChat() {
  const msgs = document.getElementById('chat-messages');
  msgs.innerHTML = '<div class="chat-msg assistant"><div class="chat-msg-bubble">Hey — I\\'m your coach. I can see your workouts, nutrition, and body metrics. Ask me anything or send a photo of your meal to log it.</div></div>';
}

// ── METRICS ───────────────────────────────────────────────────────────────────
async function logMetrics() {
  const data = {
    user_id: USER_ID,
    weight_lbs: parseFloat(document.getElementById('m-weight').value)||null,
    body_fat_pct: parseFloat(document.getElementById('m-bf').value)||null,
    muscle_mass_pct: parseFloat(document.getElementById('m-muscle').value)||null,
    waist_cm: parseFloat(document.getElementById('m-waist').value)||null,
    chest_cm: parseFloat(document.getElementById('m-chest').value)||null,
    arm_cm: parseFloat(document.getElementById('m-arm').value)||null
  };
  await api('/api/metrics', 'POST', data);
  showToast('📊 Metrics saved!'); loadMetricsHistory(); loadHome();
}

async function loadMetricsHistory() {
  const metrics = await api('/api/metrics?user_id='+USER_ID+'&limit=14');
  const el = document.getElementById('metrics-history');
  if (!metrics.length) { el.innerHTML = '<div class="empty-state"><div class="icon">📊</div><p>No metrics yet</p></div>'; return; }
  el.innerHTML = '<h3 style="margin-bottom:10px">History</h3>' + metrics.map(m => `
    <div class="history-item">
      <div style="display:flex;justify-content:space-between">
        <div>
          <div style="font-weight:600;font-size:13px">${new Date(m.metric_date+'T12:00:00').toLocaleDateString('en-US',{month:'short',day:'numeric'})}</div>
          <div style="font-size:11px;color:var(--muted);margin-top:2px">${m.weight_lbs?m.weight_lbs+'lbs':''} ${m.body_fat_pct?'· '+m.body_fat_pct+'% BF':''} ${m.muscle_mass_pct?'· '+m.muscle_mass_pct+'% muscle':''}</div>
        </div>
        ${m.body_fat_pct?`<div style="font-size:20px;font-weight:700;color:var(--accent)">${m.body_fat_pct}%</div>`:''}
      </div>
    </div>`).join('');
}

async function init() {
  if (window.Telegram?.WebApp) { Telegram.WebApp.ready(); Telegram.WebApp.expand(); }
  await loadHome();
  await loadWorkoutList();
}

init();
</script>
</body>
</html>
"""
