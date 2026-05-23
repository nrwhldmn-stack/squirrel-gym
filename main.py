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

app.mount("/", StaticFiles(directory="static", html=True), name="static")
