from flask import Flask, request, jsonify, render_template, session, has_request_context
import requests
import base64
import json
import os
import threading
import time
import uuid
from datetime import datetime, timedelta
from collections import deque
import schedule
from sqlalchemy import create_engine, text

def env_flag(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "anaplan-pipeline-secret-2024")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = env_flag("SESSION_COOKIE_SECURE", False)

ANAPLAN_BASE = "https://api.anaplan.com/2/0"
AUTH_URL     = "https://auth.anaplan.com/token/authenticate"
DB_PATH      = os.getenv("DB_PATH", os.path.join(app.root_path, "dashboard.db"))
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
SESSION_TIMEOUT_SECONDS = 30 * 60
SESSION_WARNING_SECONDS = 5 * 60
ENABLE_SCHEDULER = env_flag("ENABLE_SCHEDULER", True)

# Ignore machine-level proxy env vars for direct Anaplan API access.
http = requests.Session()
http.trust_env = False
db_lock = threading.Lock()
effective_database_url = DATABASE_URL or f"sqlite:///{DB_PATH}"
if effective_database_url.startswith("postgres://"):
    effective_database_url = effective_database_url.replace("postgres://", "postgresql+psycopg2://", 1)
elif effective_database_url.startswith("postgresql://") and "+psycopg2" not in effective_database_url:
    effective_database_url = effective_database_url.replace("postgresql://", "postgresql+psycopg2://", 1)
engine_kwargs = {"future": True, "pool_pre_ping": True}
if effective_database_url.startswith("sqlite:///"):
    engine_kwargs["connect_args"] = {"check_same_thread": False}
db_engine = create_engine(effective_database_url, **engine_kwargs)

# ─────────────────────────────────────────
# IN-MEMORY STORE
# ─────────────────────────────────────────
pipelines   = {}          # id -> pipeline config
job_history = deque(maxlen=500)   # last 500 runs across all pipelines
alerts      = deque(maxlen=100)   # last 100 alerts
scheduler_running = False
scheduler_thread  = None

def get_db_connection():
    return db_engine.begin()

def fetch_all(query, params=None):
    with db_engine.connect() as conn:
        return [dict(row) for row in conn.execute(text(query), params or {}).mappings().all()]

def fetch_one(query, params=None):
    with db_engine.connect() as conn:
        row = conn.execute(text(query), params or {}).mappings().first()
        return dict(row) if row else None

def fetch_scalar(query, params=None, default=None):
    with db_engine.connect() as conn:
        value = conn.execute(text(query), params or {}).scalar()
        return default if value is None else value

def execute_sql(query, params=None):
    with db_engine.begin() as conn:
        conn.execute(text(query), params or {})

def get_current_user():
    if not has_request_context():
        return ""
    return (session.get("user") or "").strip()

def require_user():
    enforce_session_expiry()
    user = get_current_user()
    if not user:
        return None, (jsonify({"error": "Not authenticated"}), 401)
    return user, None

def mark_auth_session(user, auth_header):
    issued_at = int(time.time())
    expires_at = issued_at + SESSION_TIMEOUT_SECONDS
    session["auth_header"] = auth_header
    session["user"] = user
    session["auth_issued_at"] = issued_at
    session["auth_expires_at"] = expires_at
    return expires_at

def get_session_status():
    if not has_request_context():
        return {
            "authenticated": False,
            "expires_in_seconds": 0,
            "warning_threshold_seconds": SESSION_WARNING_SECONDS,
            "expired": True,
        }

    auth_header = session.get("auth_header", "")
    user = get_current_user()
    expires_at = int(session.get("auth_expires_at") or 0)
    now_ts = int(time.time())
    authenticated = bool(auth_header and user and expires_at)
    expires_in = max(0, expires_at - now_ts) if authenticated else 0
    expired = authenticated and expires_in <= 0
    warning = authenticated and 0 < expires_in <= SESSION_WARNING_SECONDS
    return {
        "authenticated": authenticated and not expired,
        "expires_in_seconds": expires_in,
        "warning_threshold_seconds": SESSION_WARNING_SECONDS,
        "expired": expired,
        "warning": warning,
    }

def enforce_session_expiry():
    status = get_session_status()
    if status["expired"]:
        session.pop("auth_header", None)
        session.pop("auth_issued_at", None)
        session.pop("auth_expires_at", None)
        return True
    return False

def init_db():
    with db_lock:
        with db_engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS pipelines (
                    id TEXT PRIMARY KEY,
                    owner_user TEXT NOT NULL DEFAULT '',
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    workspace_id TEXT NOT NULL DEFAULT '',
                    workspace_name TEXT NOT NULL DEFAULT '',
                    model_id TEXT NOT NULL DEFAULT '',
                    model_name TEXT NOT NULL DEFAULT '',
                    steps_json TEXT NOT NULL DEFAULT '[]',
                    schedule_type TEXT NOT NULL DEFAULT 'manual',
                    schedule_value TEXT NOT NULL DEFAULT '',
                    stop_on_failure INTEGER NOT NULL DEFAULT 1,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL DEFAULT 'IDLE',
                    last_status TEXT NOT NULL DEFAULT 'NEVER RUN',
                    last_run TEXT,
                    last_duration REAL,
                    current_step INTEGER NOT NULL DEFAULT -1,
                    run_count INTEGER NOT NULL DEFAULT 0,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    fail_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS job_history (
                    id TEXT PRIMARY KEY,
                    owner_user TEXT NOT NULL DEFAULT '',
                    pipeline_id TEXT NOT NULL,
                    pipeline_name TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    action_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    duration_s REAL NOT NULL,
                    details TEXT NOT NULL DEFAULT '',
                    time TEXT NOT NULL
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id TEXT PRIMARY KEY,
                    owner_user TEXT NOT NULL DEFAULT '',
                    pipeline_id TEXT NOT NULL,
                    pipeline_name TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    time TEXT NOT NULL
                )
            """))
            for table_name in ("pipelines", "job_history", "alerts"):
                if db_engine.dialect.name == "postgresql":
                    columns = [row["column_name"] for row in conn.execute(
                        text("""
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_name = :table_name
                        """),
                        {"table_name": table_name},
                    ).mappings().all()]
                else:
                    columns = [row["name"] for row in conn.execute(text(f"PRAGMA table_info({table_name})")).mappings().all()]
                if "owner_user" not in columns:
                    conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN owner_user TEXT NOT NULL DEFAULT ''"))

def pipeline_to_record(pipeline):
    return (
        pipeline["id"],
        pipeline.get("owner_user", ""),
        pipeline.get("name", "Unnamed Pipeline"),
        pipeline.get("description", ""),
        pipeline.get("workspace_id", ""),
        pipeline.get("workspace_name", ""),
        pipeline.get("model_id", ""),
        pipeline.get("model_name", ""),
        json.dumps(pipeline.get("steps", [])),
        pipeline.get("schedule_type", "manual"),
        pipeline.get("schedule_value", ""),
        1 if pipeline.get("stop_on_failure", True) else 0,
        1 if pipeline.get("enabled", True) else 0,
        pipeline.get("status", "IDLE"),
        pipeline.get("last_status", "NEVER RUN"),
        pipeline.get("last_run"),
        pipeline.get("last_duration"),
        pipeline.get("current_step", -1),
        pipeline.get("run_count", 0),
        pipeline.get("success_count", 0),
        pipeline.get("fail_count", 0),
        pipeline.get("created_at", now_str()),
    )

def row_to_pipeline(row):
    return {
        "id": row["id"],
        "owner_user": row["owner_user"],
        "name": row["name"],
        "description": row["description"],
        "workspace_id": row["workspace_id"],
        "workspace_name": row["workspace_name"],
        "model_id": row["model_id"],
        "model_name": row["model_name"],
        "steps": json.loads(row["steps_json"] or "[]"),
        "schedule_type": row["schedule_type"],
        "schedule_value": row["schedule_value"],
        "stop_on_failure": bool(row["stop_on_failure"]),
        "enabled": bool(row["enabled"]),
        "auth": "",
        "status": row["status"],
        "last_status": row["last_status"],
        "last_run": row["last_run"],
        "last_duration": row["last_duration"],
        "current_step": row["current_step"],
        "run_count": row["run_count"],
        "success_count": row["success_count"],
        "fail_count": row["fail_count"],
        "created_at": row["created_at"],
    }

def save_pipeline_to_db(pipeline):
    with db_lock:
        with db_engine.begin() as conn:
            conn.execute(
                text(
                """
                INSERT INTO pipelines (
                    id, owner_user, name, description, workspace_id, workspace_name, model_id, model_name,
                    steps_json, schedule_type, schedule_value, stop_on_failure, enabled, status,
                    last_status, last_run, last_duration, current_step, run_count, success_count,
                    fail_count, created_at
                ) VALUES (
                    :id, :owner_user, :name, :description, :workspace_id, :workspace_name, :model_id, :model_name,
                    :steps_json, :schedule_type, :schedule_value, :stop_on_failure, :enabled, :status,
                    :last_status, :last_run, :last_duration, :current_step, :run_count, :success_count,
                    :fail_count, :created_at
                )
                ON CONFLICT(id) DO UPDATE SET
                    owner_user=excluded.owner_user,
                    name=excluded.name,
                    description=excluded.description,
                    workspace_id=excluded.workspace_id,
                    workspace_name=excluded.workspace_name,
                    model_id=excluded.model_id,
                    model_name=excluded.model_name,
                    steps_json=excluded.steps_json,
                    schedule_type=excluded.schedule_type,
                    schedule_value=excluded.schedule_value,
                    stop_on_failure=excluded.stop_on_failure,
                    enabled=excluded.enabled,
                    status=excluded.status,
                    last_status=excluded.last_status,
                    last_run=excluded.last_run,
                    last_duration=excluded.last_duration,
                    current_step=excluded.current_step,
                    run_count=excluded.run_count,
                    success_count=excluded.success_count,
                    fail_count=excluded.fail_count,
                    created_at=excluded.created_at
                """
                ),
                {
                    "id": pipeline["id"],
                    "owner_user": pipeline.get("owner_user", ""),
                    "name": pipeline.get("name", "Unnamed Pipeline"),
                    "description": pipeline.get("description", ""),
                    "workspace_id": pipeline.get("workspace_id", ""),
                    "workspace_name": pipeline.get("workspace_name", ""),
                    "model_id": pipeline.get("model_id", ""),
                    "model_name": pipeline.get("model_name", ""),
                    "steps_json": json.dumps(pipeline.get("steps", [])),
                    "schedule_type": pipeline.get("schedule_type", "manual"),
                    "schedule_value": pipeline.get("schedule_value", ""),
                    "stop_on_failure": 1 if pipeline.get("stop_on_failure", True) else 0,
                    "enabled": 1 if pipeline.get("enabled", True) else 0,
                    "status": pipeline.get("status", "IDLE"),
                    "last_status": pipeline.get("last_status", "NEVER RUN"),
                    "last_run": pipeline.get("last_run"),
                    "last_duration": pipeline.get("last_duration"),
                    "current_step": pipeline.get("current_step", -1),
                    "run_count": pipeline.get("run_count", 0),
                    "success_count": pipeline.get("success_count", 0),
                    "fail_count": pipeline.get("fail_count", 0),
                    "created_at": pipeline.get("created_at", now_str()),
                },
            )

def delete_pipeline_from_db(pipeline_id, owner_user):
    with db_lock:
        execute_sql(
            "DELETE FROM pipelines WHERE id = :pipeline_id AND owner_user = :owner_user",
            {"pipeline_id": pipeline_id, "owner_user": owner_user},
        )

def trim_table(conn, table_name, max_rows):
    conn.execute(text(f"""
        DELETE FROM {table_name}
        WHERE id NOT IN (
            SELECT id FROM {table_name}
            ORDER BY time DESC
            LIMIT :max_rows
        )
        """), {"max_rows": max_rows})

def save_alert_to_db(alert):
    with db_lock:
        with db_engine.begin() as conn:
            conn.execute(
                text(
                """
                INSERT INTO alerts (id, owner_user, pipeline_id, pipeline_name, level, message, time)
                VALUES (:id, :owner_user, :pipeline_id, :pipeline_name, :level, :message, :time)
                """,
                ),
                alert,
            )
            trim_table(conn, "alerts", 100)

def dismiss_alert_from_db(alert_id, owner_user):
    with db_lock:
        execute_sql(
            "DELETE FROM alerts WHERE id = :alert_id AND owner_user = :owner_user",
            {"alert_id": alert_id, "owner_user": owner_user},
        )

def save_history_to_db(run):
    with db_lock:
        with db_engine.begin() as conn:
            conn.execute(
                text(
                """
                INSERT INTO job_history (
                    id, owner_user, pipeline_id, pipeline_name, action_type, action_name,
                    status, duration_s, details, time
                ) VALUES (
                    :id, :owner_user, :pipeline_id, :pipeline_name, :action_type, :action_name,
                    :status, :duration_s, :details, :time
                )
                """,
                ),
                run,
            )
            trim_table(conn, "job_history", 500)

def load_state_from_db():
    global pipelines, job_history, alerts
    with db_lock:
        pipeline_rows = fetch_all("SELECT * FROM pipelines ORDER BY created_at DESC")
        history_rows = fetch_all("SELECT * FROM job_history ORDER BY time DESC LIMIT 500")
        alert_rows = fetch_all("SELECT * FROM alerts ORDER BY time DESC LIMIT 100")

    pipelines = {row["id"]: row_to_pipeline(row) for row in pipeline_rows}
    job_history = deque(
        [
            {
                "id": row["id"],
                "owner_user": row["owner_user"],
                "pipeline_id": row["pipeline_id"],
                "pipeline_name": row["pipeline_name"],
                "action_type": row["action_type"],
                "action_name": row["action_name"],
                "status": row["status"],
                "duration_s": row["duration_s"],
                "details": row["details"],
                "time": row["time"],
            }
            for row in history_rows
        ],
        maxlen=500,
    )
    alerts = deque(
        [
            {
                "id": row["id"],
                "owner_user": row["owner_user"],
                "pipeline_id": row["pipeline_id"],
                "pipeline_name": row["pipeline_name"],
                "level": row["level"],
                "message": row["message"],
                "time": row["time"],
            }
            for row in alert_rows
        ],
        maxlen=100,
    )

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def anaplan_headers(auth):
    return {"Authorization": auth, "Content-Type": "application/json"}

def get_auth():
    if not has_request_context():
        return ""
    return session.get("auth_header", "")

def normalize_auth_header(raw_auth):
    """Accept a copied Authorization header, raw token value, or common pasted variants."""
    auth = (raw_auth or "").strip()
    if not auth:
        return ""

    if auth.lower().startswith("authorization:"):
        auth = auth.split(":", 1)[1].strip()

    # Handle users pasting quoted token values from other tools.
    auth = auth.strip().strip("'").strip('"')

    if auth.startswith("{") and "tokenValue" in auth:
        try:
            parsed = json.loads(auth)
            auth = (
                parsed.get("tokenInfo", {}).get("tokenValue")
                or parsed.get("tokenValue")
                or auth
            )
        except Exception:
            pass

    lowered = auth.lower()
    if lowered.startswith("anaplanauthtoken ") or lowered.startswith("bearer ") or lowered.startswith("basic "):
        return auth
    return f"AnaplanAuthToken {auth}"

def safe_json(response):
    try:
        return response.json()
    except Exception:
        return {}

def response_error_message(response, fallback):
    payload = safe_json(response)
    return (
        payload.get("statusMessage")
        or payload.get("message")
        or payload.get("error")
        or (response.text or "").strip()
        or fallback
    )

def mask_auth_header(auth_header):
    auth = (auth_header or "").strip()
    if not auth:
        return ""
    if " " not in auth:
        return f"{auth[:6]}..."
    scheme, value = auth.split(" ", 1)
    value = value.strip()
    if len(value) <= 10:
        masked = "*" * len(value)
    else:
        masked = f"{value[:6]}...{value[-4:]}"
    return f"{scheme} {masked}"

def anaplan_get(auth, path):
    try:
        r = http.get(ANAPLAN_BASE + path, headers=anaplan_headers(auth), timeout=30)
        return r.status_code, r.json() if r.text else {}
    except Exception as e:
        return 500, {"error": str(e)}

def anaplan_post(auth, path, body=None):
    try:
        r = http.post(ANAPLAN_BASE + path, headers=anaplan_headers(auth), json=body or {}, timeout=30)
        return r.status_code, r.json() if r.text else {}
    except Exception as e:
        return 500, {"error": str(e)}

def poll_task(auth, task_path, max_wait=120):
    """Poll a task until complete or timeout."""
    start = time.time()
    while time.time() - start < max_wait:
        status, data = anaplan_get(auth, task_path)
        if status != 200:
            return "FAILED", data
        task_state = data.get("task", {}).get("taskState", "")
        if task_state in ("COMPLETE", "COMPLETE_WITH_ERRORS"):
            result = data.get("task", {}).get("result", {})
            failed = result.get("failedCounts", 0)
            return ("COMPLETE_WITH_ERRORS" if failed else "COMPLETE"), data
        if task_state == "FAILED":
            return "FAILED", data
        time.sleep(3)
    return "TIMEOUT", {}

def add_alert(pipeline_id, pipeline_name, level, message):
    owner_user = pipelines.get(pipeline_id, {}).get("owner_user", "")
    alert = {
        "id": str(uuid.uuid4())[:8],
        "owner_user": owner_user,
        "pipeline_id": pipeline_id,
        "pipeline_name": pipeline_name,
        "level": level,   # "error" | "warning" | "success"
        "message": message,
        "time": now_str(),
    }
    alerts.appendleft(alert)
    save_alert_to_db(alert)

def record_run(pipeline_id, pipeline_name, action_type, action_name,
               status, duration_s, details=""):
    owner_user = pipelines.get(pipeline_id, {}).get("owner_user", "")
    run = {
        "id": str(uuid.uuid4())[:8],
        "owner_user": owner_user,
        "pipeline_id": pipeline_id,
        "pipeline_name": pipeline_name,
        "action_type": action_type,
        "action_name": action_name,
        "status": status,     # SUCCESS | FAILED | RUNNING | WARNING
        "duration_s": round(duration_s, 1),
        "details": details,
        "time": now_str(),
    }
    job_history.appendleft(run)
    save_history_to_db(run)

# ─────────────────────────────────────────
# EXECUTE A SINGLE PIPELINE STEP
# ─────────────────────────────────────────
def execute_step(auth, step, ws_id, model_id, pipeline_id, pipeline_name):
    action_type = step.get("type")          # import | export | action | process
    action_id   = step.get("action_id", "")
    action_name = step.get("name", action_type)
    start       = time.time()

    # Map type to API path segment
    type_map = {
        "import":  "imports",
        "export":  "exports",
        "action":  "actions",
        "process": "actions",
    }
    segment = type_map.get(action_type, "actions")
    task_path = f"/workspaces/{ws_id}/models/{model_id}/{segment}/{action_id}/tasks"

    status_code, task_resp = anaplan_post(auth, task_path, {"localeName": "en_US"})
    if status_code not in (200, 201):
        duration = time.time() - start
        record_run(pipeline_id, pipeline_name, action_type, action_name,
                   "FAILED", duration, str(task_resp))
        add_alert(pipeline_id, pipeline_name, "error",
                  f"Step '{action_name}' failed to start: HTTP {status_code}")
        return False

    task_id = (task_resp.get("task", {}).get("taskId")
               or task_resp.get("taskId", ""))
    if not task_id:
        duration = time.time() - start
        record_run(pipeline_id, pipeline_name, action_type, action_name,
                   "FAILED", duration, "No taskId in response")
        add_alert(pipeline_id, pipeline_name, "error",
                  f"Step '{action_name}' returned no taskId")
        return False

    poll_path = f"/workspaces/{ws_id}/models/{model_id}/{segment}/{action_id}/tasks/{task_id}"
    final_state, final_data = poll_task(auth, poll_path)
    duration = time.time() - start

    if final_state == "COMPLETE":
        record_run(pipeline_id, pipeline_name, action_type, action_name,
                   "SUCCESS", duration, "Completed successfully")
        return True
    elif final_state == "COMPLETE_WITH_ERRORS":
        result = final_data.get("task", {}).get("result", {})
        failed = result.get("failedCounts", 0)
        record_run(pipeline_id, pipeline_name, action_type, action_name,
                   "WARNING", duration, f"{failed} rows failed")
        add_alert(pipeline_id, pipeline_name, "warning",
                  f"Step '{action_name}' completed with {failed} row errors")
        return True  # continue pipeline
    else:
        record_run(pipeline_id, pipeline_name, action_type, action_name,
                   "FAILED", duration, f"State: {final_state}")
        add_alert(pipeline_id, pipeline_name, "error",
                  f"Step '{action_name}' ended with state: {final_state}")
        return False

# ─────────────────────────────────────────
# RUN A FULL PIPELINE
# ─────────────────────────────────────────
def run_pipeline(pipeline_id, auth=None, owner_user=None):
    pipeline = pipelines.get(pipeline_id)
    if not pipeline:
        return
    if owner_user is not None and pipeline.get("owner_user") and pipeline.get("owner_user") != owner_user:
        return

    if auth is None:
        auth = pipeline.get("auth", "")
    if not auth:
        auth = get_auth()
    if not auth:
        add_alert(pipeline_id, pipeline.get("name", ""), "error", "No auth token available for scheduled run")
        return

    ws_id      = pipeline["workspace_id"]
    model_id   = pipeline["model_id"]
    name       = pipeline["name"]
    steps      = pipeline.get("steps", [])

    # Update pipeline state
    pipelines[pipeline_id]["status"]    = "RUNNING"
    pipelines[pipeline_id]["last_run"]  = now_str()
    pipelines[pipeline_id]["run_count"] = pipeline.get("run_count", 0) + 1
    save_pipeline_to_db(pipelines[pipeline_id])

    pipeline_start = time.time()
    success_count  = 0
    failed_step    = None

    for i, step in enumerate(steps):
        pipelines[pipeline_id]["current_step"] = i
        save_pipeline_to_db(pipelines[pipeline_id])
        ok = execute_step(auth, step, ws_id, model_id, pipeline_id, name)
        if ok:
            success_count += 1
        else:
            failed_step = step.get("name", f"Step {i+1}")
            if pipeline.get("stop_on_failure", True):
                break

    total_duration = time.time() - pipeline_start
    pipelines[pipeline_id]["current_step"] = -1
    pipelines[pipeline_id]["last_duration"] = round(total_duration, 1)

    if failed_step:
        pipelines[pipeline_id]["status"]        = "FAILED"
        pipelines[pipeline_id]["last_status"]   = "FAILED"
        pipelines[pipeline_id]["fail_count"]    = pipeline.get("fail_count", 0) + 1
        add_alert(pipeline_id, name, "error",
                  f"Pipeline '{name}' FAILED at step '{failed_step}' after {round(total_duration,1)}s")
    else:
        pipelines[pipeline_id]["status"]        = "SUCCESS"
        pipelines[pipeline_id]["last_status"]   = "SUCCESS"
        pipelines[pipeline_id]["success_count"] = pipeline.get("success_count", 0) + 1
        add_alert(pipeline_id, name, "success",
                  f"Pipeline '{name}' completed successfully in {round(total_duration,1)}s ({success_count} steps)")
    save_pipeline_to_db(pipelines[pipeline_id])


# ─────────────────────────────────────────
# SCHEDULER THREAD
# ─────────────────────────────────────────
def run_scheduler():
    global scheduler_running
    while scheduler_running:
        schedule.run_pending()
        time.sleep(10)

def start_scheduler():
    global scheduler_running, scheduler_thread
    if not scheduler_running:
        scheduler_running = True
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()

def reschedule_pipeline(pipeline_id):
    """Remove old schedule jobs for this pipeline and re-add."""
    tag = f"pipeline_{pipeline_id}"
    schedule.clear(tag)

    pipeline = pipelines.get(pipeline_id)
    if not pipeline or not pipeline.get("enabled"):
        return

    schedule_type = pipeline.get("schedule_type", "manual")
    schedule_val  = pipeline.get("schedule_value", "")

    def job():
        pipeline = pipelines.get(pipeline_id, {})
        run_pipeline(
            pipeline_id,
            pipeline.get("auth", ""),
            pipeline.get("owner_user", ""),
        )

    if schedule_type == "interval_minutes":
        mins = int(schedule_val or 60)
        schedule.every(mins).minutes.do(job).tag(tag)
    elif schedule_type == "daily":
        t = schedule_val or "00:00"
        schedule.every().day.at(t).do(job).tag(tag)
    elif schedule_type == "hourly":
        schedule.every().hour.do(job).tag(tag)

init_db()
load_state_from_db()
if ENABLE_SCHEDULER:
    start_scheduler()

# ─────────────────────────────────────────
# ROUTES — PAGES
# ─────────────────────────────────────────
@app.route("/")
def index():
    enforce_session_expiry()
    return render_template(
        "dashboard.html",
        logged_in=bool(get_session_status()["authenticated"]),
        user=get_current_user(),
        auth_header=get_auth(),
        session_status=get_session_status(),
    )

@app.route("/admin/db")
def db_admin():
    user, error = require_user()
    if error:
        return error

    table = request.args.get("table", "pipelines")
    allowed_tables = ("pipelines", "job_history", "alerts")
    if table not in allowed_tables:
        table = "pipelines"

    with db_lock:
        tables = []
        for name in allowed_tables:
            count = fetch_scalar(
                f"SELECT COUNT(*) FROM {name} WHERE owner_user = :owner_user",
                {"owner_user": user},
                0,
            )
            tables.append({"name": name, "count": count})
        rows = fetch_all(
            f"SELECT * FROM {table} WHERE owner_user = :owner_user ORDER BY time DESC LIMIT 100"
            if table in ("job_history", "alerts")
            else f"SELECT * FROM {table} WHERE owner_user = :owner_user ORDER BY created_at DESC LIMIT 100",
            {"owner_user": user},
        )
        if db_engine.dialect.name == "postgresql":
            columns = [row["column_name"] for row in fetch_all(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
                ORDER BY ordinal_position
                """,
                {"table_name": table},
            )]
        else:
            columns = [row["name"] for row in fetch_all(f"PRAGMA table_info({table})")]

    return render_template(
        "db_admin.html",
        tables=tables,
        selected_table=table,
        columns=columns,
        rows=rows,
        db_name=os.path.basename(DB_PATH),
    )

# ─────────────────────────────────────────
# ROUTES — AUTH
# ─────────────────────────────────────────
@app.route("/auth/login", methods=["POST"])
def login():
    data = request.json or {}
    auth_type = data.get("auth_type", "basic")

    if auth_type == "token":
        token = data.get("token", "").strip()
        auth_header = normalize_auth_header(token)
        if not auth_header:
            return jsonify({"success": False, "error": "Bearer token is required."}), 400

        try:
            test = http.get(
                ANAPLAN_BASE + "/workspaces",
                headers={"Authorization": auth_header},
                timeout=10,
            )
            if test.status_code == 401:
                return jsonify({
                    "success": False,
                    "error": "Token rejected by Anaplan (401). Check that it is valid and not expired.",
                }), 401
            if test.status_code >= 400:
                return jsonify({
                    "success": False,
                    "error": f"Unexpected response from Anaplan: HTTP {test.status_code}. Response: {(test.text or '')[:200]}",
                }), test.status_code
        except requests.exceptions.RequestException as e:
            return jsonify({"success": False, "error": f"Network error: {e}"}), 500

        session["auth_header"] = auth_header
        expires_at = mark_auth_session("Token User", auth_header)
        return jsonify({
            "success": True,
            "user": session["user"],
            "auth": auth_header,
            "expires_at": expires_at,
            "expires_in_seconds": SESSION_TIMEOUT_SECONDS,
        })

    email = data.get("email", "").strip()
    password = data.get("password", "")
    if not email or not password:
        return jsonify({"success": False, "error": "Email and password are required."}), 400

    encoded = base64.b64encode(f"{email}:{password}".encode()).decode()
    basic_token = f"Basic {encoded}"

    try:
        auth_resp = http.post(
            AUTH_URL,
            headers={
                "Authorization": basic_token,
                "Content-Type": "application/json",
            },
            timeout=15,
        )

        if auth_resp.ok:
            auth_json = safe_json(auth_resp)
            token_value = (
                auth_json.get("tokenInfo", {}).get("tokenValue")
                or auth_json.get("token")
                or auth_json.get("access_token")
            )

            if token_value:
                auth_header = f"AnaplanAuthToken {token_value}"
                expires_at = mark_auth_session(email, auth_header)
                return jsonify({
                    "success": True,
                    "user": email,
                    "auth": auth_header,
                    "expires_at": expires_at,
                    "expires_in_seconds": SESSION_TIMEOUT_SECONDS,
                    "note": "Authenticated via Anaplan auth service.",
                })

        print(f"[auth] Auth service returned {auth_resp.status_code}: {(auth_resp.text or '')[:300]}")
    except requests.exceptions.RequestException as e:
        print(f"[auth] Auth service unreachable: {e}")

    try:
        test = http.get(
            ANAPLAN_BASE + "/workspaces",
            headers={"Authorization": basic_token},
            timeout=10,
        )

        if test.status_code == 200:
            expires_at = mark_auth_session(email, basic_token)
            return jsonify({
                "success": True,
                "user": email,
                "auth": basic_token,
                "expires_at": expires_at,
                "expires_in_seconds": SESSION_TIMEOUT_SECONDS,
                "note": "Authenticated with Basic Auth.",
            })

        if test.status_code == 401:
            return jsonify({
                "success": False,
                "error": (
                    "Credentials rejected by Anaplan (401).\n\n"
                    "Common causes:\n"
                    "• Wrong email or password\n"
                    "• Your account uses SSO / OAuth2 (not email+password)\n"
                    "• MFA is required — generate a token instead\n\n"
                    "Tip: If your company uses SSO, generate a token from the Anaplan portal and use token login."
                ),
            }), 401

        return jsonify({
            "success": False,
            "error": f"Unexpected response from Anaplan: HTTP {test.status_code}. Response: {(test.text or '')[:200]}",
        }), test.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "error": f"Could not reach Anaplan API: {e}"}), 500

@app.route("/auth/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"success": True})

@app.route("/auth/debug", methods=["POST"])
def debug_auth():
    data = request.json or {}
    auth_type = data.get("auth_type", "token")

    if auth_type == "basic":
        email = data.get("email", "").strip()
        pwd = data.get("password", "")
        if not email or not pwd:
            return jsonify({"success": False, "error": "Email and password required"}), 400

        try:
            auth_resp = http.post(AUTH_URL, auth=(email, pwd), timeout=15)
            return jsonify({
                "success": auth_resp.status_code == 200,
                "request": {
                    "auth_type": "basic",
                    "email": email,
                    "endpoint": AUTH_URL,
                },
                "anaplan": {
                    "status_code": auth_resp.status_code,
                    "message": response_error_message(auth_resp, "No response body"),
                    "body": safe_json(auth_resp) or (auth_resp.text or "").strip(),
                },
            }), auth_resp.status_code
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 502

    auth_header = normalize_auth_header(data.get("token", ""))
    if not auth_header:
        return jsonify({"success": False, "error": "Token required"}), 400

    try:
        test = http.get(
            ANAPLAN_BASE + "/workspaces",
            headers={"Authorization": auth_header},
            timeout=10,
        )
        return jsonify({
            "success": test.status_code == 200,
            "request": {
                "auth_type": "token",
                "endpoint": ANAPLAN_BASE + "/workspaces",
                "authorization_preview": mask_auth_header(auth_header),
            },
            "anaplan": {
                "status_code": test.status_code,
                "message": response_error_message(test, "No response body"),
                "body": safe_json(test) or (test.text or "").strip(),
            },
        }), test.status_code
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 502

# ─────────────────────────────────────────
# ROUTES — ANAPLAN PROXY (for dropdowns)
# ─────────────────────────────────────────
@app.route("/proxy/workspaces")
def proxy_workspaces():
    auth = request.args.get("auth") or get_auth()
    sc, data = anaplan_get(auth, "/workspaces")
    return jsonify(data), sc

@app.route("/proxy/models")
def proxy_models():
    auth = request.args.get("auth") or get_auth()
    ws   = request.args.get("ws_id", "")
    sc, data = anaplan_get(auth, f"/workspaces/{ws}/models")
    return jsonify(data), sc

@app.route("/proxy/imports")
def proxy_imports():
    auth = request.args.get("auth") or get_auth()
    ws   = request.args.get("ws_id", "")
    m    = request.args.get("model_id", "")
    sc, data = anaplan_get(auth, f"/workspaces/{ws}/models/{m}/imports")
    return jsonify(data), sc

@app.route("/proxy/exports")
def proxy_exports():
    auth = request.args.get("auth") or get_auth()
    ws   = request.args.get("ws_id", "")
    m    = request.args.get("model_id", "")
    sc, data = anaplan_get(auth, f"/workspaces/{ws}/models/{m}/exports")
    return jsonify(data), sc

@app.route("/proxy/actions")
def proxy_actions():
    auth = request.args.get("auth") or get_auth()
    ws   = request.args.get("ws_id", "")
    m    = request.args.get("model_id", "")
    sc, data = anaplan_get(auth, f"/workspaces/{ws}/models/{m}/actions")
    return jsonify(data), sc

# ─────────────────────────────────────────
# ROUTES — PIPELINES CRUD
# ─────────────────────────────────────────
@app.route("/pipelines", methods=["GET"])
def list_pipelines():
    user, error = require_user()
    if error:
        return error
    visible = [p for p in pipelines.values() if p.get("owner_user") == user]
    return jsonify(visible)

@app.route("/pipelines", methods=["POST"])
def create_pipeline():
    user, error = require_user()
    if error:
        return error
    data = request.json or {}
    pid  = str(uuid.uuid4())[:8]
    auth = data.get("auth", get_auth())

    pipeline = {
        "id":             pid,
        "owner_user":     user,
        "name":           data.get("name", "Unnamed Pipeline"),
        "description":    data.get("description", ""),
        "workspace_id":   data.get("workspace_id", ""),
        "workspace_name": data.get("workspace_name", ""),
        "model_id":       data.get("model_id", ""),
        "model_name":     data.get("model_name", ""),
        "steps":          data.get("steps", []),
        "schedule_type":  data.get("schedule_type", "manual"),
        "schedule_value": data.get("schedule_value", ""),
        "stop_on_failure":data.get("stop_on_failure", True),
        "enabled":        data.get("enabled", True),
        "auth":           auth,
        "status":         "IDLE",
        "last_status":    "NEVER RUN",
        "last_run":       None,
        "last_duration":  None,
        "current_step":   -1,
        "run_count":      0,
        "success_count":  0,
        "fail_count":     0,
        "created_at":     now_str(),
    }
    pipelines[pid] = pipeline
    save_pipeline_to_db(pipeline)
    reschedule_pipeline(pid)
    return jsonify({"success": True, "pipeline": pipeline})

@app.route("/pipelines/<pid>", methods=["GET"])
def get_pipeline(pid):
    user, error = require_user()
    if error:
        return error
    p = pipelines.get(pid)
    if not p or p.get("owner_user") != user:
        return jsonify({"error": "Not found"}), 404
    return jsonify(p)

@app.route("/pipelines/<pid>", methods=["PUT"])
def update_pipeline(pid):
    user, error = require_user()
    if error:
        return error
    if pid not in pipelines or pipelines[pid].get("owner_user") != user:
        return jsonify({"error": "Not found"}), 404
    data = request.json or {}
    existing_auth = pipelines[pid].get("auth", "")
    for k, v in data.items():
        if k not in ("id", "auth"):
            pipelines[pid][k] = v
    pipelines[pid]["auth"] = data.get("auth", existing_auth)
    save_pipeline_to_db(pipelines[pid])
    reschedule_pipeline(pid)
    return jsonify({"success": True, "pipeline": pipelines[pid]})

@app.route("/pipelines/<pid>", methods=["DELETE"])
def delete_pipeline(pid):
    user, error = require_user()
    if error:
        return error
    if pid in pipelines and pipelines[pid].get("owner_user") == user:
        schedule.clear(f"pipeline_{pid}")
        del pipelines[pid]
        delete_pipeline_from_db(pid, user)
    return jsonify({"success": True})

@app.route("/pipelines/<pid>/run", methods=["POST"])
def trigger_pipeline(pid):
    user, error = require_user()
    if error:
        return error
    if pid not in pipelines or pipelines[pid].get("owner_user") != user:
        return jsonify({"error": "Pipeline not found"}), 404
    if pipelines[pid]["status"] == "RUNNING":
        return jsonify({"error": "Pipeline is already running"}), 409

    auth = request.json.get("auth", "") if request.json else ""
    if auth:
        pipelines[pid]["auth"] = auth

    t = threading.Thread(
        target=run_pipeline,
        args=(pid, pipelines[pid].get("auth", ""), user),
        daemon=True,
    )
    t.start()
    return jsonify({"success": True, "message": f"Pipeline '{pipelines[pid]['name']}' started"})

@app.route("/pipelines/<pid>/toggle", methods=["POST"])
def toggle_pipeline(pid):
    user, error = require_user()
    if error:
        return error
    if pid not in pipelines or pipelines[pid].get("owner_user") != user:
        return jsonify({"error": "Not found"}), 404
    pipelines[pid]["enabled"] = not pipelines[pid].get("enabled", True)
    save_pipeline_to_db(pipelines[pid])
    reschedule_pipeline(pid)
    return jsonify({"success": True, "enabled": pipelines[pid]["enabled"]})

# ─────────────────────────────────────────
# ROUTES — HISTORY & ALERTS
# ─────────────────────────────────────────
@app.route("/history")
def get_history():
    user, error = require_user()
    if error:
        return error
    pid = request.args.get("pipeline_id")
    h   = [r for r in job_history if r.get("owner_user") == user]
    if pid:
        h = [r for r in h if r["pipeline_id"] == pid]
    return jsonify(h[:100])

@app.route("/alerts")
def get_alerts():
    user, error = require_user()
    if error:
        return error
    return jsonify([a for a in alerts if a.get("owner_user") == user][:50])

@app.route("/alerts/<aid>/dismiss", methods=["POST"])
def dismiss_alert(aid):
    user, error = require_user()
    if error:
        return error
    global alerts
    alerts = deque((a for a in alerts if not (a["id"] == aid and a.get("owner_user") == user)), maxlen=100)
    dismiss_alert_from_db(aid, user)
    return jsonify({"success": True})

# ─────────────────────────────────────────
# ROUTES — STATS
# ─────────────────────────────────────────
@app.route("/stats")
def get_stats():
    user, error = require_user()
    if error:
        return error
    session_status = get_session_status()
    user_pipelines = [p for p in pipelines.values() if p.get("owner_user") == user]
    total    = len(user_pipelines)
    running  = sum(1 for p in user_pipelines if p["status"] == "RUNNING")
    failed   = sum(1 for p in user_pipelines if p["last_status"] == "FAILED")
    success  = sum(1 for p in user_pipelines if p["last_status"] == "SUCCESS")
    h        = [r for r in job_history if r.get("owner_user") == user]
    total_runs      = len(h)
    successful_runs = sum(1 for r in h if r["status"] == "SUCCESS")
    failed_runs     = sum(1 for r in h if r["status"] == "FAILED")
    avg_duration    = (sum(r["duration_s"] for r in h) / len(h)) if h else 0
    unread_alerts   = len([a for a in alerts if a.get("owner_user") == user and a["level"] == "error"])
    return jsonify({
        "total_pipelines":  total,
        "running":          running,
        "failed_pipelines": failed,
        "success_pipelines":success,
        "total_runs":       total_runs,
        "successful_runs":  successful_runs,
        "failed_runs":      failed_runs,
        "avg_duration_s":   round(avg_duration, 1),
        "unread_alerts":    unread_alerts,
        "session":          session_status,
    })

if __name__ == "__main__":
    print("\n🚀 Anaplan Data Pipeline Dashboard")
    print("━" * 42)
    print("  Open → http://localhost:5001")
    print("━" * 42 + "\n")
    app.run(debug=env_flag("FLASK_DEBUG", True), host="0.0.0.0", port=int(os.getenv("PORT", "5001")))
