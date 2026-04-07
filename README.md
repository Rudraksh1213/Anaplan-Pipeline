# 🚀 Anaplan Automated Data Pipeline Dashboard

A real-time pipeline monitoring and automation dashboard for Anaplan.
Build, schedule, and monitor multi-step data pipelines with live status tracking.

---

## 🚀 Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the app
python app.py

# 3. Open in browser
http://localhost:5001
```

## Render Deploy

This repo is now prepared for Render with [`render.yaml`](/c:/Users/RudrakshLohiya/Desktop/anaplan-pipeline-dashboard/anaplan-pipeline/render.yaml). The blueprint creates:

- one free Python web service
- one free Render Postgres database

### Deploy steps

1. Push this project to GitHub.
2. In Render, create a new Blueprint and connect the repo.
3. Render will detect `render.yaml` and create the web service automatically.
4. Wait for the first deploy to complete, then open the generated `onrender.com` URL.
5. Render will inject `DATABASE_URL` automatically from the provisioned Postgres instance.

### Important free-tier notes

- The free web service sleeps after idle time.
- local SQLite is still used automatically for local development when `DATABASE_URL` is not set.
- on Render, saved pipelines/history/alerts now go to Postgres instead of local SQLite.
- The in-app scheduler is disabled in `render.yaml` because sleeping/restarts make it unreliable on free hosting.
- Manual use and demos are fine, but production scheduling should use a persistent database and a non-sleeping host.
- Free Render Postgres expires after 30 days unless upgraded, so it is still best for demo/testing use.

---

## ✨ Features

### 📊 Dashboard
- Live pipeline status cards (auto-refresh every 5s)
- Real-time stats: total runs, success rate, avg duration
- Recent activity feed

### ⚡ Pipeline Builder
- Visual drag-and-drop style step builder
- Load workspaces → models → actions dynamically from Anaplan
- Chain: Imports → Exports → Actions → Processes in any order
- Stop-on-failure control per pipeline

### 🕐 Scheduler
- Manual (trigger anytime)
- Every N minutes
- Hourly
- Daily at a specific time (e.g. 09:00)

### 📋 Run History
- Full audit trail of every step execution
- Filter by pipeline
- Status: SUCCESS / FAILED / WARNING
- Duration tracking

### 🔔 Alerts
- Real-time error/warning/success notifications
- Per-step failure details
- Row-level error counts for imports

---

## 📁 Project Structure

```
anaplan-pipeline/
├── app.py                  ← Flask server + scheduler + Anaplan proxy
├── requirements.txt        ← flask, requests, schedule
├── README.md
└── templates/
    └── dashboard.html      ← Full dashboard UI
```

---

## 🔧 How Pipelines Work

1. **Create** a pipeline — select workspace, model, add steps
2. **Run manually** with the ▶ Run button, or set a schedule
3. Each step runs **sequentially** — import, then export, then action, etc.
4. Task status is **polled automatically** until complete
5. Results appear in **Run History** and **Alerts** in real-time

---

## 💡 Example Pipeline: Nightly Sales Load

```
Step 1: IMPORT  → "Load Sales Data from CSV"
Step 2: ACTION  → "Calculate Forecasts Process"  
Step 3: EXPORT  → "Export Summary to BI Tool"
Schedule: Daily at 23:00
```

This runs automatically every night at 11 PM with zero human intervention.
