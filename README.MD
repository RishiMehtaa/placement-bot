# WhatsApp Placement Intelligence System

A production-grade system that listens to a WhatsApp placement group, extracts structured opportunity data, deduplicates it, and syncs it to Google Sheets and Google Calendar — automatically.

---

## What It Does

- Connects to WhatsApp via a linked device (QR scan) using Baileys — **read-only**
- Captures messages from exactly one placement group
- Extracts structured fields: **Company, Role, Deadline, Package, Apply Link**
- Groups related messages into placement "families"
- Deduplicates across 3 layers (message ID, content hash, URL normalisation)
- Syncs clean records to **Google Sheets** and **Google Calendar**
- Designed for local use first, with optional AWS / Azure cloud deployment

---

## System Architecture
```
WhatsApp Group
  → Baileys (Node.js)         — read-only listener
  → POST /ingest              — FastAPI receiver
  → PostgreSQL                — raw message storage
  → Queue                     — decoupled processing
  → Processing Pipeline       — preprocess → regex → context → LLM → normalize
  → Family Records            — structured placement opportunities
  → Google Sheets             — human-readable output
  → Google Calendar           — deadline tracking
  → React Dashboard           — optional frontend (Phase 18)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| OS / IDE | Windows 11 + VSCode |
| WhatsApp | @whiskeysockets/baileys (read-only) |
| API | FastAPI + Pydantic + SQLAlchemy (async) |
| Database | PostgreSQL 15+ |
| Queue (local) | PostgreSQL-backed |
| Queue (cloud) | AWS SQS or Azure Queue (Phase 17) |
| LLM | OpenAI gpt-4o-mini (Phase 9 only) |
| Google APIs | google-auth + googleapiclient via service account |
| Frontend | React + Vite (Phase 18 only) |
| Runtime | Python 3.12 + Node.js LTS |

---

## Project Structure
```
placement_bot/
├── baileys/
│   ├── index.js               # WhatsApp listener (read-only)
│   ├── package.json
│   └── sessions/              # Baileys auth — never commit
├── scraper/
│   └── receiver.py            # FastAPI /ingest endpoint
├── extraction/
│   ├── preprocessor.py        # Text cleaning and URL extraction
│   ├── regex_extractor.py     # Deadline and package regex
│   ├── context_resolver.py    # Reply chain and sliding window
│   ├── llm_extractor.py       # OpenAI fallback (Phase 9)
│   └── normalizer.py          # Field normalisation (Phase 10)
├── db/
│   ├── models.py              # SQLAlchemy ORM models
│   ├── database.py            # Async engine and session
│   └── queries.py             # DB query helpers
├── integrations/
│   ├── sheets.py              # Google Sheets sync (Phase 15)
│   └── calendar.py            # Google Calendar sync (Phase 16)
├── utils/
│   └── logger.py              # Structured logger (structlog)
├── config/
│   ├── settings.py            # Centralised settings via pydantic-settings
│   ├── normalizations.json    # Company/role alias maps (Phase 10)
│   └── google_service_account.json  # Never commit — add to .gitignore
├── tests/
├── logs/
├── .env                       # Never commit — secrets live here
├── requirements.txt
└── main.py                    # Entry point
```

---

## Build Phases

| Phase | Description | Status |
|---|---|---|
| 0 | Environment Setup | ✅ Complete |
| 1 | Baileys WhatsApp Listener | 🔲 |
| 2 | FastAPI Receiver | 🔲 |
| 3 | PostgreSQL Schema | 🔲 |
| 4 | Ingest → Database Integration | 🔲 |
| 5 | Queue Integration | 🔲 |
| 6 | Preprocessing Layer | 🔲 |
| 7 | Regex Extraction | 🔲 |
| 8 | Context Resolution | 🔲 |
| 9 | LLM Extraction | 🔲 |
| 10 | Normalization Layer | 🔲 |
| 11 | Family Resolution | 🔲 |
| 12 | Merge Engine | 🔲 |
| 13 | Deduplication System | 🔲 |
| 14 | Processing Loop | 🔲 |
| 15 | Google Sheets Integration | 🔲 |
| 16 | Google Calendar Integration | 🔲 |
| 17 | Cloud Deployment | 🔲 |
| 18 | React Dashboard | 🔲 |
| 19 | Analytics Layer | 🔲 |

---

## Local Setup

### Prerequisites

- Python 3.12
- Node.js LTS (v20+)
- PostgreSQL 15+
- A WhatsApp account (for QR scan)
- A Google Cloud service account with Sheets + Calendar APIs enabled

### 1. Clone the repo
```powershell
git clone https://github.com/YOUR_USERNAME/placement-bot.git
cd placement-bot
```

### 2. Create and activate Python virtual environment
```powershell
py -3.12 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Install Node dependencies
```powershell
cd baileys
npm install
cd ..
```

### 4. Set up PostgreSQL
```sql
CREATE DATABASE placement_bot;
CREATE USER placement_user WITH ENCRYPTED PASSWORD 'yourpassword';
GRANT ALL PRIVILEGES ON DATABASE placement_bot TO placement_user;
```

### 5. Configure environment
```powershell
copy .env.example .env
```

Edit `.env` and fill in:
- `DATABASE_URL` — your PostgreSQL connection string
- `TARGET_GROUP_JID` — filled automatically after Phase 1 QR scan
- `GOOGLE_SHEET_ID` — from your Google Sheet URL
- `GOOGLE_CALENDAR_ID` — from Google Calendar settings

### 6. Add Google service account

Place your downloaded service account JSON at:
```
config/google_service_account.json
```

Share your Google Sheet and Calendar with the service account email.

### 7. Run
```powershell
python main.py
```

---

## Security Notes

- `.env` is gitignored — **never commit it**
- `config/google_service_account.json` is gitignored — **never commit it**
- `baileys/sessions/` is gitignored — contains WhatsApp auth tokens
- Baileys is configured **read-only** — the bot never sends WhatsApp messages

---

## License

MIT