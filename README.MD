# WhatsApp Placement Intelligence System

An end-to-end automated system that monitors a college placement WhatsApp group, extracts structured opportunity data from unstructured messages using NLP and LLMs, and delivers it through a cross-platform Flutter dashboard — running 24/7 on AWS with zero human intervention.

---

## The Problem

College placement groups on WhatsApp are chaotic. Dozens of messages arrive daily — some are job postings, some are replies, some are noise. Students miss deadlines, lose apply links, and have no way to search or filter past opportunities. There is no structure, no history, no analytics.

## The Solution

This system sits in the background, reads every message the moment it arrives, understands it, and instantly organizes it into a clean, searchable, filterable dashboard. Every opportunity is automatically added to Google Sheets and Google Calendar. Students open the app and see everything — sorted, deduplicated, and ready to act on.

---

## Key Features

- **Real-time processing** — every new WhatsApp message is processed and synced within seconds of arrival, no manual trigger
- **Intelligent extraction** — multi-layer pipeline: rule-based regex → context resolution → LLM fallback (Groq), used only when simpler methods fail
- **Smart deduplication** — 3-layer system prevents duplicate entries across message ID, content hash, and normalized URLs
- **Family grouping** — related messages (a posting + its replies + follow-up info) are grouped into a single opportunity record
- **Google Sheets sync** — every opportunity lands in a shared spreadsheet automatically
- **Google Calendar sync** — every deadline becomes a calendar event automatically
- **Cross-platform Flutter app** — one codebase ships as Android app, iOS app, and web dashboard
- **Fully automated on AWS** — runs 24/7 on EC2, survives reboots via systemd, no laptop required

---

## System Architecture

```
WhatsApp Group
    │
    ▼
Baileys Listener (Node.js)         ← read-only, runs on EC2
    │  POST each message instantly
    ▼
FastAPI Backend (Python)           ← runs on EC2
    │  immediately fires background processing
    ▼
Processing Pipeline
    ├── Preprocessor               ← clean, normalize, detect intent
    ├── Regex Extractor            ← deadline, package, links
    ├── Context Resolver           ← resolve reply chains, sliding window
    ├── LLM Extractor              ← Groq fallback (capped + cached)
    └── Normalizer                 ← company/role aliases, deduplication
    │
    ▼
PostgreSQL Database                ← Amazon RDS in production
    │
    ├──▶ Google Sheets             ← updated within seconds
    ├──▶ Google Calendar           ← deadline events created automatically
    └──▶ Flutter Dashboard         ← web (S3 + CloudFront) + Android + iOS
```

---

## Tech Stack

**Backend**
- Python 3.12, FastAPI, SQLAlchemy (async), Pydantic
- Node.js LTS, @whiskeysockets/baileys
- PostgreSQL 15
- Groq (LLM fallback, rate-capped, response-cached)
- Google Sheets API + Google Calendar API

**Infrastructure**
- Docker + Docker Compose (local and production)
- AWS EC2 t3.small (Ubuntu 22.04)
- AWS RDS db.t3.micro PostgreSQL 15
- AWS SQS (production message queue)
- AWS S3 + CloudFront (Flutter web hosting)
- systemd (auto-restart on reboot)

**Frontend**
- Flutter (single codebase → web + Android + iOS)
- Riverpod (state management)
- Dio (HTTP client)
- fl_chart (analytics charts)
- go_router (navigation)

---

## Intelligence Pipeline

Each message goes through a prioritized extraction pipeline designed to minimize LLM usage and cost:

1. **Preprocessor** — strips emoji, normalizes whitespace, extracts URLs, determines if the message is placement-related at all
2. **Regex extractor** — handles all known deadline and package formats (e.g. "25 March", "by EOD", "20 LPA", "₹50,000/month")
3. **Context resolver** — resolves partial messages by checking reply chains (confidence 0.95) and sliding message windows (confidence 0.60)
4. **LLM extractor** — only called when company or role cannot be determined by steps 1–3; results are cached with a 24-hour TTL; daily call limit enforced
5. **Normalizer** — maps aliases, deduplicates URLs, enforces merge rules so no field is ever overwritten with a null

---

## Flutter Dashboard

Three screens built from a single codebase, fully responsive across mobile, tablet, and desktop:

**Dashboard** — live count cards (total opportunities, deadlines this week, new today), sortable and filterable opportunity table, search by company or role

**Opportunity Detail** — full record with all contributing messages, apply link, deadline countdown

**Analytics** — bar chart of top hiring companies, line chart of opportunities over time, role distribution, salary distribution

---

## Automation Design

Once deployed to AWS, the system requires zero human action:

- Baileys runs 24/7 as a Docker container managed by systemd
- Every incoming WhatsApp message is immediately processed as an async background task
- Google Sheets and Calendar are updated within seconds of each new message
- A background scheduler runs every 2 hours as a safety net, retrying any messages that failed processing
- All processing is idempotent — running the same message twice produces identical final state
- The system survives EC2 reboots automatically via systemd

---

## Project Status

| Phase | Description | Status |
|---|---|---|
| 0 | Environment setup + Docker baseline | ✅ Complete |
| 1 | Baileys WhatsApp listener | ✅ Complete |
| 2–5 | FastAPI receiver + Database + Queue | ✅ Complete |
| 6–10 | Extraction pipeline (regex + context + LLM) | ✅ Complete |
| 11–13 | Family grouping + Merge + Deduplication | ✅ Complete |
| 14 | Real-time processing loop + scheduler | ✅ Complete |
| 15–16 | Google Sheets + Calendar integration | ✅ Complete |
| 17 | AWS deployment (EC2 + RDS + SQS) | ✅ Complete |
| 18–19 | Flutter app + Analytics | 🔄 Ongoing | 

---

## Author

Built by  [Rishi Mehta](www.linkedin.com/in/rishimehta1607) · 
