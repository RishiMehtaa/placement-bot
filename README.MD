# PlacementBot — WhatsApp Placement Intelligence System

Your CGPA is fine. Your resume is ready. Your preparation is solid. And yet, the thing most likely to cost you a placement isn't any of that - it's a message you didn't see in time. One missed message is all it takes to lose an opportunity you'd been preparing for since years. Somewhere in your placement group right now, there's a message that matters. Maybe a deadline got moved up. Maybe a new JD just dropped. Are you sure you saw it?

**PlacementBot** has you covered. While you're in class, asleep, or just living your life, it's silently watching your college WhatsApp placement groups, extracting every deadline, every JD, every opportunity that gets posted, and making sure none of it slips through the cracks. No more manually sifting through hundreds of messages. No more missed deadline changes. No more "I didn't see it in time."

Access your placement data from anywhere - through a **Flutter mobile app** or a **web dashboard**, with real-time updates and built-in **analytics** that give you a clear picture of the current placement landscape in your college: which companies have visited, what roles are open, deadlines approaching, and more.

---

## ✨ Features

### Passive WhatsApp Monitoring
Silently listens to college placement WhatsApp groups in read-only mode — no messages sent, no interruptions, just continuous intelligence gathering in the background.

### WhatsApp Chat Import
Already have months of placement messages? Export your WhatsApp chat and import it directly into PlacementBot — no opportunity from the past gets left behind.

### 8-Stage Extraction Pipeline
Every message passes through a sophisticated pipeline — preprocessing, regex extraction, LLM-powered parsing, context resolution, normalization, family grouping, merge engine, and deduplication — to extract clean, structured placement data from noisy, unstructured chat.

### Message Family Grouping
Messages related to the same opportunity are automatically grouped into families — so if a deadline shifts, a role gets updated, or new details are added, PlacementBot captures the change and updates the record. Nothing gets missed, nothing gets duplicated.

### Per-Role Opportunity Splitting
When a company posts multiple roles in a single message, PlacementBot splits them into separate, individual opportunities — so each role gets its own deadline, eligibility check, and tracking entry. No role gets buried inside another.

### Smart Message Filtering
Not every message in a placement group is relevant. PlacementBot automatically identifies and skips unrelated messages — memes, casual conversation, announcements — so only meaningful placement data gets processed.

### Automatic Google Calendar Sync
Deadlines are automatically added to your Google Calendar the moment they're detected — and updated if they shift. No more manually noting dates. No more missed deadline changes.

### Google Sheets Integration
All placement data is synced to a live Google Sheet — company, role, stipend, deadline, JD link, eligibility — giving you a single source of truth for every opportunity.

### Eligibility Filtering
Every opportunity is automatically checked against your profile — branch, CGPA, batch, backlogs — so you only see the ones you're actually eligible for.

### Low Confidence Transparency
When the system isn't fully sure about an extraction, it flags it and shows you the contributing messages behind the decision — so you can verify, correct, or override with full context.

### Flutter Mobile App
Track all placement opportunities on the go with a clean, responsive mobile app — with real-time updates and push notifications for deadline changes.

### Web Dashboard
Access the same data from your browser — monitor opportunities, track deadlines, and stay on top of placement season from anywhere.

### Placement Analytics
Understand the bigger picture — which companies have visited, what roles are trending, how many opportunities are open, and how the season is progressing overall.

### Production-Grade AWS Infrastructure
Built on EC2, RDS PostgreSQL, and SQS — designed to run 24/7 without manual intervention, with Docker-based deployment for reliability and consistency.

---

## 🏗️ Architecture



---

## 🛠️ Tech Stack

| Technology | Role | Why |
|------------|------|-----|
| **Node.js + Baileys** | WhatsApp listener | Only mature unofficial WhatsApp Web client. Runs strictly read-only — `sock.sendMessage()` does not exist anywhere in the codebase |
| **Python 3.12 + FastAPI** | Backend API + processing engine | Async-first design handles real-time message ingestion without blocking. Pydantic ensures strict payload validation |
| **Groq (LLaMA 3.1 8B Instant)** | LLM extraction | Free tier with extremely fast inference. Used only as a fallback when regex and context resolution fail, with a hard cap of 200 calls/day |
| **PostgreSQL 15** | Primary database | Handles relational placement data with UUID primary keys, JSONB for raw payloads, and TIMESTAMPTZ for deadline handling |
| **Amazon EC2 (t3.small)** | Production compute | Full control over runtime environment and Docker networking. Chosen over managed platforms for real-world cloud engineering experience |
| **Amazon RDS (db.t3.micro)** | Managed production database | Automated backups, failover, and SSL — communicates with EC2 over AWS private networking, never exposed to the public internet |
| **Amazon SQS** | Managed message queue | Decouples ingestion from processing in production. Dead letter queue ensures no message is silently lost |
| **Caddy** | Reverse proxy | Automatic HTTPS with zero configuration. Proxies `placement-bot.duckdns.org` to FastAPI on port 8000 |
| **Docker + Docker Compose** | Containerisation | One command starts the entire stack. Same containers deploy locally and to EC2 without environment-specific code changes |
| **Google Sheets API** | Live opportunity tracker | Familiar, shareable view of all placement data — accessible to every student without any app install |
| **Google Calendar API** | Deadline sync + notifications | Every student already has Google Calendar on their phone — no separate notification system needed |
| **Flutter (Stable)** | Mobile app + web dashboard | Single codebase compiles to Android APK, iOS IPA, and web. Riverpod for state, Dio for API calls, fl_chart for analytics |
| **Netlify** | Flutter web hosting | Zero-config static hosting with automatic HTTPS and instant deploys from the Flutter web build output |
| **GitHub Actions** | CI/CD | On every push to `main`, SSHs into EC2, pulls latest code, and restarts the FastAPI container automatically |

---

## Try it out

| Platform | Link |
|----------|------|
| 🌐 Web Dashboard | [placement-bot.duckdns.org](https://placement-bot.duckdns.org) |
| 📱 Android APK | [Download latest release] |

### 🎥 Demo




---

## 🛠️ Running Locally (For Contributors)

#### 1. Clone the repository
```bash
git clone https://RishiMehtaa/placement_bot.git
cd placement_bot
```

#### 2. Set up environment variables
```bash
cp .env.example .env
# Fill in the required values in .env
```

#### 3. Add your Google Service Account key
Save your downloaded JSON key as:
config/google_service_account.json

#### 4. Start the backend
```bash
docker compose up --build
```

#### 5. Run Flutter locally
```bash
cd flutter_app
flutter pub get
flutter run -d chrome --web-port 8080
```
---

## 🔮 Future Scope

### 🏫 Multi-College Support
Currently built for a single college's placement group. The architecture can be extended to support multiple colleges, each with their own WhatsApp group, database namespace, and dashboard — making PlacementBot a platform, not just a personal tool.

### 🔔 Push Notifications
Deadline changes and new opportunities currently sync to Google Calendar. A dedicated push notification layer — via Firebase Cloud Messaging — would deliver instant alerts to the mobile app the moment a critical update is detected.

### 📤 Student-Side Applications
Allow students to apply directly to opportunities from within the app — auto-filling forms, tracking application status, and maintaining a personal placement history across companies and roles.

### 🤝 Placement Cell Dashboard
A separate admin-facing dashboard for placement coordinators — letting them post opportunities directly, track student applications, and monitor group message processing in real time.

### 📊 Advanced Analytics
Expand beyond basic placement tracking to deeper insights — offer acceptance rates, average packages by branch and batch, company visit frequency, and year-over-year placement trends.

### 🌐 Official WhatsApp Business API
Replace Baileys with the official WhatsApp Business API for a production-grade, ToS-compliant integration — enabling the system to scale without the risk of session bans or unofficial API breakage.

### 🧠 Smarter LLM Pipeline
Move from a single LLM call to a multi-agent pipeline — one agent for extraction, one for verification, one for eligibility scoring — improving accuracy on ambiguous and noisy placement messages.

### 📥 Resume Screening Integration
Cross-reference extracted job requirements against a student's uploaded resume — automatically flagging roles where the student is a strong match beyond just branch and CGPA filters.

---

## Author

Built by  [Rishi Mehta](www.linkedin.com/in/rishimehta1607) · 
