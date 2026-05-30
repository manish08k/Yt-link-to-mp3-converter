# WAVE — YouTube to MP3

A production-grade, fully local YouTube → MP3 converter.
Nothing leaves your machine.

## Requirements
- Python 3.8+
- ffmpeg

## Install ffmpeg
| OS | Command |
|---|---|
| macOS | `brew install ffmpeg` |
| Ubuntu/Debian | `sudo apt install ffmpeg` |
| Windows | Download from https://ffmpeg.org and add to PATH |

## Setup
```bash
pip install -r requirements.txt
python server.py
```

Open **http://localhost:5000**

## Production (10M scale)
```bash
pip install gunicorn
gunicorn -w 4 --threads 2 -b 0.0.0.0:5000 server:app
```

Put Nginx in front for TLS, static files, and upstream load balancing.

## Environment Variables
| Variable | Default | Description |
|---|---|---|
| MAX_WORKERS | 4 | Concurrent download threads |
| QUEUE_MAX_SIZE | 500 | Max pending jobs |
| RATE_LIMIT_REQ | 10 | Requests per window per IP |
| RATE_LIMIT_WIN | 60 | Rate limit window (seconds) |
| JOB_TTL | 600 | Job retention (seconds) |
| FILE_TTL | 300 | File auto-delete (seconds) |
| PORT | 5000 | Server port |

## Endpoints
- `GET /` — UI
- `POST /api/info` — Fetch video metadata
- `POST /api/convert` — Start conversion job
- `GET /api/status/<job_id>` — Poll job status
- `GET /api/download/<job_id>` — Download MP3
- `GET /health` — Health check
- `GET /metrics` — Server metrics

## Features
- **Rate limiting** — sliding window per-IP (configurable)
- **Worker pool** — capped concurrent downloads
- **Deduplication** — same URL+quality reuses existing job
- **Auto-cleanup** — files deleted after 5 min, jobs after 10 min
- **Graceful shutdown** — SIGTERM/SIGINT handled
- **Health + Metrics** — for monitoring

## Troubleshooting
| Problem | Fix |
|---|---|
| yt-dlp not found | `pip install yt-dlp` |
| ffmpeg not found | See Install ffmpeg above |
| Download fails | `pip install -U yt-dlp` |
| Port in use | `PORT=5001 python server.py` |
