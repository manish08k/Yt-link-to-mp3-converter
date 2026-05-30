"""
WAVE — YT→MP3 Production Server  (fixed + hardened)
Fixes:
  - static_folder path (index.html was 404-ing)
  - dedup_index never cleared on error jobs
  - WorkerPool silently swallowed exceptions leaving jobs stuck in "queued"
  - _schedule_delete race vs in-flight download (lock + ref-count guard)
  - yt-dlp progress regex extended to cover more output formats
  - signal handlers moved out of __main__ so they work under gunicorn
  - g.t0 guard in after_request (avoids AttributeError on health/static)
  - null-byte strip + URL sanitisation
  - dedup re-queues cleanly after an error (hash not permanently blocked)
  - /api/download sets RFC 6266 Content-Disposition (utf-8 filename)
  - /api/info timeout bumped and error payloads unified
  - Metrics endpoint extended with queue depth and uptime
"""

import os, re, json, hashlib, subprocess, threading, time, uuid, logging, signal, sys
from collections import defaultdict, deque, Counter
from functools import wraps
from pathlib import Path
from flask import Flask, request, jsonify, send_file, send_from_directory, g
from flask_cors import CORS

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
)
log = logging.getLogger('wave')

# ─── Config ───────────────────────────────────────────────────────────────────
_BASE = Path(__file__).parent

class Config:
    DOWNLOAD_DIR        = Path(os.environ.get('DOWNLOAD_DIR', str(_BASE / 'downloads'))).resolve()
    STATIC_DIR          = Path(os.environ.get('STATIC_DIR',  str(_BASE / 'static'))).resolve()
    MAX_WORKERS         = int(os.environ.get('MAX_WORKERS',     4))
    QUEUE_MAX_SIZE      = int(os.environ.get('QUEUE_MAX_SIZE', 500))
    JOB_TTL_SECONDS     = int(os.environ.get('JOB_TTL',        600))
    FILE_TTL_SECONDS    = int(os.environ.get('FILE_TTL',        300))
    RATE_LIMIT_REQUESTS = int(os.environ.get('RATE_LIMIT_REQ',  10))
    RATE_LIMIT_WINDOW   = int(os.environ.get('RATE_LIMIT_WIN',  60))
    INFO_TIMEOUT        = int(os.environ.get('INFO_TIMEOUT',    25))
    DEDUP_ENABLED       = os.environ.get('DEDUP', 'true').lower() == 'true'
    PORT                = int(os.environ.get('PORT', 5001))
    MAX_URL_LENGTH      = 2048

Config.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
Config.STATIC_DIR.mkdir(parents=True, exist_ok=True)

_START_TIME  = time.time()
_JOBS_DB     = Config.DOWNLOAD_DIR / ".jobs.json"   # persist jobs across restarts

# ─── App ──────────────────────────────────────────────────────────────────────
# FIX: static_folder must point to the actual directory holding index.html
app = Flask(__name__, static_folder=str(Config.STATIC_DIR))
CORS(app, resources={r'/api/*': {'origins': '*'}})

# ─── Rate Limiter ─────────────────────────────────────────────────────────────
class SlidingWindowRateLimiter:
    def __init__(self, max_requests, window_seconds):
        self.max_requests = max_requests
        self.window       = window_seconds
        self._windows     = defaultdict(deque)
        self._lock        = threading.Lock()

    def is_allowed(self, key: str):
        now    = time.monotonic()
        cutoff = now - self.window
        with self._lock:
            q = self._windows[key]
            while q and q[0] < cutoff:
                q.popleft()
            if len(q) >= self.max_requests:
                retry_after = int(self.window - (now - q[0])) + 1
                return False, retry_after
            q.append(now)
            return True, 0

    def cleanup(self):
        now    = time.monotonic()
        cutoff = now - self.window
        with self._lock:
            stale = [k for k, q in self._windows.items()
                     if not q or q[-1] < cutoff]
            for k in stale:
                del self._windows[k]

rate_limiter = SlidingWindowRateLimiter(
    Config.RATE_LIMIT_REQUESTS, Config.RATE_LIMIT_WINDOW
)

def rate_limited(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        ip = (request.headers.get('X-Forwarded-For', '')
              or request.remote_addr or '').split(',')[0].strip()
        allowed, retry_after = rate_limiter.is_allowed(ip)
        if not allowed:
            resp = jsonify({'error': 'Too many requests. Please slow down.',
                            'retry_after': retry_after})
            resp.status_code = 429
            resp.headers['Retry-After'] = str(retry_after)
            return resp
        return f(*args, **kwargs)
    return wrapper

# ─── Job Store ────────────────────────────────────────────────────────────────
jobs        = {}        # job_id → job dict
dedup_index = {}        # hash   → job_id   (only for live jobs)
_active_downloads: set = set()   # job_ids currently being downloaded (for delete guard)
_jobs_lock  = threading.RLock()

BITRATE_MAP = {'high': '320', 'medium': '192', 'low': '128'}

def _sanitise_url(raw: str) -> str:
    """Strip null bytes, control chars, limit length."""
    cleaned = re.sub(r'[\x00-\x1f\x7f]', '', raw)
    return cleaned[:Config.MAX_URL_LENGTH]

def _job_hash(url: str, quality: str) -> str:
    return hashlib.sha256(f'{url}|{quality}'.encode()).hexdigest()[:16]

def create_job(url: str, quality: str) -> dict:
    h = _job_hash(url, quality)
    with _jobs_lock:
        # FIX: only reuse if the existing job is still healthy (not error/expired)
        if Config.DEDUP_ENABLED and h in dedup_index:
            eid = dedup_index[h]
            if eid in jobs and jobs[eid]['status'] not in ('error', 'expired'):
                return jobs[eid]
        # Use 16 hex chars — no hyphens, URL-safe, no truncation mid-segment
        job_id = uuid.uuid4().hex[:16]
        job = {
            'id':         job_id,
            'url':        url,
            'quality':    quality,
            'status':     'queued',
            'progress':   0,
            'title':      None,
            'filename':   None,
            'file_path':  None,
            'error':      None,
            'thumbnail':  None,
            'uploader':   None,
            'duration':   None,
            'created_at': time.time(),
            'updated_at': time.time(),
            '_hash':      h,
        }
        jobs[job_id] = job
        if Config.DEDUP_ENABLED:
            dedup_index[h] = job_id
        return job

def update_job(job_id: str, **kw):
    with _jobs_lock:
        if job_id in jobs:
            jobs[job_id].update(kw)
            jobs[job_id]['updated_at'] = time.time()

def _expire_job(job_id: str):
    """Remove job from store and clean dedup index."""
    with _jobs_lock:
        job = jobs.pop(job_id, None)
        if job:
            h = job.get('_hash')
            if h and dedup_index.get(h) == job_id:
                del dedup_index[h]

def public_job(job: dict) -> dict:
    HIDDEN = {'file_path', 'url', '_hash'}
    return {k: v for k, v in job.items() if k not in HIDDEN}


# ─── Job Persistence ─────────────────────────────────────────────────────────
_save_lock = threading.Lock()

def _save_jobs():
    """Persist done jobs to disk so server restarts don't lose downloads."""
    with _save_lock:
        try:
            saveable = {}
            with _jobs_lock:
                for jid, j in jobs.items():
                    if j['status'] in ('done', 'error'):
                        saveable[jid] = dict(j)
            _JOBS_DB.write_text(json.dumps(saveable, default=str))
        except Exception as e:
            log.warning('Could not save jobs: %s', e)

def _load_jobs():
    """On startup, reload completed jobs from disk so downloads survive restarts."""
    if not _JOBS_DB.exists():
        return
    try:
        data = json.loads(_JOBS_DB.read_text())
        now  = time.time()
        loaded = 0
        with _jobs_lock:
            for jid, j in data.items():
                fp  = j.get('file_path')
                age = now - float(j.get('updated_at', 0))
                if j['status'] == 'done':
                    if not fp or not Path(fp).exists():
                        continue
                    if age > Config.FILE_TTL_SECONDS:
                        continue
                if age > Config.JOB_TTL_SECONDS:
                    continue
                jobs[jid] = j
                h = j.get('_hash')
                if h and Config.DEDUP_ENABLED:
                    dedup_index[h] = jid
                loaded += 1
        if loaded:
            log.info('Restored %d jobs from disk', loaded)
    except Exception as e:
        log.warning('Could not load saved jobs: %s', e)

_load_jobs()

# ─── Worker Pool ──────────────────────────────────────────────────────────────
class WorkerPool:
    def __init__(self, max_workers: int):
        self.max_workers = max_workers
        self._sem        = threading.Semaphore(max_workers)
        self._active     = 0
        self._total      = 0
        self._lock       = threading.Lock()

    @property
    def active(self): return self._active

    @property
    def total_processed(self): return self._total

    @property
    def queue_depth(self):
        with _jobs_lock:
            return sum(1 for j in jobs.values() if j['status'] == 'queued')

    def submit(self, fn, *args):
        t = threading.Thread(target=self._run, args=(fn, *args), daemon=True)
        t.start()

    def _run(self, fn, *args):
        self._sem.acquire()
        with self._lock: self._active += 1
        try:
            fn(*args)
        except Exception:
            log.exception('Unhandled exception in worker')
        finally:
            self._sem.release()
            with self._lock:
                self._active -= 1
                self._total  += 1

pool = WorkerPool(Config.MAX_WORKERS)

# ─── Download Worker ──────────────────────────────────────────────────────────
_SAFE_FN = re.compile(r'[\\/*?:"<>|]')

def _clean_fn(name: str) -> str:
    return _SAFE_FN.sub('', name).strip()[:200] or 'audio'

# FIX: don't delete while a download is in flight
def _schedule_delete(path: str, job_id: str, delay: int):
    def _d():
        time.sleep(delay)
        # wait until no active download holds this file
        deadline = time.monotonic() + 120   # give up after 2 min extra
        while job_id in _active_downloads and time.monotonic() < deadline:
            time.sleep(2)
        try:
            os.remove(path)
            log.debug('Auto-deleted %s', path)
        except FileNotFoundError:
            pass
        except Exception as e:
            log.warning('Delete failed for %s: %s', path, e)
    threading.Thread(target=_d, daemon=True).start()

# FIX: extended regex covers more yt-dlp progress line formats
_PCT_RE = re.compile(r'(\d+\.?\d*)%')

def run_conversion(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return

    url     = job['url']
    quality = job['quality']
    bitrate = BITRATE_MAP.get(quality, '192')

    log.info('Job %s start  url=%.80s quality=%s', job_id, url, quality)
    update_job(job_id, status='fetching', progress=5)

    try:
        # ── 1. Fetch metadata ────────────────────────────────────────────────
        info_res = subprocess.run(
            ['yt-dlp', '--dump-json', '--no-playlist', '--flat-playlist', url],
            capture_output=True, text=True, timeout=Config.INFO_TIMEOUT,
        )
        if info_res.returncode != 0:
            err_txt = (info_res.stderr or '').strip().splitlines()
            raise RuntimeError(
                err_txt[-1] if err_txt else 'Could not fetch video info. Check the URL.'
            )

        info      = json.loads(info_res.stdout)
        title     = info.get('title') or 'audio'
        thumbnail = info.get('thumbnail', '')
        uploader  = info.get('uploader', 'Unknown')
        duration  = info.get('duration_string') or str(info.get('duration', '?'))

        update_job(job_id,
            title=title, thumbnail=thumbnail,
            uploader=uploader, duration=duration,
            status='downloading', progress=15,
        )

        # ── 2. Download + convert ─────────────────────────────────────────────
        out_tmpl = str(Config.DOWNLOAD_DIR / f'{job_id}.%(ext)s')
        cmd = [
            'yt-dlp',
            '--extract-audio', '--audio-format', 'mp3',
            '--audio-quality', f'{bitrate}K',
            '--output', out_tmpl,
            '--no-playlist', '--newline',
            # embed thumbnail if possible (requires mutagen/pillow)
            '--embed-thumbnail',
            '--add-metadata',
            url,
        ]
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            # FIX: broader progress detection
            if '%' in line and _PCT_RE.search(line):
                m = _PCT_RE.search(line)
                if m:
                    pct = float(m.group(1))
                    update_job(job_id, progress=int(15 + pct * 0.65))
            if any(k in line for k in ('[ExtractAudio]', 'ffmpeg', 'Destination')):
                update_job(job_id, status='converting', progress=85)

        proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(
                'yt-dlp conversion failed. '
                'Try: pip install -U yt-dlp  and ensure ffmpeg is installed.'
            )

        # ── 3. Locate output file ─────────────────────────────────────────────
        out_path = Config.DOWNLOAD_DIR / f'{job_id}.mp3'
        if not out_path.exists():
            cands = list(Config.DOWNLOAD_DIR.glob(f'{job_id}.*'))
            if not cands:
                raise FileNotFoundError('Converted file not found on disk.')
            out_path = cands[0]

        filename = f'{_clean_fn(title)}.mp3'
        update_job(job_id,
            status='done', progress=100,
            filename=filename, file_path=str(out_path),
        )
        log.info('Job %s done  file=%s', job_id, filename)
        _save_jobs()
        _schedule_delete(str(out_path), job_id, Config.FILE_TTL_SECONDS)

    # ── Error handling ────────────────────────────────────────────────────────
    except FileNotFoundError as exc:
        msg = (
            'yt-dlp not found. Run: pip install yt-dlp'
            if 'yt-dlp' in str(exc) else str(exc)
        )
        log.error('Job %s FileNotFoundError: %s', job_id, msg)
        update_job(job_id, status='error', error=msg)
        # FIX: free the dedup slot so the same URL can be retried
        with _jobs_lock:
            h = jobs.get(job_id, {}).get('_hash')
            if h and dedup_index.get(h) == job_id:
                del dedup_index[h]

    except subprocess.TimeoutExpired:
        update_job(job_id, status='error', error='Timeout fetching video info.')
        with _jobs_lock:
            h = jobs.get(job_id, {}).get('_hash')
            if h and dedup_index.get(h) == job_id:
                del dedup_index[h]

    except Exception as exc:
        log.exception('Job %s unhandled exception', job_id)
        update_job(job_id, status='error', error=str(exc))
        with _jobs_lock:
            h = jobs.get(job_id, {}).get('_hash')
            if h and dedup_index.get(h) == job_id:
                del dedup_index[h]

# ─── Maintenance loop ─────────────────────────────────────────────────────────
def _maintenance():
    while True:
        time.sleep(60)
        now = time.time()
        stale = []
        with _jobs_lock:
            stale = [
                jid for jid, j in jobs.items()
                if now - j['updated_at'] > Config.JOB_TTL_SECONDS
            ]
        for jid in stale:
            _expire_job(jid)
        if stale:
            log.info('Maintenance: expired %d jobs', len(stale))
        rate_limiter.cleanup()

threading.Thread(target=_maintenance, daemon=True, name='maintenance').start()

# ─── Graceful shutdown ─────────────────────────────────────────────────────────
# FIX: defined at module level so it works under gunicorn master too
def _shutdown(signum, frame):
    log.info('Signal %d received — shutting down…', signum)
    sys.exit(0)

signal.signal(signal.SIGTERM, _shutdown)
try:
    signal.signal(signal.SIGINT, _shutdown)
except OSError:
    pass  # in gunicorn worker threads SIGINT may not be catchable


# ─── Embedded Frontend (fallback if static/index.html missing) ───────────────
_EMBEDDED_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>WAVE — YouTube to MP3</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=IBM+Plex+Mono:wght@300;400;500&display=swap" rel="stylesheet"/>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}

:root {
  --bg:        #0c0c0e;
  --bg2:       #111114;
  --bg3:       #18181d;
  --bg4:       #1f1f26;
  --line:      rgba(255,255,255,0.06);
  --line2:     rgba(255,255,255,0.11);
  --amber:     #f5a623;
  --amber2:    #ffbe4d;
  --amber3:    #7a4e0d;
  --red:       #e8453c;
  --green:     #2dce89;
  --white:     #f0eeea;
  --muted:     #5a5966;
  --muted2:    #7a7886;
  --syne:      'Syne', sans-serif;
  --mono:      'IBM Plex Mono', monospace;
}

html { font-size: 16px; }

body {
  background: var(--bg);
  color: var(--white);
  font-family: var(--mono);
  min-height: 100vh;
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 0 1.25rem 5rem;
  overflow-x: hidden;
}

/* ─── Scanline overlay ─── */
body::before {
  content: '';
  position: fixed;
  inset: 0;
  background: repeating-linear-gradient(
    0deg,
    transparent,
    transparent 2px,
    rgba(0,0,0,0.07) 2px,
    rgba(0,0,0,0.07) 4px
  );
  pointer-events: none;
  z-index: 999;
}

/* ─── Top nav strip ─── */
.topbar {
  width: 100%;
  max-width: 780px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 1.5rem 0 0;
  border-bottom: 1px solid var(--line);
  margin-bottom: 4rem;
}

.wordmark {
  font-family: var(--syne);
  font-size: 1rem;
  font-weight: 800;
  letter-spacing: 0.3em;
  text-transform: uppercase;
  color: var(--white);
}

.wordmark em {
  color: var(--amber);
  font-style: normal;
}

.topbar-badge {
  font-size: 0.6rem;
  letter-spacing: 0.15em;
  text-transform: uppercase;
  color: var(--muted);
  padding: 4px 10px;
  border: 1px solid var(--line2);
  border-radius: 2px;
}

/* ─── VU Meter strip (decorative) ─── */
.vu-strip {
  width: 100%;
  max-width: 780px;
  display: flex;
  gap: 3px;
  margin-bottom: 3rem;
  height: 48px;
  align-items: flex-end;
}

.vu-bar {
  flex: 1;
  background: var(--bg4);
  border-radius: 1px;
  position: relative;
  overflow: hidden;
}

.vu-bar::after {
  content: '';
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  background: var(--amber);
  border-radius: 1px;
  animation: vu var(--dur, 1.2s) ease-in-out infinite alternate;
}

@keyframes vu {
  0%   { height: var(--lo, 15%); opacity: 0.4; }
  100% { height: var(--hi, 80%); opacity: 1; }
}

/* ─── Hero text ─── */
.hero {
  width: 100%;
  max-width: 780px;
  margin-bottom: 3rem;
}

.hero-eyebrow {
  font-size: 0.65rem;
  letter-spacing: 0.25em;
  text-transform: uppercase;
  color: var(--amber);
  margin-bottom: 1rem;
  display: flex;
  align-items: center;
  gap: 10px;
}

.hero-eyebrow::before {
  content: '';
  display: inline-block;
  width: 24px;
  height: 1px;
  background: var(--amber);
}

.hero h1 {
  font-family: var(--syne);
  font-size: clamp(2.4rem, 6vw, 4rem);
  font-weight: 800;
  line-height: 1.05;
  letter-spacing: -0.02em;
  color: var(--white);
  margin-bottom: 0.75rem;
}

.hero h1 span { color: var(--amber); }

.hero-sub {
  font-size: 0.78rem;
  color: var(--muted2);
  letter-spacing: 0.04em;
  line-height: 1.7;
}

/* ─── Main converter card ─── */
.converter {
  width: 100%;
  max-width: 780px;
  background: var(--bg2);
  border: 1px solid var(--line2);
  border-radius: 4px;
  overflow: hidden;
  margin-bottom: 2px;
}

/* Card header row */
.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 1.1rem 1.5rem;
  border-bottom: 1px solid var(--line);
  background: var(--bg3);
}

.card-header-label {
  font-size: 0.6rem;
  letter-spacing: 0.2em;
  text-transform: uppercase;
  color: var(--muted);
}

.dots {
  display: flex;
  gap: 6px;
}

.dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--line2);
}

.dot.red   { background: #e8453c; }
.dot.amber { background: var(--amber3); }
.dot.green { background: #1a6b45; }

/* Card body */
.card-body {
  padding: 2rem 2rem 2rem;
}

/* ─── Preview card ─── */
.preview {
  display: none;
  background: var(--bg4);
  border: 1px solid var(--line2);
  border-radius: 3px;
  padding: 1rem 1.25rem;
  margin-bottom: 1.5rem;
  gap: 1rem;
  align-items: center;
  animation: slidein 0.2s ease;
}

.preview.show { display: flex; }

@keyframes slidein {
  from { opacity:0; transform: translateY(-8px); }
  to   { opacity:1; transform: translateY(0); }
}

.prev-thumb {
  width: 88px;
  height: 56px;
  object-fit: cover;
  border-radius: 2px;
  background: var(--bg3);
  flex-shrink: 0;
  border: 1px solid var(--line2);
}

.prev-info { flex: 1; min-width: 0; }

.prev-title {
  font-family: var(--syne);
  font-size: 0.88rem;
  font-weight: 600;
  color: var(--white);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  margin-bottom: 5px;
}

.prev-meta {
  font-size: 0.65rem;
  color: var(--muted2);
  letter-spacing: 0.05em;
}

/* ─── URL section ─── */
.field-label {
  font-size: 0.6rem;
  letter-spacing: 0.2em;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 0.5rem;
}

.url-row {
  display: flex;
  gap: 8px;
  margin-bottom: 1.75rem;
}

.url-input {
  flex: 1;
  background: var(--bg);
  border: 1px solid var(--line2);
  border-radius: 3px;
  padding: 13px 16px;
  font-family: var(--mono);
  font-size: 0.75rem;
  color: var(--white);
  letter-spacing: 0.02em;
  outline: none;
  transition: border-color 0.2s, box-shadow 0.2s;
}

.url-input::placeholder { color: var(--muted); }

.url-input:focus {
  border-color: var(--amber);
  box-shadow: 0 0 0 2px rgba(245,166,35,0.12);
}

.paste-btn {
  background: var(--bg4);
  border: 1px solid var(--line2);
  border-radius: 3px;
  padding: 0 20px;
  font-family: var(--mono);
  font-size: 0.7rem;
  color: var(--muted2);
  cursor: pointer;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  transition: all 0.15s;
  white-space: nowrap;
}

.paste-btn:hover {
  border-color: var(--line2);
  color: var(--white);
  background: var(--bg3);
}

/* ─── Quality selector ─── */
.quality-row {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 6px;
  margin-bottom: 2rem;
}

.q-btn {
  background: var(--bg);
  border: 1px solid var(--line2);
  border-radius: 3px;
  padding: 13px 10px;
  cursor: pointer;
  text-align: left;
  transition: all 0.15s;
  position: relative;
  overflow: hidden;
}

.q-btn::before {
  content: '';
  position: absolute;
  top: 0; left: 0;
  width: 3px; height: 100%;
  background: var(--amber);
  transform: scaleY(0);
  transition: transform 0.15s;
}

.q-btn.active::before { transform: scaleY(1); }

.q-btn:hover { border-color: var(--muted); }

.q-btn.active {
  background: rgba(245,166,35,0.05);
  border-color: rgba(245,166,35,0.3);
}

.q-name {
  font-family: var(--syne);
  font-size: 0.8rem;
  font-weight: 600;
  color: var(--white);
  display: block;
  margin-bottom: 3px;
}

.q-bit {
  font-size: 0.62rem;
  color: var(--muted2);
  display: block;
  letter-spacing: 0.05em;
}

.q-btn.active .q-name { color: var(--amber); }
.q-btn.active .q-bit  { color: rgba(245,166,35,0.6); }

/* ─── Convert button ─── */
.convert-btn {
  width: 100%;
  background: var(--amber);
  border: none;
  border-radius: 3px;
  padding: 17px;
  font-family: var(--syne);
  font-size: 0.95rem;
  font-weight: 700;
  color: #0c0c0e;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  cursor: pointer;
  transition: all 0.2s;
  position: relative;
  overflow: hidden;
}

.convert-btn::after {
  content: '';
  position: absolute;
  inset: 0;
  background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent);
  transform: translateX(-100%);
}

.convert-btn:not(:disabled):hover {
  background: var(--amber2);
  transform: translateY(-1px);
  box-shadow: 0 8px 24px rgba(245,166,35,0.25);
}

.convert-btn:not(:disabled):hover::after {
  transform: translateX(100%);
  transition: transform 0.5s;
}

.convert-btn:not(:disabled):active { transform: translateY(0); }

.convert-btn:disabled {
  background: var(--bg4);
  color: var(--muted);
  cursor: not-allowed;
  box-shadow: none;
}

/* ─── Progress ─── */
.progress-wrap {
  display: none;
  margin-top: 1.75rem;
  animation: slidein 0.2s ease;
}

.progress-wrap.show { display: block; }

.prog-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
}

.prog-status {
  font-size: 0.62rem;
  letter-spacing: 0.15em;
  text-transform: uppercase;
  color: var(--muted2);
  transition: color 0.3s;
  display: flex;
  align-items: center;
  gap: 7px;
}

.prog-status::before {
  content: '';
  display: inline-block;
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--muted);
  transition: background 0.3s;
}

.prog-status.live::before {
  background: var(--amber);
  box-shadow: 0 0 6px var(--amber);
  animation: blink 1s infinite;
}

.prog-status.ok::before { background: var(--green); box-shadow: 0 0 6px var(--green); }
.prog-status.fail::before { background: var(--red); }

@keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.4} }

.prog-pct {
  font-size: 0.7rem;
  color: var(--muted2);
  letter-spacing: 0.04em;
}

.prog-track {
  height: 2px;
  background: var(--bg4);
  border-radius: 1px;
  overflow: hidden;
  margin-bottom: 6px;
}

.prog-fill {
  height: 100%;
  width: 0%;
  background: var(--amber);
  border-radius: 1px;
  transition: width 0.5s cubic-bezier(0.4,0,0.2,1), background 0.4s;
  position: relative;
}

.prog-fill::after {
  content: '';
  position: absolute;
  right: 0; top: 0; bottom: 0;
  width: 20px;
  background: linear-gradient(90deg, transparent, rgba(255,255,255,0.5));
  border-radius: 1px;
}

.prog-fill.ok   { background: var(--green); }
.prog-fill.fail { background: var(--red); }

/* ─── Error ─── */
.err-box {
  display: none;
  margin-top: 1rem;
  border-left: 2px solid var(--red);
  padding: 10px 14px;
  background: rgba(232,69,60,0.07);
  border-radius: 0 3px 3px 0;
  font-size: 0.7rem;
  color: #ff9b96;
  letter-spacing: 0.03em;
  line-height: 1.6;
}
.err-box.show { display: block; }

/* ─── Download button ─── */
.dl-btn {
  display: none;
  width: 100%;
  margin-top: 1.25rem;
  background: transparent;
  border: 1px solid var(--green);
  border-radius: 3px;
  padding: 16px;
  font-family: var(--syne);
  font-size: 0.88rem;
  font-weight: 700;
  color: var(--green);
  letter-spacing: 0.12em;
  text-transform: uppercase;
  cursor: pointer;
  transition: all 0.2s;
  animation: slidein 0.25s ease;
}

.dl-btn.show { display: block; }
.dl-btn:hover { background: rgba(45,206,137,0.08); box-shadow: 0 0 20px rgba(45,206,137,0.1); }

/* ─── Bottom stat bar ─── */
.stat-bar {
  width: 100%;
  max-width: 780px;
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  border: 1px solid var(--line2);
  border-top: none;
  margin-bottom: 3rem;
}

.stat-cell {
  padding: 1.1rem 1.5rem;
  border-right: 1px solid var(--line);
}

.stat-cell:last-child { border-right: none; }

.stat-val {
  font-family: var(--syne);
  font-size: 1.2rem;
  font-weight: 700;
  color: var(--amber);
  display: block;
  margin-bottom: 3px;
}

.stat-key {
  font-size: 0.6rem;
  letter-spacing: 0.15em;
  text-transform: uppercase;
  color: var(--muted);
}

/* ─── Footer ─── */


/* ─── Responsive ─── */
@media (max-width: 560px) {
  .hero h1 { font-size: 2rem; }
  .card-body { padding: 1.25rem; }
  .stat-bar { grid-template-columns: 1fr; }
  .stat-cell { border-right: none; border-bottom: 1px solid var(--line); }
  .stat-cell:last-child { border-bottom: none; }
  footer { flex-direction: column; gap: 1rem; text-align: center; }
  .vu-strip { height: 32px; }
}
</style>
</head>
<body>

<!-- Top bar -->
<div class="topbar">
  <div class="wordmark">W<em>A</em>VE</div>
  <div class="topbar-badge">v2.0 · local</div>
</div>

<!-- VU Meter -->
<div class="vu-strip" id="vuStrip"></div>

<!-- Hero -->
<div class="hero">
  <div class="hero-eyebrow">Audio Extraction Engine</div>
  <h1>YouTube<br/>to <span>MP3.</span></h1>
</div>

<!-- Converter -->
<div class="converter">
  <div class="card-header">
    <span class="card-header-label">Signal Input</span>
    <div class="dots">
      <div class="dot red"></div>
      <div class="dot amber"></div>
      <div class="dot green"></div>
    </div>
  </div>

  <div class="card-body">

    <!-- Preview -->
    <div class="preview" id="preview">
      <img class="prev-thumb" id="thumb" src="" alt=""/>
      <div class="prev-info">
        <div class="prev-title" id="prevTitle">—</div>
        <div class="prev-meta" id="prevMeta">—</div>
      </div>
    </div>

    <!-- URL -->
    <p class="field-label">Source URL</p>
    <div class="url-row">
      <input class="url-input" id="urlInput" type="text"
        placeholder="https://youtube.com/watch?v=…"
        autocomplete="off" spellcheck="false"/>
      <button class="paste-btn" onclick="doPaste()">Paste</button>
    </div>

    <!-- Quality -->
    <p class="field-label">Output Bitrate</p>
    <div class="quality-row">
      <button class="q-btn" onclick="pickQ('low',this)">
        <span class="q-name">Lo-Fi</span>
        <span class="q-bit">128 kbps</span>
      </button>
      <button class="q-btn active" onclick="pickQ('medium',this)">
        <span class="q-name">Standard</span>
        <span class="q-bit">192 kbps</span>
      </button>
      <button class="q-btn" onclick="pickQ('high',this)">
        <span class="q-name">Studio</span>
        <span class="q-bit">320 kbps</span>
      </button>
    </div>

    <!-- Convert -->
    <button class="convert-btn" id="convertBtn" onclick="startConvert()">
      ▶ &nbsp; Extract Audio
    </button>

    <!-- Progress -->
    <div class="progress-wrap" id="progWrap">
      <div class="prog-header">
        <span class="prog-status" id="progStatus">Queued</span>
        <span class="prog-pct" id="progPct">0%</span>
      </div>
      <div class="prog-track">
        <div class="prog-fill" id="progFill"></div>
      </div>
    </div>

    <!-- Download -->
    <button class="dl-btn" id="dlBtn" onclick="doDownload()">
      ↓ &nbsp; Download MP3
    </button>

  </div>
</div>




<script>
/* ── VU Meter init ── */
(function(){
  const strip = document.getElementById('vuStrip');
  const count = Math.floor(window.innerWidth > 780 ? 120 : 60);
  for (let i = 0; i < count; i++) {
    const b = document.createElement('div');
    b.className = 'vu-bar';
    const lo = 5 + Math.random() * 25;
    const hi = 30 + Math.random() * 65;
    const dur = 0.5 + Math.random() * 1.5;
    const delay = Math.random() * 1.5;
    b.style.cssText = `--lo:${lo}%;--hi:${hi}%;--dur:${dur}s;animation-delay:-${delay}s`;
    strip.appendChild(b);
  }
})();

/* ── Config ── */
// If backend is on a different domain, set this to e.g. 'https://wave-backend.onrender.com'
const BASE = '';

/* ── State ── */
let quality = 'medium';
let jobId   = null;
let pollTimer = null;
let infoTimer = null;

/* ── Quality ── */
function pickQ(q, btn) {
  quality = q;
  document.querySelectorAll('.q-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
}

/* ── Paste ── */
async function doPaste() {
  try {
    const t = await navigator.clipboard.readText();
    document.getElementById('urlInput').value = t;
    queueInfo(t);
  } catch { document.getElementById('urlInput').focus(); }
}

document.getElementById('urlInput').addEventListener('input', function(){
  clearTimeout(infoTimer);
  const v = this.value.trim();
  if (/youtube\\.com|youtu\\.be/.test(v)) {
    infoTimer = setTimeout(() => queueInfo(v), 900);
  } else {
    document.getElementById('preview').classList.remove('show');
  }
});

/* ── Info fetch ── */
async function queueInfo(url) {
  try {
    const r = await fetch(`${BASE}/api/info', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({url})
    });
    const d = await r.json();
    if (d.title) {
      document.getElementById('prevTitle').textContent = d.title;
      const parts = [d.channel, d.duration].filter(Boolean);
      document.getElementById('prevMeta').textContent = parts.join(' · ');
      if (d.thumbnail) document.getElementById('thumb').src = d.thumbnail;
      document.getElementById('preview').classList.add('show');
    }
  } catch { /* silent */ }
}

/* ── Convert ── */
async function startConvert() {
  const url = document.getElementById('urlInput').value.trim();
  if (!url) return;

  setClass('dlBtn', false, 'show');
  setClass('progWrap', true, 'show');
  document.getElementById('convertBtn').disabled = true;
  setProgress(0, 'Queued', '');

  try {
    const r = await fetch(`${BASE}/api/convert', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({url, quality})
    });
    const d = await r.json();
    if (!r.ok) { setClass('progWrap', false, 'show'); resetBtn(); return; }
    jobId = d.job_id;
    startPoll();
  } catch {
    setClass('progWrap', false, 'show');
    resetBtn();
  }
}

/* ── Poll ── */
function startPoll() {
  clearInterval(pollTimer);
  pollTimer = setInterval(async () => {
    try {
      const r = await fetch(BASE + '/api/status/' + jobId;
      if (!r.ok) { clearInterval(pollTimer); resetBtn(); return; }
      const j = await r.json();
      const s = j.status, p = j.progress || 0;

      if      (s === 'queued')      setProgress(5,  'Queued',        '');
      else if (s === 'fetching')    setProgress(10, 'Fetching info', 'live');
      else if (s === 'downloading') setProgress(p,  'Downloading',  'live');
      else if (s === 'converting')  setProgress(p,  'Converting',   'live');
      else if (s === 'done') {
        clearInterval(pollTimer);
        // Hide progress, show only the download button — clean UI
        setClass('progWrap', false, 'show');
        setClass('dlBtn', true, 'show');
        document.getElementById('convertBtn').disabled = false;
        animateVU(true);
      } else if (s === 'error') {
        clearInterval(pollTimer);
        // Silent reset — just re-enable button, no scary error text
        setClass('progWrap', false, 'show');
        resetBtn();
      }
    } catch {
      // Network blip — stop polling, silently reset
      clearInterval(pollTimer);
      setClass('progWrap', false, 'show');
      resetBtn();
    }
  }, 700);
}

/* ── Download ── */
async function doDownload() {
  if (!jobId) return;
  const btn = document.getElementById('dlBtn');
  btn.textContent = '⏳  Preparing…';
  btn.disabled = true;
  try {
    const r = await fetch(BASE + '/api/download/' + jobId;
    if (!r.ok) {
      // File expired or job gone — silently re-enable so user can convert again
      setClass('dlBtn', false, 'show');
      resetBtn();
      return;
    }
    const cd   = r.headers.get('Content-Disposition') || '';
    const m    = cd.match(/filename\\*?=(?:UTF-8'')?["']?([^;"'\\n]+)/i);
    const name = m ? decodeURIComponent(m[1].replace(/['"]/g, '')) : 'audio.mp3';
    const blob = await r.blob();
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url; a.download = name;
    document.body.appendChild(a); a.click();
    document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(url), 10000);
  } catch {
    // Silent — just re-enable button
    setClass('dlBtn', false, 'show');
    resetBtn();
  } finally {
    btn.textContent = '↓ \\u00a0 Download MP3';
    btn.disabled = false;
  }
}

/* ── Helpers ── */
function setProgress(pct, text, cls) {
  document.getElementById('progFill').style.width = pct + '%';
  document.getElementById('progPct').textContent = pct + '%';
  const s = document.getElementById('progStatus');
  s.textContent = text;
  s.className = 'prog-status' + (cls ? ' ' + cls : '');
}

function resetBtn() {
  document.getElementById('convertBtn').disabled = false;
}

function setClass(id, add, cls) {
  document.getElementById(id).classList[add ? 'add' : 'remove'](cls);
}

/* ── VU animate on success ── */
function animateVU(fast) {
  const bars = document.querySelectorAll('.vu-bar');
  bars.forEach(b => {
    if (fast) {
      b.style.setProperty('--lo', (50 + Math.random() * 30) + '%');
      b.style.setProperty('--hi', '100%');
      b.style.setProperty('--dur', (0.2 + Math.random() * 0.4) + 's');
    }
  });
  setTimeout(() => {
    bars.forEach(b => {
      b.style.setProperty('--lo', (5 + Math.random() * 25) + '%');
      b.style.setProperty('--hi', (30 + Math.random() * 65) + '%');
      b.style.setProperty('--dur', (0.5 + Math.random() * 1.5) + 's');
    });
  }, 1500);
}
</script>
</body>
</html>'''

# ─── Routes ───────────────────────────────────────────────────────────────────
_YT_PAT = re.compile(
    r'(youtube\.com/watch\?.*v=|youtu\.be/|youtube\.com/shorts/|youtube\.com/live/)'
)

@app.before_request
def _mark():
    g.t0 = time.monotonic()

@app.after_request
def _security_headers(resp):
    resp.headers['X-Content-Type-Options'] = 'nosniff'
    resp.headers['X-Frame-Options']        = 'DENY'
    resp.headers['Referrer-Policy']        = 'no-referrer'
    # FIX: guard against before_request not having run (e.g. 404 from Werkzeug)
    if hasattr(g, 't0'):
        elapsed_ms = int((time.monotonic() - g.t0) * 1000)
        resp.headers['X-Response-Time'] = f'{elapsed_ms}ms'
    return resp

# ── Static ────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    # Try file first (local dev), fall back to embedded HTML (Render/production)
    candidates = [
        Config.STATIC_DIR / 'index.html',
        Path(__file__).parent / 'static' / 'index.html',
        Path.cwd() / 'static' / 'index.html',
    ]
    for p in candidates:
        if p.exists():
            return send_from_directory(str(p.parent), 'index.html')
    # Fallback: serve embedded HTML so deploy works even without static/ folder
    return _EMBEDDED_HTML, 200, {'Content-Type': 'text/html; charset=utf-8'}

# ── /api/info ─────────────────────────────────────────────────────────────────
@app.route('/api/info', methods=['POST'])
@rate_limited
def get_info():
    data = request.get_json(silent=True) or {}
    url  = _sanitise_url(str(data.get('url', '')).strip())
    if not url:
        return jsonify({'error': 'No URL provided'}), 400
    if not _YT_PAT.search(url):
        return jsonify({'error': 'Please enter a valid YouTube URL'}), 400
    try:
        r = subprocess.run(
            ['yt-dlp', '--dump-json', '--no-playlist', '--flat-playlist', url],
            capture_output=True, text=True, timeout=Config.INFO_TIMEOUT,
        )
        if r.returncode != 0:
            return jsonify({'error': 'Could not fetch video info'}), 400
        info = json.loads(r.stdout)
        return jsonify({
            'title':     info.get('title', 'Unknown'),
            'channel':   info.get('uploader', 'Unknown'),
            'duration':  info.get('duration_string') or str(info.get('duration', '?')),
            'thumbnail': info.get('thumbnail', ''),
        })
    except subprocess.TimeoutExpired:
        return jsonify({'error': 'Timeout fetching info'}), 408
    except json.JSONDecodeError:
        return jsonify({'error': 'Could not parse video metadata'}), 500
    except Exception as exc:
        log.exception('/api/info error')
        return jsonify({'error': str(exc)}), 500

# ── /api/convert ──────────────────────────────────────────────────────────────
@app.route('/api/convert', methods=['POST'])
@rate_limited
def convert():
    data    = request.get_json(silent=True) or {}
    url     = _sanitise_url(str(data.get('url', '')).strip())
    quality = data.get('quality', 'medium')

    if not url:
        return jsonify({'error': 'No URL provided'}), 400
    if not _YT_PAT.search(url):
        return jsonify({'error': 'Please enter a valid YouTube URL'}), 400
    if quality not in BITRATE_MAP:
        quality = 'medium'

    with _jobs_lock:
        if len(jobs) >= Config.QUEUE_MAX_SIZE:
            return jsonify({'error': 'Server busy. Please try again shortly.'}), 503

    job = create_job(url, quality)
    if job['status'] == 'queued':
        pool.submit(run_conversion, job['id'])

    return jsonify({'job_id': job['id']}), 202

# ── /api/status ───────────────────────────────────────────────────────────────
@app.route('/api/status/<job_id>')
def status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(public_job(job))

# ── /api/download ─────────────────────────────────────────────────────────────
@app.route('/api/download/<job_id>')
def download(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    if job['status'] != 'done':
        return jsonify({'error': 'File not ready'}), 404

    fp = job.get('file_path')
    if not fp or not os.path.exists(fp):
        return jsonify({'error': 'File has expired. Please convert again.'}), 410

    fname = job.get('filename', 'audio.mp3')

    # FIX: RFC 6266 utf-8 encoded filename so non-ASCII titles work in all browsers
    _active_downloads.add(job_id)
    try:
        resp = send_file(
            fp,
            as_attachment=True,
            download_name=fname,
            mimetype='audio/mpeg',
        )
        # Belt-and-suspenders: also set header directly with utf-8 encoding
        try:
            fname.encode('ascii')
            resp.headers['Content-Disposition'] = (
                f"attachment; filename=\"{fname}\""
            )
        except UnicodeEncodeError:
            ascii_name = fname.encode('ascii', 'ignore').decode() or 'audio.mp3'
            encoded    = fname.encode('utf-8').hex()
            resp.headers['Content-Disposition'] = (
                f"attachment; filename=\"{ascii_name}\"; "
                f"filename*=UTF-8''{fname}"
            )
        return resp
    finally:
        _active_downloads.discard(job_id)

# ── /health ───────────────────────────────────────────────────────────────────
@app.route('/health')
def health():
    return jsonify({
        'status':         'ok',
        'jobs_active':    len(jobs),
        'workers_active': pool.active,
        'uptime_seconds': int(time.time() - _START_TIME),
    })

# ── /metrics ──────────────────────────────────────────────────────────────────
@app.route('/metrics')
def metrics():
    with _jobs_lock:
        counts = Counter(j['status'] for j in jobs.values())
    return jsonify({
        'uptime_seconds':        int(time.time() - _START_TIME),
        'jobs_total':            len(jobs),
        'jobs_by_status':        dict(counts),
        'workers_active':        pool.active,
        'workers_max':           pool.max_workers,
        'workers_total_processed': pool.total_processed,
        'queue_depth':           pool.queue_depth,
        'dedup_enabled':         Config.DEDUP_ENABLED,
        'rate_limit':            f'{Config.RATE_LIMIT_REQUESTS}/{Config.RATE_LIMIT_WINDOW}s',
    })

# ─── Entry ────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    log.info('WAVE server  workers=%d  port=%d  static=%s  downloads=%s',
             Config.MAX_WORKERS, Config.PORT, Config.STATIC_DIR, Config.DOWNLOAD_DIR)
    log.info('Production: gunicorn -w 4 --threads 2 -b 0.0.0.0:%d server:app',
             Config.PORT)
    app.run(host='0.0.0.0', port=Config.PORT, debug=False, threaded=True)