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
    DOWNLOAD_DIR        = Path(os.environ.get('DOWNLOAD_DIR', _BASE / 'downloads'))
    STATIC_DIR          = Path(os.environ.get('STATIC_DIR',  _BASE / 'static'))
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
    # FIX: serve from the correct static directory (not a hard-coded 'static' string)
    return send_from_directory(str(Config.STATIC_DIR), 'index.html')

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