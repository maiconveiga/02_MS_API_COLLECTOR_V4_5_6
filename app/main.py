from fastapi import FastAPI, HTTPException, Query, Path as ApiPath, Body, Response
from pydantic import BaseModel, HttpUrl, Field, constr
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable, Awaitable, Tuple
from urllib.parse import urljoin
from contextlib import asynccontextmanager
from datetime import datetime
import aiosqlite, httpx, os, uuid, asyncio, math, random
from fastapi.middleware.cors import CORSMiddleware
import re, unicodedata

# ---------------------------------------------------------
# App + CORS
# ---------------------------------------------------------
app = FastAPI(title="API_COLLECTOR_V4_5_6", version="1.0.0-activities")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# ---------------------------------------------------------
# DB (usado para comments; collect não persiste)
# ---------------------------------------------------------
DB_DIR = Path("data")
DB_PATH = DB_DIR / "collector.db"

CREATE_ALARMS = """
CREATE TABLE IF NOT EXISTS alarms(
  id TEXT PRIMARY KEY,
  itemReference TEXT,
  name TEXT,
  message TEXT,
  isAckRequired INTEGER,
  type TEXT,
  priority INTEGER,
  triggerValue_value TEXT,
  triggerValue_units TEXT,
  creationTime TEXT,
  isAcknowledged INTEGER,
  isDiscarded INTEGER,
  category TEXT,
  objectUrl TEXT,
  annotationsUrl TEXT,
  source_base_url TEXT,
  inserted_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

# --- Esquema de comments V2: usa alarm_reference (sem FK) ---
CREATE_COMMENTS_V2 = """
CREATE TABLE IF NOT EXISTS comments(
  id TEXT PRIMARY KEY,
  alarm_reference TEXT NOT NULL,
  text TEXT NOT NULL CHECK(length(text)<=255),
  status TEXT,
  created_at TEXT NOT NULL
);
"""
CREATE_IDX_V2 = "CREATE INDEX IF NOT EXISTS idx_comments_alarm_reference ON comments(alarm_reference);"

# --- Esquema antigo (para detecção/migração) ---
CREATE_COMMENTS_OLD = """
CREATE TABLE IF NOT EXISTS comments(
  id TEXT PRIMARY KEY,
  alarm_id TEXT NOT NULL,
  text TEXT NOT NULL CHECK(length(text)<=255),
  status TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(alarm_id) REFERENCES alarms(id) ON DELETE CASCADE
);
"""
CREATE_IDX_OLD = "CREATE INDEX IF NOT EXISTS idx_comments_alarm_id ON comments(alarm_id);"

# ---------------------------------------------------------
# HTTP clients (pool + HTTP/2 + timeouts + retries)
# ---------------------------------------------------------
_clients: dict[bool, httpx.AsyncClient] = {}

def _build_client(verify_ssl: bool) -> httpx.AsyncClient:
  limits = httpx.Limits(
    max_connections=200,             # total
    max_keepalive_connections=50,    # ociosas reaproveitáveis
    keepalive_expiry=30.0,           # segundos
  )
  timeout = httpx.Timeout(
    connect=3.0,                     # rápido para cair em retry
    read=10.0,
    write=5.0,
    pool=3.0,
  )
  return httpx.AsyncClient(
    http2=True,                      # HTTP/2 para multiplexação
    verify=verify_ssl,
    follow_redirects=True,
    limits=limits,
    timeout=timeout,
    headers={
      "accept": "application/json",
      "accept-encoding": "gzip, deflate, br",
      "connection": "keep-alive",
      "user-agent": "api-collector-v4_5_6/1.0.0",
    },
  )

async def _table_has_column(db: aiosqlite.Connection, table: str, col: str) -> bool:
  cur = await db.execute(f"PRAGMA table_info({table})")
  rows = await cur.fetchall()
  await cur.close()
  return any(r[1] == col for r in rows)  # r[1] = column name

async def _migrate_comments_to_v2(db: aiosqlite.Connection):
  """Migra comments(alarm_id) -> comments(alarm_reference) sem FK."""
  await db.execute("""
      CREATE TABLE IF NOT EXISTS comments_v2(
        id TEXT PRIMARY KEY,
        alarm_reference TEXT NOT NULL,
        text TEXT NOT NULL CHECK(length(text)<=255),
        status TEXT,
        created_at TEXT NOT NULL
      );
  """)
  await db.execute("""
      INSERT INTO comments_v2 (id, alarm_reference, text, status, created_at)
      SELECT id, alarm_id, text, status, created_at
      FROM comments;
  """)
  await db.execute("DROP TABLE comments;")
  await db.execute("ALTER TABLE comments_v2 RENAME TO comments;")
  await db.execute(CREATE_IDX_V2)

@asynccontextmanager
async def lifespan(_: FastAPI):
  os.makedirs(DB_DIR, exist_ok=True)
  async with aiosqlite.connect(DB_PATH) as db:
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA foreign_keys=ON;")
    await db.execute("PRAGMA cache_size=-20000;")   # ~20MB cache
    await db.execute(CREATE_ALARMS)

    # Garante comments no formato V2
    await db.execute(CREATE_COMMENTS_V2)
    await db.execute(CREATE_IDX_V2)
    await db.commit()

    # Detecta se é o esquema antigo e migra
    try:
      has_alarm_id = await _table_has_column(db, "comments", "alarm_id")
      has_alarm_reference = await _table_has_column(db, "comments", "alarm_reference")
      if has_alarm_id and not has_alarm_reference:
        await _migrate_comments_to_v2(db)
        await db.commit()
    except Exception:
      await db.execute(CREATE_COMMENTS_V2)
      await db.execute(CREATE_IDX_V2)
      await db.commit()

  _clients[True]  = _build_client(True)
  _clients[False] = _build_client(False)
  try:
    yield
  finally:
    await asyncio.gather(*[c.aclose() for c in _clients.values() if not c.is_closed])

app.router.lifespan_context = lifespan

def _client(verify_ssl: bool) -> httpx.AsyncClient:
  return _clients[bool(verify_ssl)]

# ---------------------------------------------------------
# Schemas
# ---------------------------------------------------------
class LoginIn(BaseModel):
  base_url: HttpUrl
  usuario: str
  senha: str
  verify_ssl: bool = False

class APIInput(BaseModel):
  base_url: HttpUrl
  token: str
  pageSize: int = Field(100, ge=1, le=10000)
  page: int = Field(1, ge=1, le=10)
  offset: int = Field(0, ge=-12, le=12)
  verify_ssl: bool = False

class BatchRequest(BaseModel):
  apis: List[APIInput] = Field(..., min_items=1)

# >>> Comentários por `reference`
class CommentCreate(BaseModel):
  reference: str
  text: constr(min_length=1, max_length=255)
  status: str
  offset: int = Field(0, ge=-12, le=12)  # compat; ignorado

class CommentUpdate(BaseModel):
  text: Optional[constr(min_length=1, max_length=255)] = None
  status: Optional[str] = None

class CommentOut(BaseModel):
  id: str
  reference: str
  text: str
  status: Optional[str]
  created_at: str

# ---------------------------------------------------------
# Mapeamentos p/ exibição
# ---------------------------------------------------------
UNITS_MAP: Dict[Optional[str], str] = {
  None: "", "": "",
  "unitEnumSet.milligrams": "mg",
  "unitEnumSet.millimetersPerSecond": "mm/s",
  "unitEnumSet.degC": "°C",
  "unitEnumSet.degF": "°F",
  "unitEnumSet.kilopascals": "kPa",
  "unitEnumSet.kilowatts": "kW",
  "unitEnumSet.metersPerSecondPerSecond": "m/s²",
  "unitEnumSet.perHour": "/h",
  "unitEnumSet.perMinute": "/m",
  "unitEnumSet.percent": "%",
  "unitEnumSet.noUnits": "",
  "normalAlarm2EnumSet.na2Alarm": "Alarm",
  "sabControllerStatusEnumSet.sabCsOffline": "Offline",
  "sabControllerStatusEnumSet.sabCsOnline": "Online",
  "offAutoEnumSet.1": "1",
  "unitEnumSet.jaMetersWater":"mH₂O"
}

VALUE_MAP: Dict[str, str] = {
  "offonEnumSet.1": "On",
  "offonEnumSet.0": "Off",
  "yesNoEnumSet.1": "Yes",
  "yesNoEnumSet.0": "No",
  "noyesEnumSet.1": "No",
  "noyesEnumSet.0": "Yes",
  "falsetrueEnumSet.falsetrueTrue": "True",
  "falsetrueEnumSet.falsetrueFalse": "False",
  "controllerStatusEnumSet.csOnline": "Online",
  "controllerStatusEnumSet.csOffline": "Offline",
  "batteryConditionEnumSet.bcBatteryService": "Battery Service",
  "batteryConditionEnumSet.bcBatteryDefective": "Battery Defective",
  "batteryConditionEnumSet.bcBatteryGood": "Battery Good",
  "binarypvEnumSet.bacbinActive": "Active",
  "binarypvEnumSet.bacbinInactive": "Inactivate",
  "jciSystemStatusEnumSet.onboardUploadInProgress": "Uploading in progress",
  "jciSystemStatusEnumSet.jciOperational": "Operational",
  "localremoteEnumSet.1": "Remote",
  "localremoteEnumSet.0": "Local",
  "normalAlarmEnumSet.naAlarm": "Alarm",
  "normalAlarmEnumSet.naNormal": "Normal",
  "objectStatusEnumSet.osHighAlarm": "High Alarm",
  "objectStatusEnumSet.osLowAlarm": "Low Alarm",
  "normalAlarm2EnumSet.na2Normal": "Normal",
  "normalAlarm2EnumSet.na2Alarm": "Alarm",
  "sabControllerStatusEnumSet.sabCsOffline": "Offline",
  "sabControllerStatusEnumSet.sabCsOnline": "Online",
  "offAutoEnumSet.1": "1",
}

TYPE_MAP: Dict[str, str] = {
  "alarmValueEnumSet.avAlarm": "Alarm",
  "alarmValueEnumSet.avHiAlarm": "High Alarm",
  "alarmValueEnumSet.avLoAlarm": "Low Alarm",
  "alarmValueEnumSet.avNormal": "Normal",
  "alarmValueEnumSet.avOffline": "Offline",
  "alarmValueEnumSet.avOnline": "Online",
  "alarmValueEnumSet.avUnreliable": "Unreliable",
  "alarmValueEnumSet.avLoWarn": "Low Warn",
  "alarmValueEnumSet.avHiWarn": "High Warn",
  "alarmValueEnumSet.avFault": "Fault",
}

# ---------------------------------------------------------
# Utils (exibição, limpeza e etc.)
# ---------------------------------------------------------
_qstr = re.compile(r'^(?P<q>["\'])(?P<inner>.*)(?P=q)$')

def _clean_value(v: Any) -> str:
  if v is None:
    return ""
  if isinstance(v, (int, float)):
    return str(v)
  if isinstance(v, bool):
    return "True" if v else "False"
  s = str(v).strip()
  if s in VALUE_MAP:
    return VALUE_MAP[s]
  m = _qstr.match(s)
  if m:
    s = m.group("inner")
  return s

def _clean_units(u: Optional[str]) -> str:
  return UNITS_MAP.get(u, u or "")

def _map_type(t: Optional[str]) -> str:
  if not t:
    return ""
  return TYPE_MAP.get(t, t)

def _safe_json(r: httpx.Response):
  try:
    return r.json()
  except Exception:
    return {}

def _extract_items(external: Any) -> List[Dict[str, Any]]:
  """Extrai uma lista de itens de payloads paginados (alarms/activities) ou listas cruas.

  Suporta:
  - dict com chave 'items' (paginado Metasys v4+ / v6)
  - dict com chave 'data.items'
  - lista de dicts
  """
  if isinstance(external, list):
    return [x for x in external if isinstance(x, dict)]
  if not isinstance(external, dict):
    return []
  items = external.get('items')
  if isinstance(items, list):
    return [x for x in items if isinstance(x, dict)]
  data = external.get('data')
  if isinstance(data, dict) and isinstance(data.get('items'), list):
    return [x for x in data.get('items') if isinstance(x, dict)]
  return []

def _decorate_item_for_display(raw: Dict[str, Any]) -> Dict[str, Any]:
  """Normaliza um item para o formato esperado pelo front.

  - /alarms (legado): campos ja estao no nivel raiz
  - /activities (v6+): detalhes do alarme estao em raw['alarm']
  """
  item: Dict[str, Any] = dict(raw or {})
  alarm = item.get('alarm')
  if isinstance(alarm, dict):
    merged = dict(item)
    merged.update(alarm)  # message/priority/type/... (mantem itemReference/objectName/etc do activity)
    item = merged

  out = dict(item)

  # Nome exibido
  if not out.get('name'):
    out['name'] = out.get('objectName') or out.get('description') or out.get('itemReference') or ''

  # Trigger value (suporta schema v6)
  tv = out.get('triggerValue')
  v = ''
  u = ''
  if isinstance(tv, dict):
    if 'value' in tv or 'units' in tv:
      v = _clean_value(tv.get('value'))
      u = _clean_units(tv.get('units'))
    elif 'item' in tv or 'schema' in tv:
      v = _clean_value(tv.get('item'))
      schema = tv.get('schema') if isinstance(tv.get('schema'), dict) else {}
      units_obj = schema.get('units') if isinstance(schema.get('units'), dict) else None
      if isinstance(units_obj, dict):
        u = _clean_units(units_obj.get('title') or units_obj.get('id'))
      else:
        u = _clean_units(schema.get('units'))
    else:
      v = _clean_value(tv)
  else:
    # compat (ja decorado ou formato antigo)
    v = _clean_value(out.get('triggerValue_value'))
    u = _clean_units(out.get('triggerValue_units'))

  out['triggerValue_value'] = v
  out['triggerValue_units'] = u

  # Tipo (mapa p/ texto amigavel)
  t_raw = out.get('type')
  out['type_raw'] = t_raw
  out['type'] = _map_type(t_raw)

  # Acknowledged/Discarded: no /activities vem como activityManagementStatus
  if out.get('isAcknowledged') is None or out.get('isDiscarded') is None:
    st = out.get('activityManagementStatus')
    if isinstance(st, str):
      stl = st.lower()
      if out.get('isAcknowledged') is None:
        out['isAcknowledged'] = (stl == 'acknowledged')
      if out.get('isDiscarded') is None:
        out['isDiscarded'] = (stl == 'discarded')

  return out

def _decorate_list(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
  return [_decorate_item_for_display(x) for x in items]

def _now_local_str() -> str:
  return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

# Normalização/cheque de comentário automático
def _norm(s: str) -> str:
  s = (s or "").strip().lower()
  s = unicodedata.normalize("NFD", s)
  s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
  return s

def _is_auto_normalized(text: str) -> bool:
  t = _norm(text)
  return t == "condicao normalizada"

# ---------------------------------------------------------
# Retry helper (exponencial com jitter)
# ---------------------------------------------------------
_TRANSIENT_STATUSES = {429, 500, 502, 503, 504}

async def _with_retries(
  func: Callable[[], Awaitable[httpx.Response]],
  tries: int = 3,
  base_delay: float = 0.2,
  max_delay: float = 2.0,
) -> httpx.Response:
  last_exc: Optional[Exception] = None
  for attempt in range(1, tries + 1):
    try:
      resp = await func()
      if resp.status_code in _TRANSIENT_STATUSES:
        raise httpx.HTTPStatusError("transient", request=resp.request, response=resp)
      return resp
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
      last_exc = e
      if attempt >= tries:
        break
      # backoff exponencial com jitter
      delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
      delay = delay * (0.5 + random.random())  # jitter [0.5x, 1.5x]
      await asyncio.sleep(delay)
  assert last_exc is not None
  raise last_exc

# ---------------------------------------------------------
# Auth (pass-through) — agora com retry
# ---------------------------------------------------------
@app.post("/auth/login")
async def auth_login(payload: LoginIn):
  url = f"{str(payload.base_url).rstrip('/')}/login"
  body = {"username": payload.usuario, "password": payload.senha}
  headers = {"content-type": "application/json"}
  try:
    resp = await _with_retries(lambda: _client(payload.verify_ssl).post(url, json=body, headers=headers))
    resp.raise_for_status()
    return _safe_json(resp)
  except httpx.HTTPStatusError as e:
    raise HTTPException(status_code=e.response.status_code, detail={"upstream_error": _safe_json(e.response)})
  except httpx.RequestError as e:
    raise HTTPException(status_code=502, detail={"error": f"Falha ao conectar em {url}", "detail": str(e)})

# ---------------------------------------------------------
# Metasys v6: coletar alarmes via /activities (activityType=alarm)
# ---------------------------------------------------------
_MAX_ACTIVITIES_COUNT = 100  # exemplificado na documentacao do Metasys REST API v6
_MAX_ACTIVITIES_PAGES = 500  # protecao contra loops infinitos

async def _fetch_alarm_activities_chunk(
  base_url: str,
  token: str,
  verify_ssl: bool,
  count: int,
  next_url: Optional[str] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Optional[str]]:
  headers = {"authorization": f"Bearer {token}"}
  client = _client(verify_ssl)
  if next_url:
    url = next_url
    if isinstance(url, str) and url.startswith('/'):
      url = urljoin(base_url.rstrip('/') + '/', url.lstrip('/'))
    resp = await _with_retries(lambda: client.get(url, headers=headers))
  else:
    url = f"{base_url.rstrip('/')}/activities"
    params = {"activityType": "alarm", "count": count, "sort": "-creationTime"}
    resp = await _with_retries(lambda: client.get(url, headers=headers, params=params))
  resp.raise_for_status()
  external = _safe_json(resp)
  items = _extract_items(external)
  nxt = external.get('next') if isinstance(external, dict) else None
  return external, items, nxt

async def _collect_alarm_activities(
  base_url: str,
  token: str,
  verify_ssl: bool,
  page_size: int,
  page: int,
) -> Tuple[Optional[int], List[Dict[str, Any]]]:
  """Emula page/pageSize consumindo a paginacao do /activities (next/continuationToken).

  O Metasys v6 usa um link `next` com continuationToken (e suporta `count` por requisicao).
  """
  offset = max(0, (page - 1) * page_size)
  target = offset + page_size
  per_req = max(1, min(_MAX_ACTIVITIES_COUNT, page_size))
  collected: List[Dict[str, Any]] = []
  total: Optional[int] = None
  next_url: Optional[str] = None
  pages = 0
  while len(collected) < target and pages < _MAX_ACTIVITIES_PAGES:
    external, items, next_url = await _fetch_alarm_activities_chunk(
      base_url=base_url, token=token, verify_ssl=verify_ssl, count=per_req, next_url=next_url
    )
    if total is None and isinstance(external, dict) and isinstance(external.get('total'), int):
      total = external.get('total')
    if not items:
      break
    collected.extend(items)
    pages += 1
    if not next_url:
      break
  # recorta a pagina solicitada
  return total, collected[offset:offset + page_size]

# ---------------------------------------------------------
# Collect (GET - NÃO persiste; passa cru + campos tratados p/ UI) — com retry
# ---------------------------------------------------------
@app.get("/collect/alarms")
async def get_alarms(
  base_url: HttpUrl,
  token: str,
  pageSize: int = Query(100, ge=1, le=10000),
  page: int = Query(1, ge=1, le=10),
  offset: int = Query(0, ge=-12, le=12),
  verify_ssl: bool = False,
): 
  base = str(base_url).rstrip('/')
  try:
    total, items_raw = await _collect_alarm_activities(base_url=base, token=token, verify_ssl=verify_ssl, page_size=pageSize, page=page)
  except httpx.HTTPStatusError as e:
    # compat: se /activities nao existir, tenta /alarms (versoes antigas)
    if e.response is not None and e.response.status_code in (404, 501):
      url = f"{base}/alarms"
      headers = {"authorization": f"Bearer {token}"}
      params = {"pageSize": pageSize, "page": page}
      try:
        resp = await _with_retries(lambda: _client(verify_ssl).get(url, headers=headers, params=params))
        resp.raise_for_status()
        external = _safe_json(resp)
        items_raw = _extract_items(external)
        total = external.get('total') if isinstance(external, dict) else None
      except httpx.HTTPStatusError as e2:
        raise HTTPException(status_code=e2.response.status_code, detail={"upstream_error": _safe_json(e2.response)})
      except httpx.RequestError as e2:
        raise HTTPException(status_code=502, detail={"error": f"Falha ao conectar em {url}", "detail": str(e2)})
    else:
      raise HTTPException(status_code=e.response.status_code, detail={"upstream_error": _safe_json(e.response)})
  except httpx.RequestError as e:
    raise HTTPException(status_code=502, detail={"error": f"Falha ao conectar em {base}", "detail": str(e)})

  items_ui = _decorate_list(items_raw)

  return {
    "page": page,
    "pageSize": pageSize,
    "offsetAppliedHours": offset,
    "total": total,
    "count_items": len(items_raw),
    "items": items_ui,
  }

# ---------------------------------------------------------
# Collect (POST - batch; NÃO persiste) — paralelo com limite + retry
# ---------------------------------------------------------
async def _fetch_one(api: APIInput) -> Dict[str, Any]:
  base = str(api.base_url).rstrip('/')
  try:
    total, items_raw = await _collect_alarm_activities(base_url=base, token=api.token, verify_ssl=api.verify_ssl, page_size=api.pageSize, page=api.page)
  except httpx.HTTPStatusError as e:
    # compat: se /activities nao existir, tenta /alarms
    if e.response is not None and e.response.status_code in (404, 501):
      url = f"{base}/alarms"
      headers = {"authorization": f"Bearer {api.token}"}
      params = {"pageSize": api.pageSize, "page": api.page}
      try:
        resp = await _with_retries(lambda: _client(api.verify_ssl).get(url, headers=headers, params=params))
        resp.raise_for_status()
        external = _safe_json(resp)
        items_raw = _extract_items(external)
        total = external.get('total') if isinstance(external, dict) else None
      except httpx.HTTPStatusError as e2:
        return {"ok": False, "base_url": str(api.base_url), "error": {"status_code": e2.response.status_code, "upstream_error": _safe_json(e2.response)}}
      except httpx.RequestError as e2:
        return {"ok": False, "base_url": str(api.base_url), "error": {"status_code": 502, "message": f"Falha ao conectar em {url}", "detail": str(e2)}}
    else:
      return {"ok": False, "base_url": str(api.base_url), "error": {"status_code": e.response.status_code, "upstream_error": _safe_json(e.response)}}
  except httpx.RequestError as e:
    return {"ok": False, "base_url": str(api.base_url), "error": {"status_code": 502, "message": f"Falha ao conectar em {base}", "detail": str(e)}}

  items_ui  = _decorate_list(items_raw)

  return {
    "ok": True,
    "base_url": str(api.base_url),
    "page": api.page,
    "pageSize": api.pageSize,
    "offsetAppliedHours": api.offset,
    "total": total,
    "count_items": len(items_raw),
    "items": items_ui,
  }

async def _bounded_gather(coros: List[Callable[[], Awaitable[Dict[str, Any]]]], concurrency: int) -> List[Dict[str, Any]]:
  sem = asyncio.Semaphore(max(1, concurrency))
  async def runner(fn: Callable[[], Awaitable[Dict[str, Any]]]):
    async with sem:
      return await fn()
  return await asyncio.gather(*(runner(c) for c in coros))

@app.post("/collect/alarms")
async def collect_alarms_list(
  payload: BatchRequest = Body(...),
  concurrency: int = Query(20, ge=1, le=200),
):
  # Executa N chamadas em paralelo, respeitando limite
  callables: List[Callable[[], Awaitable[Dict[str, Any]]]] = [lambda api=api: _fetch_one(api) for api in payload.apis]
  results = await _bounded_gather(callables, concurrency)

  ok   = [r for r in results if r.get("ok")]
  fail = [r for r in results if not r.get("ok")]

  total_items = sum(r["count_items"] for r in ok)
  flat: List[Dict[str, Any]] = [it for r in ok for it in r["items"]]

  return {
    "total_apis": len(payload.apis),
    "succeeded": len(ok),
    "failed": len(fail),
    "total_items": total_items,
    "errors": fail,
    "by_api": [
      {
        "base_url": r["base_url"],
        "page": r["page"],
        "pageSize": r["pageSize"],
        "offsetAppliedHours": r["offsetAppliedHours"],
        "count_items": r["count_items"],
        "items": r["items"],
      }
      for r in ok
    ],
    "items": flat,
  }

# ---------------------------------------------------------
# Comments (V2 usando alarm_reference) — idempotência mantida
# ---------------------------------------------------------
class _CommentSQL:
  ins = "INSERT INTO comments (id, alarm_reference, text, status, created_at) VALUES (?,?,?,?,?)"
  by_id = "SELECT id, alarm_reference, text, status, created_at FROM comments WHERE id=?"
  list = """
  SELECT id, alarm_reference, text, status, created_at
  FROM comments
  WHERE (? IS NULL OR alarm_reference = ?)
  ORDER BY created_at DESC
  LIMIT ? OFFSET ?
  """
  last_by_ref = """
  SELECT id, alarm_reference, text, status, created_at
  FROM comments
  WHERE alarm_reference = ?
  ORDER BY created_at DESC
  LIMIT 1
  """
  upd = "UPDATE comments SET text=?, status=? WHERE id=?"
  del_ = "DELETE FROM comments WHERE id=?"

@app.post("/comments", response_model=CommentOut)
async def create_comment(payload: CommentCreate, response: Response):
  cid = str(uuid.uuid4())
  created = _now_local_str()
  async with aiosqlite.connect(DB_PATH, isolation_level=None) as db:
    await db.execute("PRAGMA foreign_keys=ON;")
    cur = await db.execute(_CommentSQL.last_by_ref, (payload.reference,))
    last = await cur.fetchone()
    await cur.close()

    if last:
      last_txt = last[2] or ""
      if _is_auto_normalized(payload.text) and _is_auto_normalized(last_txt):
        return CommentOut(id=last[0], reference=last[1], text=last[2], status=last[3], created_at=last[4])

    try:
      await db.execute(_CommentSQL.ins, (cid, payload.reference, payload.text, payload.status, created))
    except aiosqlite.IntegrityError:
      raise HTTPException(status_code=400, detail="Falha ao salvar comentário")

  response.status_code = 201
  return CommentOut(id=cid, reference=payload.reference, text=payload.text, status=payload.status, created_at=created)

@app.get("/comments", response_model=List[CommentOut])
async def list_comments(
  reference: Optional[str] = None,
  pageSize: int = Query(50, ge=1, le=1000),
  page: int = Query(1, ge=1)
):
  async with aiosqlite.connect(DB_PATH) as db:
    cur = await db.execute(_CommentSQL.list, (reference, reference, pageSize, (page-1)*pageSize))
    rows = await cur.fetchall(); await cur.close()
  return [CommentOut(id=r[0], reference=r[1], text=r[2], status=r[3], created_at=r[4]) for r in rows]

@app.get("/comments/{comment_id}", response_model=CommentOut)
async def get_comment(comment_id: str = ApiPath(...)):
  async with aiosqlite.connect(DB_PATH) as db:
    cur = await db.execute(_CommentSQL.by_id, (comment_id,))
    row = await cur.fetchone(); await cur.close()
  if not row:
    raise HTTPException(status_code=404, detail="Comentário não encontrado")
  return CommentOut(id=row[0], reference=row[1], text=row[2], status=row[3], created_at=row[4])

@app.patch("/comments/{comment_id}", response_model=CommentOut)
async def update_comment(comment_id: str, payload: CommentUpdate):
  async with aiosqlite.connect(DB_PATH, isolation_level=None) as db:
    cur = await db.execute(_CommentSQL.by_id, (comment_id,))
    row = await cur.fetchone()
    if not row:
      await cur.close()
      raise HTTPException(status_code=404, detail="Comentário não encontrado")
    new_text = payload.text if payload.text is not None else row[2]
    new_status = payload.status if payload.status is not None else row[3]
    await db.execute(_CommentSQL.upd, (new_text, new_status, comment_id))
    await cur.close()
  return CommentOut(id=comment_id, reference=row[1], text=new_text, status=new_status, created_at=row[4])

@app.delete("/comments/{comment_id}")
async def delete_comment(comment_id: str = ApiPath(...)):
  async with aiosqlite.connect(DB_PATH, isolation_level=None) as db:
    cur = await db.execute(_CommentSQL.del_, (comment_id,))
    deleted = cur.rowcount; await cur.close()
  if not deleted:
    raise HTTPException(status_code=404, detail="Comentário não encontrado")
  return {"deleted": True, "id": comment_id}

# ---------------------------------------------------------
# DB utilities
# ---------------------------------------------------------
@app.delete("/db/alarms/clear")
async def clear_alarms(vacuum: bool = Query(False)):
  async with aiosqlite.connect(DB_PATH, isolation_level=None) as db:
    await db.execute("PRAGMA foreign_keys=ON;")
    cur = await db.execute("SELECT COUNT(*) FROM alarms"); (n,) = await cur.fetchone(); await cur.close()
    await db.execute("DELETE FROM alarms")
    if vacuum:
      await db.execute("VACUUM")
  return {"table": "alarms", "deleted": n, "vacuum": vacuum}

@app.delete("/db/comments/clear")
async def clear_comments(vacuum: bool = Query(False)):
  async with aiosqlite.connect(DB_PATH, isolation_level=None) as db:
    await db.execute("PRAGMA foreign_keys=ON;")
    cur = await db.execute("SELECT COUNT(*) FROM comments"); (n,) = await cur.fetchone(); await cur.close()
    await db.execute("DELETE FROM comments")
    if vacuum:
      await db.execute("VACUUM")
  return {"table": "comments", "deleted": n, "vacuum": vacuum}

@app.delete("/db/alarms/without-comments")
async def delete_alarms_without_comments(dry_run: bool = Query(False)):
  """
  Remove alarmes sem comentários (associação por itemReference ↔ alarm_reference).
  """
  async with aiosqlite.connect(DB_PATH, isolation_level=None) as db:
    await db.execute("PRAGMA foreign_keys=ON;")
    cur = await db.execute("""
      SELECT COUNT(*) FROM alarms a
      WHERE NOT EXISTS (
        SELECT 1 FROM comments c WHERE c.alarm_reference = a.itemReference
      )
    """)
    (to_delete,) = await cur.fetchone(); await cur.close()

    if dry_run:
      return {"dry_run": True, "would_delete": to_delete}

    await db.execute("""
      DELETE FROM alarms
      WHERE id IN (
        SELECT a.id FROM alarms a
        LEFT JOIN comments c ON c.alarm_reference = a.itemReference
        WHERE c.alarm_reference IS NULL
      )
    """)
  return {"dry_run": False, "deleted": to_delete}
