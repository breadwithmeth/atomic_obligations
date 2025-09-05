#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
med-audit API — FastAPI + Ollama + SQLite (ускоренная)
- Быстрее извлекает текст из PDF (PyMuPDF вместо pdfplumber)
- Параллелит LLM-проверки по разделам (ThreadPool)
- Держит модель в памяти (keep_alive) и HTTP keep-alive (httpx)
- Кеширует ответы LLM по хэшу секции (SQLite)
- Оптимизирует SQLite (WAL, synchronous=NORMAL)

Установка:
  python -m pip install fastapi uvicorn sqlalchemy httpx python-docx pymupdf openpyxl pydantic-settings
  # опционально: pdfplumber больше не обязателен
  export OLLAMA_BASE_URL=http://localhost:11434
  uvicorn app:app --host 0.0.0.0 --port 8000 --reload

Переменные окружения:
  AUDIT_CONCURRENCY=4       # параллельные запросы к Ollama
  MAX_SECTION_CHARS=8000    # обрезка текста секции
  OLLAMA_KEEP_ALIVE=5m      # сколько держать модель в памяти на стороне Ollama
"""

from __future__ import annotations
import enum, json, os, re, uuid, datetime, logging, hashlib
from typing import Dict, Any, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy import (
    create_engine, Column, String, DateTime, Text, Enum as SAEnum, Boolean, event
)
from sqlalchemy.orm import sessionmaker, declarative_base

# ----------------------- Settings -----------------------
class Settings(BaseSettings):
    DATABASE_URL: str = "sqlite:///./med_audit.db"
    STORAGE_DIR: str = "./storage"
    OLLAMA_BASE_URL: str = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
    DEFAULT_MODEL: str = "llama3.1:8b"
    MAX_SECTION_CHARS: int = int(os.environ.get("MAX_SECTION_CHARS", 8000))
    AUDIT_CONCURRENCY: int = int(os.environ.get("AUDIT_CONCURRENCY", 4))
    OLLAMA_KEEP_ALIVE: str = os.environ.get("OLLAMA_KEEP_ALIVE", "5m")

settings = Settings()
os.makedirs(settings.STORAGE_DIR, exist_ok=True)

# ----------------------- DB ----------------------------
Base = declarative_base()

class AuditStatus(str, enum.Enum):
    queued = "QUEUED"
    running = "RUNNING"
    succeeded = "SUCCEEDED"
    failed = "FAILED"

class Audit(Base):
    __tablename__ = "audits"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    status = Column(SAEnum(AuditStatus), default=AuditStatus.queued, nullable=False)
    model = Column(String, default=settings.DEFAULT_MODEL, nullable=False)

    input_name = Column(String)
    input_mime = Column(String)
    input_path = Column(String)

    output_json_path = Column(String)
    output_xlsx_path = Column(String)
    annotated_path = Column(String)

    summary_json = Column(Text)
    sections_json = Column(Text)
    deterministic_issues_json = Column(Text)
    llm_by_section_json = Column(Text)
    issues_flat_json = Column(Text)

    error = Column(Text)

    created_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)

    want_xlsx = Column(Boolean, default=False)
    want_annotate = Column(Boolean, default=False)

class LLMCache(Base):
    __tablename__ = "llm_cache"
    key = Column(String, primary_key=True)  # sha256(model+section+text)
    value = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

engine = create_engine(
    settings.DATABASE_URL,
    future=True,
    connect_args={"check_same_thread": False} if settings.DATABASE_URL.startswith("sqlite") else {},
)

@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    try:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        cursor.execute("PRAGMA temp_store=MEMORY;")
        cursor.execute("PRAGMA mmap_size=134217728;")
        cursor.close()
    except Exception:
        pass

Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

# ----------------------- App init ----------------------
app = FastAPI(title="med-audit API (fast)", version="1.2")
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=True, allow_methods=['*'], allow_headers=['*'])
log = logging.getLogger("med-audit")
logging.basicConfig(level=logging.INFO)

# HTTP client с keep-alive и HTTP/2
HTTP = httpx.Client(base_url=settings.OLLAMA_BASE_URL, timeout=300, http2=True)

# ----------------------- Utils: read & normalize --------
def read_text(path: str) -> str:
    ext = os.path.splitext(path.lower())[1]
    if ext == ".docx":
        from docx import Document
        doc = Document(path)
        return "\n".join(p.text for p in doc.paragraphs)
    elif ext == ".pdf":
        import fitz  # PyMuPDF — быстрее, чем pdfplumber/pdfminer
        doc = fitz.open(path)
        text = []
        for page in doc:
            text.append(page.get_text("text") or "")
        doc.close()
        return "\n".join(text)
    else:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()

def norm_text(s: str) -> str:
    s = s.replace("\u00a0", " ")  # NBSP
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

# ----------------------- Split to sections --------------
SECTION_PATTERNS: List[Tuple[str,str]] = [
    ("Титул/шапка", r"(?:^|\n)(?:Медицинская документация|СТАЦИОНАРЛЫҚ|ФОРМА\s*№\s*001|ИИН|Фамилия)"),
    ("Триаж", r"(?:^|\n)Триаж"),
    ("Осмотр приёмного покоя", r"(?:^|\n)Осмотр врача приемного покоя"),
    ("Первичный осмотр", r"(?:^|\n)Первичный\s+осмотр"),
    ("Сестринский осмотр", r"(?:^|\n)Первичный\s+сестринский\s+осмотр"),
    ("Лист назначений", r"(?:^|\n)Лист(?:\s+врачебных)?\s+назначений"),
    ("Назначения на исследования", r"(?:^|\n)Лист\s+назначений\s+на\s+исследование|ЗЕРТТЕУГЕ ТАҒАЙЫНДАУЛАР ПАРАҒЫ"),
    ("Температурный лист / показатели", r"(?:^|\n)ТЕМПЕРАТУРНЫЙ ЛИСТ|ПОКАЗАТЕЛИ ЗДОРОВЬЯ"),
    ("Результаты исследований", r"(?:^|\n)Результаты исследований"),
    ("Обоснование диагноза/эпикриз", r"(?:^|\n)(Обоснование диагноза|Выписн|Эпикриз)"),
]

def split_sections(text: str) -> Dict[str, str]:
    marks: List[Tuple[int, str]] = []
    for name, pat in SECTION_PATTERNS:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            marks.append((m.start(), name))
    marks.sort()
    if not marks:
        return {"Документ целиком": text}

    sections: Dict[str,str] = {}
    for i, (start, name) in enumerate(marks):
        end = marks[i+1][0] if i+1 < len(marks) else len(text)
        chunk = text[start:end].strip()
        if chunk:
            sections[name] = chunk
    return sections

# ----------------------- Deterministic checks -----------
ICD_RE  = re.compile(r"\b([A-TV-ZА-ЯЁ]{1}\d{2}(?:\.\d)?)\b", re.IGNORECASE)
BOX_RE  = re.compile(r"(?:Бокс|Палата)\s*№\s*([0-9]+)")

def extract_icd(text: str) -> List[str]:
    return list({m.group(1).upper().replace("Ё","Е") for m in ICD_RE.finditer(text)})

def extract_boxes(text: str) -> List[str]:
    return list({m.group(1) for m in BOX_RE.finditer(text)})

def deterministic_checks(full_text: str, sections: Dict[str,str]) -> List[Dict[str,Any]]:
    issues: List[Dict[str,Any]] = []
    years = re.findall(r"\b20\d{2}\b", full_text)
    distinct = sorted(set(years))
    if len(distinct) >= 2:
        counts = {y: years.count(y) for y in distinct}
        issues.append({
            "severity":"critical",
            "section":"Документ",
            "title":"Несоответствие годов в датах",
            "evidence":f"Встречаются разные годы: {counts}",
            "fix":"Привести все даты к фактическому году госпитализации."
        })

    icds = extract_icd(full_text)
    if ("тонзиллит" in full_text.lower()) or ("лакунарн" in full_text.lower()) or ("паратонзилл" in full_text.lower()):
        wrong_b = any(code.upper().startswith("B") for code in icds)
        if wrong_b:
            issues.append({
                "severity":"critical",
                "section":"Диагноз",
                "title":"МКБ-10 не соответствует тонзиллиту/паратонзиллиту",
                "evidence":f"Обнаружены коды: {icds}",
                "fix":"Для острого тонзиллита — J03.x; для паратонзиллярного абсцесса/целлюлита — J36."
            })

    boxes = extract_boxes(full_text)
    if len(boxes) > 1:
        issues.append({
            "severity":"major",
            "section":"Размещение",
            "title":"Разные номера бокса/палаты",
            "evidence":f"Встречаются номера: {boxes}",
            "fix":"Унифицировать место; при переводах — отдельная запись с датой/временем."
        })

    triage = sections.get("Триаж", "")
    if triage and ("Пациент должен быть изолирован" in triage) and not re.search(r"изолирован:\s*(Да|Нет)", triage, re.IGNORECASE):
        issues.append({
            "severity":"major",
            "section":"Триаж",
            "title":"Не отмечено решение по изоляции",
            "evidence":"В форме триажа есть поле, но нет выбранного ‘Да/Нет’.",
            "fix":"Отметить и указать режим изоляции (контактный/капельный)."
        })

    plan = sections.get("Осмотр приёмного покоя", "") + "\n" + sections.get("Первичный осмотр", "")
    orders = sections.get("Лист назначений", "")
    if plan and orders:
        need_cef = bool(re.search(r"\bцеф ?(3|триаксон)|ceftriax", plan, re.IGNORECASE))
        got_cef  = bool(re.search(r"\bцеф ?(3|триаксон)|ceftriax", orders, re.IGNORECASE))
        if need_cef and not got_cef:
            issues.append({
                "severity":"critical",
                "section":"Назначения",
                "title":"В плане заявлена парентеральная АБ, в листе назначений её нет",
                "evidence":"В плане — цефтриаксон; в листе назначений не найден.",
                "fix":"Добавить фактические инъекции или оформить смену схемы на per os (step-down)."
            })

    lab_orders = sections.get("Назначения на исследования", "")
    labs_results = sections.get("Результаты исследований", "")
    if lab_orders and (not labs_results or len(labs_results) < 50):
        issues.append({
            "severity":"major",
            "section":"Исследования",
            "title":"Есть заказы на исследования, но нет результатов",
            "evidence":"Лист назначений на исследования заполнен, раздел результатов пуст/скуден.",
            "fix":"Подшить результаты и дать интерпретацию в дневниках."
        })

    return issues

# ----------------------- Ollama chat (параллельно + кеш) --------
OLLAMA_CHAT_PATH = "/api/chat"

def _hash_key(model: str, section_name: str, text: str) -> str:
    h = hashlib.sha256()
    h.update(model.encode("utf-8"))
    h.update(b"\0"); h.update(section_name.encode("utf-8"))
    h.update(b"\0"); h.update(text.encode("utf-8"))
    return h.hexdigest()

def cache_get(key: str) -> Optional[dict]:
    with SessionLocal() as db:
        row = db.get(LLMCache, key)
        if row and row.value:
            try:
                return json.loads(row.value)
            except Exception:
                return None
    return None

def cache_set(key: str, value: dict):
    with SessionLocal() as db:
        db.merge(LLMCache(key=key, value=json.dumps(value, ensure_ascii=False)))
        db.commit()

SECTION_PROMPT = (
    "Ты — медицинский аудитор стационара (инфекционный профиль) в РК.\n"
    "Проверь фрагмент истории болезни строго по чек-листу: прием, эпиданамнез, триаж/изоляция, дневники, назначения (дозы/кратность/подписи и отметки выполнения), исследования (что назначено, что выполнено, интерпретация), инфекционный контроль, переводы/боксы/палаты, выписка/эпикриз/рекомендации/лист нетрудоспособности.\n"
    "Ищи противоречия (даты/годы, разные боксы без записи перевода, диагноз vs МКБ vs назначения, ‘в плане есть — в листе нет’, ‘в результатах пусто’ и т.д.).\n\n"
    "Верни СТРОГО JSON по схеме:\n"
    "{\n  \"section\": \"<название раздела>\",\n  \"issues\": [\n    {\n      \"severity\": \"critical|major|minor\",\n      \"title\": \"...\",\n      \"evidence\": \"короткая цитата/факт из текста\",\n      \"fix\": \"конкретное исправление одной фразой\"\n    }\n  ]\n}\n"
    "Без пояснений и рассуждений."
)

def _ollama_chat_sync(model: str, user_content: str) -> Dict[str,Any]:
    body = {
        "model": model,
        "messages": [
            {"role":"system","content": SECTION_PROMPT},
            {"role":"user","content": user_content}
        ],
        "stream": False,
        "format": "json",
        "options": {"temperature": 0, "num_ctx": 4096},
        "keep_alive": settings.OLLAMA_KEEP_ALIVE,
    }
    r = HTTP.post(OLLAMA_CHAT_PATH, json=body)
    r.raise_for_status()
    data = r.json()
    try:
        content = data["message"]["content"]
        return json.loads(content)
    except Exception:
        return {"error":"bad_json_from_model","raw":data}

def audit_sections_with_llm_parallel(model: str, sections: Dict[str,str]) -> List[Dict[str,Any]]:
    tasks = []
    for name, chunk in sections.items():
        short = chunk if len(chunk) < settings.MAX_SECTION_CHARS else chunk[:settings.MAX_SECTION_CHARS]
        key = _hash_key(model, name, short)
        cached = cache_get(key)
        if cached is not None:
            if isinstance(cached, dict) and "issues" in cached:
                cached["section"] = cached.get("section", name)
                tasks.append((name, None, key, cached))
                continue
        user = f"Раздел: {name}\n\nТекст:\n<<<\n{short}\n>>>\n"
        tasks.append((name, user, key, None))

    results: List[Dict[str,Any]] = []

    def worker(name_user_key):
        name, user, key, already = name_user_key
        if already is not None:
            return already
        res = _ollama_chat_sync(model, user)
        if isinstance(res, dict):
            cache_set(key, res)
        if isinstance(res, dict) and "issues" in res:
            res["section"] = res.get("section", name)
            return res
        return {"section": name, "issues": [{"severity":"minor","title":"Модель вернула нечитаемый JSON","evidence":str(res)[:500],"fix":"Повторить прогон/уменьшить фрагмент"}]}

    to_call = [t for t in tasks if t[1] is not None]
    cached_ready = [t[3] for t in tasks if t[1] is None]
    results.extend(cached_ready)

    if to_call:
        workers = max(1, settings.AUDIT_CONCURRENCY)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(worker, t) for t in to_call]
            for fut in as_completed(futures):
                results.append(fut.result())

    order = list(sections.keys())
    results.sort(key=lambda x: order.index(x.get("section","")) if x.get("section","") in order else 1e9)
    return results

# ----------------------- Reports ------------------------
def flatten_issues(det_issues: List[Dict[str,Any]], llm_blocks: List[Dict[str,Any]]):
    arr: List[Dict[str,Any]] = []
    for it in det_issues:
        arr.append({**it})
    for block in llm_blocks:
        sec = block.get("section","")
        for it in block.get("issues", []):
            arr.append({"section":sec, **it})
    return arr

def summarize(issues_flat: List[Dict[str,Any]]):
    from collections import Counter
    c = Counter(x.get("severity") for x in issues_flat)
    return {"critical": c.get("critical",0), "major": c.get("major",0), "minor": c.get("minor",0), "total": sum(c.values())}

def save_json_report(path: str, report: Dict[str,Any]):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

def save_xlsx_report(path: str, report: Dict[str,Any]):
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Issues"
    ws.append(["Severity","Section","Title","Evidence","Fix"])
    for issue in report["issues_flat"]:
        ws.append([issue.get("severity"), issue.get("section"), issue.get("title"), issue.get("evidence"), issue.get("fix")])
    wb.save(path)

# ----------------------- Annotation ---------------------
def annotate_pdf(pdf_path: str, issues: List[Dict[str,Any]], out_path: str):
    import fitz  # PyMuPDF
    doc = fitz.open(pdf_path)

    def add_note(page, rect, title, content):
        ann = page.add_highlight_annot(rect)
        ann.set_info(title=title, content=content)

    for it in issues:
        ev = (it.get("evidence") or "").strip()
        if not ev:
            continue
        title = f"[{(it.get('severity') or '?').upper()}] {it.get('title','Issue')}"
        fix   = it.get("fix","")
        candidates = [ev]
        if len(ev) > 120:
            candidates.append(ev[:120])
        if len(ev) > 60:
            candidates.append(ev[:60])
        found = False
        for page in doc:
            for cand in candidates:
                cand = cand.strip()
                if not cand:
                    continue
                rects = page.search_for(cand, quads=False)
                if rects:
                    for r in rects:
                        add_note(page, r, title, f"Fix: {fix}")
                    found = True
                    break
            if found:
                break
        if not found and ev:
            words = [w for w in re.split(r"\s+", ev) if len(w) > 3][:6]
            for page in doc:
                hit_rects = []
                for w in words:
                    rects = page.search_for(w)
                    if rects:
                        hit_rects.append(rects[0])
                if hit_rects:
                    union = hit_rects[0]
                    for r in hit_rects[1:]:
                        union |= r
                    add_note(page, union, title, f"Fix: {fix}\n(note: fuzzy)")
                    break
    doc.save(out_path, incremental=False, deflate=True)
    doc.close()

from docx.enum.text import WD_COLOR_INDEX

def annotate_docx(docx_path: str, issues: List[Dict[str,Any]], out_path: str):
    from docx import Document
    doc = Document(docx_path)

    def highlight_substring_in_paragraph(p, needle: str, note_text: str):
        text = p.text
        idx = text.find(needle)
        if idx < 0:
            return False
        before, mid, after = text[:idx], text[idx:idx+len(needle)], text[idx+len(needle):]
        for _ in range(len(p.runs)-1, -1, -1):
            r = p.runs[_]
            r.clear()
            r._element.getparent().remove(r._element)
        p.add_run(before)
        r2 = p.add_run(mid); r2.font.highlight_color = WD_COLOR_INDEX.YELLOW
        p.add_run(after)
        p.add_run(f"  [AUDIT] {note_text}").font.highlight_color = WD_COLOR_INDEX.YELLOW
        return True

    for it in issues:
        ev = (it.get("evidence") or "").strip()
        if not ev:
            continue
        short_ev = ev[:120] if len(ev) > 120 else ev
        note = f"{(it.get('severity') or '?').upper()}: {it.get('title','Issue')} | Fix: {it.get('fix','')}"
        done = False
        for p in doc.paragraphs:
            if short_ev and short_ev in p.text:
                if highlight_substring_in_paragraph(p, short_ev, note):
                    done = True
                    break
        if not done:
            words = [w for w in re.split(r"\s+", short_ev) if len(w) > 5][:5]
            for p in doc.paragraphs:
                if (words and words[0] in p.text) or (len(words) >= 2 and all(w in p.text for w in words[:2])):
                    if p.runs:
                        p.runs[0].font.highlight_color = WD_COLOR_INDEX.YELLOW
                    p.add_run(f"  [AUDIT] {note}").font.highlight_color = WD_COLOR_INDEX.YELLOW
                    break

    doc.save(out_path)

# ----------------------- Schemas ------------------------
class AuditCreateResp(BaseModel):
    id: str
    status: AuditStatus
    model: str

class AuditResp(BaseModel):
    id: str
    status: AuditStatus
    model: str
    input_name: Optional[str] = None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    summary: Optional[Dict[str,Any]] = None
    sections: Optional[List[str]] = None
    error: Optional[str] = None

# ----------------------- API ----------------------------
@app.get("/health")
def health():
    return {"ok": True, "ollama": settings.OLLAMA_BASE_URL}

@app.get("/api/v1/models")
def list_models():
    try:
        r = HTTP.get("/api/tags")
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Ollama not reachable: {e}")

@app.post("/api/v1/audits", response_model=AuditCreateResp)
def create_audit(background_tasks: BackgroundTasks,
                 file: UploadFile = File(...),
                 model: str = Form(settings.DEFAULT_MODEL),
                 xlsx: bool = Form(False),
                 annotate: bool = Form(False)):
    with SessionLocal() as db:
        audit = Audit(model=model, status=AuditStatus.queued, want_xlsx=xlsx, want_annotate=annotate)
        audit.input_name = file.filename
        audit.input_mime = file.content_type
        db.add(audit); db.commit(); db.refresh(audit)

        workdir = os.path.join(settings.STORAGE_DIR, audit.id)
        os.makedirs(workdir, exist_ok=True)
        in_ext = os.path.splitext(file.filename or "")[1] or ".bin"
        in_path = os.path.join(workdir, f"input{in_ext}")
        with open(in_path, "wb") as f:
            f.write(file.file.read())
        audit.input_path = in_path
        db.commit()

        background_tasks.add_task(run_audit_job, audit.id)
        return AuditCreateResp(id=audit.id, status=audit.status, model=audit.model)

@app.get("/api/v1/audits/{audit_id}", response_model=AuditResp)
def get_audit(audit_id: str):
    with SessionLocal() as db:
        audit = db.get(Audit, audit_id)
        if not audit:
            raise HTTPException(status_code=404, detail="Not found")
        summary = json.loads(audit.summary_json) if audit.summary_json else None
        sections = json.loads(audit.sections_json) if audit.sections_json else None
        return AuditResp(
            id=audit.id, status=audit.status, model=audit.model,
            input_name=audit.input_name, created_at=audit.created_at, updated_at=audit.updated_at,
            summary=summary, sections=sections, error=audit.error)

@app.get("/api/v1/audits/{audit_id}/issues")
def get_audit_issues(audit_id: str):
    with SessionLocal() as db:
        audit = db.get(Audit, audit_id)
        if not audit:
            raise HTTPException(status_code=404, detail="Not found")
        return {
            "deterministic": json.loads(audit.deterministic_issues_json) if audit.deterministic_issues_json else [],
            "llm_by_section": json.loads(audit.llm_by_section_json) if audit.llm_by_section_json else [],
            "issues_flat": json.loads(audit.issues_flat_json) if audit.issues_flat_json else []
        }

@app.post("/api/v1/audits/{audit_id}/annotate")
def make_annotation(audit_id: str):
    with SessionLocal() as db:
        audit = db.get(Audit, audit_id)
        if not audit:
            raise HTTPException(status_code=404, detail="Not found")
        if not audit.input_path:
            raise HTTPException(status_code=400, detail="No input file")
        if not audit.issues_flat_json:
            raise HTTPException(status_code=400, detail="No issues to annotate; run audit first")
        issues = json.loads(audit.issues_flat_json)
        base, ext = os.path.splitext(audit.input_path.lower())
        out_path = os.path.join(os.path.dirname(audit.input_path), f"annot{ext}")
        if ext == ".pdf":
            annotate_pdf(audit.input_path, issues, out_path)
        elif ext == ".docx":
            annotate_docx(audit.input_path, issues, out_path)
        else:
            raise HTTPException(status_code=415, detail="Only PDF/DOCX supported for annotation")
        audit.annotated_path = out_path
        db.commit()
        return {"ok": True, "annotated_path": out_path}

@app.get("/api/v1/audits/{audit_id}/download")
def download(audit_id: str, kind: str = Query(..., regex="^(json|xlsx|annotated)$")):
    with SessionLocal() as db:
        audit = db.get(Audit, audit_id)
        if not audit:
            raise HTTPException(status_code=404, detail="Not found")
        path = None
        media = "application/octet-stream"
        if kind == "json":
            path = audit.output_json_path; media = "application/json"
        elif kind == "xlsx":
            path = audit.output_xlsx_path; media = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif kind == "annotated":
            path = audit.annotated_path
            if path:
                media = "application/pdf" if path.lower().endswith(".pdf") else "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        if not path or not os.path.exists(path):
            raise HTTPException(status_code=404, detail="File not found for this kind")
        filename = os.path.basename(path)
        return FileResponse(path, media_type=media, filename=filename)

# ----------------------- Job runner ---------------------
def run_audit_job(audit_id: str):
    log.info(f"run_audit_job: {audit_id}")
    with SessionLocal() as db:
        audit = db.get(Audit, audit_id)
        if not audit:
            return
        try:
            audit.status = AuditStatus.running
            db.commit()

            # read & normalize
            raw = read_text(audit.input_path)
            text = norm_text(raw)
            sections = split_sections(text)

            # deterministic
            det = deterministic_checks(text, sections)

            # LLM per-section (параллельно + кеш)
            llm = audit_sections_with_llm_parallel(audit.model, sections)

            # flatten & summarize
            flat = flatten_issues(det, llm)
            summary = summarize(flat)

            # save reports
            workdir = os.path.dirname(audit.input_path)
            out_json = os.path.join(workdir, "audit.json")
            report = {
                "file": os.path.basename(audit.input_path),
                "model": audit.model,
                "summary": summary,
                "sections": list(sections.keys()),
                "deterministic_issues": det,
                "llm_by_section": llm,
                "issues_flat": flat
            }
            save_json_report(out_json, report)
            audit.output_json_path = out_json

            if audit.want_xlsx:
                out_xlsx = os.path.join(workdir, "audit.xlsx")
                save_xlsx_report(out_xlsx, report)
                audit.output_xlsx_path = out_xlsx

            audit.summary_json = json.dumps(summary, ensure_ascii=False)
            audit.sections_json = json.dumps(list(sections.keys()), ensure_ascii=False)
            audit.deterministic_issues_json = json.dumps(det, ensure_ascii=False)
            audit.llm_by_section_json = json.dumps(llm, ensure_ascii=False)
            audit.issues_flat_json = json.dumps(flat, ensure_ascii=False)

            if audit.want_annotate:
                base, ext = os.path.splitext(audit.input_path.lower())
                out_path = os.path.join(workdir, f"annot{ext}")
                if ext == ".pdf":
                    annotate_pdf(audit.input_path, flat, out_path)
                elif ext == ".docx":
                    annotate_docx(audit.input_path, flat, out_path)
                else:
                    log.warning("Annotation skipped: unsupported ext %s", ext)
                if os.path.exists(out_path):
                    audit.annotated_path = out_path

            audit.status = AuditStatus.succeeded
            db.commit()
        except Exception as e:
            log.exception("audit job failed: %s", e)
            audit.status = AuditStatus.failed
            audit.error = str(e)
            db.commit()

# ----------------------- Root ---------------------------
@app.get("/")
def root():
    return {"name": "med-audit API (fast)", "version": "1.2", "endpoints": [
        "GET /health",
        "GET /api/v1/models",
        "POST /api/v1/audits (multipart: file, model, xlsx, annotate)",
        "GET /api/v1/audits/{id}",
        "GET /api/v1/audits/{id}/issues",
        "POST /api/v1/audits/{id}/annotate",
        "GET /api/v1/audits/{id}/download?kind=json|xlsx|annotated",
    ]}
