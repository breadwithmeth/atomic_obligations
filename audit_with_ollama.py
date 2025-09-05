#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Audit documents with a local LLM via Ollama.

Highlights
- Robust to models that output chatter / <think> blocks / non-JSON.
- "Statuses" mode: model returns only per-rule statuses, which is faster & steadier.
- Chunking + worst-case merge across chunks (fail > warn > pass).
- Offline heuristic fallback for every chunk and for the whole document.
- CSV is never empty: summary + one row per rule with a concrete status.
- DeepSeek-friendly: optional --think false to stop on <think> tags and strip them.

Quick start (offline smoke test)
    python audit_with_ollama.py \
      "+*.docx" --profile infektsionka --offline \
      --out audit_report --debug

With a small local model (recommended on MacBook Air)
    ollama pull llama3.2:3b-instruct-q5_1
    python audit_with_ollama.py "infection.docx" \
      --model llama3.2:3b-instruct-q5_1 --profile infektsionka \
      --llm-mode statuses --think false \
      --num-ctx 4096 --num-predict 500 \
      --chunk-chars 8000 --chunk-overlap 500 \
      --out audit_report --dump-prompts --debug
"""

from __future__ import annotations
import os, sys, json, csv, re, glob, time, zipfile, xml.etree.ElementTree as ET
from typing import List, Dict, Any, Tuple, Optional
import argparse

# ------------------------ CLI ------------------------
ap = argparse.ArgumentParser()
ap.add_argument("inputs", nargs="+", help="DOCX files or globs")
ap.add_argument("--model", default="llama3.2:3b-instruct-q5_1", help="Ollama model name (must be pulled)")
ap.add_argument("--out", default="audit_report", help="Output base path (without extension)")
ap.add_argument("--profile", default="auto", choices=["auto", "generic", "infektsionka"], help="Audit profile")
ap.add_argument("--ollama-host", default=os.getenv("OLLAMA_HOST", "127.0.0.1:11434"), help="host:port for Ollama")
ap.add_argument("--timeout", type=int, default=int(os.getenv("OLLAMA_TIMEOUT", "600")), help="HTTP timeout seconds per request")
ap.add_argument("--retries", type=int, default=int(os.getenv("OLLAMA_RETRIES", "2")), help="Retries on timeout/HTTP errors")
ap.add_argument("--backoff", type=float, default=float(os.getenv("OLLAMA_BACKOFF", "2.0")), help="Exponential backoff factor")
ap.add_argument("--max-chars", type=int, default=int(os.getenv("AUDIT_MAX_CHARS", "120000")), help="Max characters to keep after focusing")
ap.add_argument("--offline", action="store_true", help="Run heuristic offline audit (no Ollama)")
ap.add_argument("--debug", action="store_true", help="Verbose debug output")
ap.add_argument("--dump-prompts", action="store_true", help="Save focused prompts into <out>.prompts/")
# LLM tuning
ap.add_argument("--num-ctx", type=int, default=int(os.getenv("AUDIT_NUM_CTX", "4096")), help="Ollama options.num_ctx")
ap.add_argument("--num-predict", type=int, default=int(os.getenv("AUDIT_NUM_PREDICT", "600")), help="Ollama options.num_predict")
# Chunking
ap.add_argument("--chunk-chars", type=int, default=int(os.getenv("AUDIT_CHUNK_CHARS", "8000")), help="If >0, split focused text into chunks of this size")
ap.add_argument("--chunk-overlap", type=int, default=int(os.getenv("AUDIT_CHUNK_OVERLAP", "500")), help="Chars of overlap between chunks")
# Output discipline
ap.add_argument("--llm-mode", choices=["full", "statuses"], default=os.getenv("AUDIT_LLM_MODE", "statuses"), help="Model output mode")
ap.add_argument("--think", choices=["true", "false"], default="true", help="Allow model thoughts (<think>...</think>). Set false to disable.")
ap.add_argument("--structured", action="store_true", help="Use JSON Schema (Ollama structured outputs) in statuses mode")
args = ap.parse_args()

OLLAMA_HOST = args.ollama_host

# ------------------------ DeepSeek-safe cleaning & lenient JSON parsing ------------------------
import re as _re2, json as _json2

def strip_think_blocks(s: str) -> str:
    """Remove <think>...</think> blocks that some models (e.g., DeepSeek) emit."""
    return _re2.sub(r"<think>.*?</think>", "", s or "", flags=_re2.S|_re2.I)


def parse_json_lenient(s: str) -> dict:
    """Extract the first plausible JSON object from a free-form model response.
    Tries: direct json; fenced ```json ...```; first balanced {...} span.
    """
    s = strip_think_blocks(s)
    # 1) direct JSON
    try:
        return _json2.loads(s)
    except Exception:
        pass
    # 2) fenced block ```json ... ```
    m = _re2.search(r"```(?:json)?\s*(\{.*?\})\s*```", s, flags=_re2.S|_re2.I)
    if m:
        try:
            return _json2.loads(m.group(1))
        except Exception:
            pass
    # 3) first balanced {...}
    start = s.find("{")
    while start != -1:
        depth = 0
        for i, ch in enumerate(s[start:], start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    cand = s[start:i+1]
                    try:
                        return _json2.loads(cand)
                    except Exception:
                        break
        start = s.find("{", start + 1)
    return {}


def with_stops(opts: Optional[dict]) -> dict:
    """Add stop sequences to cut out <think> blocks early when --think false."""
    o = dict(opts or {})
    if getattr(args, "think", "true") == "false":
        st = o.get("stop") or []
        if isinstance(st, str):
            st = [st]
        st += ["<think>", "</think>"]
        o["stop"] = list(dict.fromkeys(st))  # dedupe, keep order
    return o

# ------------------------ .docx text extraction ------------------------

def extract_docx_text(docx_path: str) -> str:
    try:
        with zipfile.ZipFile(docx_path) as z:
            xml = z.read("word/document.xml")
    except Exception:
        return ""
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    root = ET.fromstring(xml)
    parts = []
    for p in root.findall(".//w:p", ns):
        parts.extend([t.text for t in p.findall(".//w:t", ns) if t.text])
        parts.append("\n")
    return "".join(parts)

# ------------------------ helpers ------------------------

RE_ICD10 = re.compile(r"\b([A-ZА-Я]\d{2}(?:\.\d{1,3})?)\b")
RE_SERVICE = re.compile(r"\b([A-Z]\d{2}\.\d{3}\.\d{3})\b")
RE_IIN = re.compile(r"\b\d{11}\b")
RE_DATE = re.compile(r"(20\d{2})[.\-/](\d{2})[.\-/](\d{2})")

FOCUS_KEYS = [
    "эпиданамнез", "Эпидемиологический анамнез", "Диагноз", "МКБ", "Назначени", "Лист врачебных назначений",
    "ЛИСТ НАЗНАЧЕНИЙ НА ИССЛЕДОВАНИЕ", "Исследован", "ПЦР", "бакпосев", "посев", "микробиолог",
    "изоляц", "бокс", "кохорт", "СЭК", "058/у", "058-у", "экстренное извещ", "Выписной эпикриз",
    "Приемное", "Приёмное", "Исход", "Койко", "дезинфек", "УФ"
]


def detect_doc_type(text: str) -> str:
    if "ИСТОРИЯ РАЗВИТИЯ НОВОРОЖДЕННОГО" in text or "НӘРЕСТЕНІҢ ДАМУ ТАРИХЫ" in text:
        return "newborn"
    if "ФОРМА № 001/У" in text or "МЕДИЦИНСКАЯ КАРТА СТАЦИОНАРНОГО ПАЦИЕНТА" in text:
        return "inpatient"
    return "unknown"


def quick_parse(text: str) -> Dict[str, Any]:
    icd10 = sorted(set(RE_ICD10.findall(text)))
    services = sorted(set(RE_SERVICE.findall(text)))
    low = text.lower()
    flags = {
        "has_epidanamnez": ("эпид" in low) or ("эпидемиологический анамнез" in low),
        "has_isolation": any(k in low for k in ["изоляц", "бокс", "боксирован", "контактно-изоляц", "кохорт"]),
        "has_058u": ("058/у" in text) or ("058-у" in text) or ("экстренное извещ" in low) or ("сэк" in low),
        "has_pcr": "пцр" in low,
        "has_bact": any(k in low for k in ["бакпосев", "кровь на посев", "посев", "микробиолог", "мазок"]),
        "has_epi_measures": any(k in low for k in ["дезинфек", "кварц", "бактерицид", "уборка", "инфекционный контроль", "исмп"]),
        "has_contact_list": any(k in low for k in ["контактные лица", "контактировал", "список контактов"]),
        "has_orders_sheet": ("лист врачебных назначений" in low) or ("назначения:" in low),
        "has_department_hint": ("отделен" in low) or ("профиль" in low),
        "has_patient_name_hint": any(k in low for k in ["фамилия", "имя", "отчество", "пациент"]),
        "has_date_hint": bool(RE_DATE.search(text)),
        "has_iin_hint": bool(RE_IIN.search(text)),
        "maybe_infectious": any(code.startswith(("A", "B", "А", "В")) for code in icd10) or "инфекц" in low,
    }
    return {
        "icd10_candidates": icd10,
        "service_code_candidates": services,
        "flags": flags,
    }


def focus_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    idxs: List[int] = []
    low = text.lower()
    for key in FOCUS_KEYS:
        pos = 0
        key_low = key.lower()
        while True:
            i = low.find(key_low, pos)
            if i == -1:
                break
            idxs.append(i)
            pos = i + len(key_low)
    if not idxs:
        return text[:max_chars]
    WINDOW = max_chars // max(1, min(len(idxs), 12))
    HALF = max(1000, WINDOW // 2)
    chunks: List[str] = []
    taken = 0
    for i in sorted(idxs)[:12]:
        start = max(0, i - HALF)
        end = min(len(text), i + HALF)
        chunk = text[start:end]
        chunks.append(chunk)
        taken += len(chunk)
        if taken >= max_chars:
            break
    focused = "\n...\n".join(chunks)
    return focused[:max_chars]


def chunk_text(text: str, size: int, overlap: int) -> List[str]:
    if size <= 0 or len(text) <= size:
        return [text]
    chunks: List[str] = []
    i = 0
    n = len(text)
    step = max(1, size - max(0, overlap))
    while i < n:
        chunks.append(text[i : min(i + size, n)])
        if i + size >= n:
            break
        i += step
    return chunks

# ------------------------ HTTP client ------------------------

def http_post_json(url: str, body: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    import urllib.request, urllib.error
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def call_ollama_chat(model: str, messages: List[Dict[str, str]], *, options: Optional[Dict[str, Any]] = None, timeout: int = 600, retries: int = 2, backoff: float = 2.0, schema: Optional[dict] = None) -> str:
    """Chat API wrapper with retries. We DO NOT set format:"json" to avoid content=null bugs."""
    import urllib.error
    url = f"http://{OLLAMA_HOST}/api/chat"
    body: Dict[str, Any] = {"model": model, "messages": messages, "stream": False, "think": False}
    if options:
        body["options"] = options
    err = None
    for attempt in range(1, retries + 1):
        try:
            resp = http_post_json(url, body, timeout=timeout)
            msg = (resp.get("message", {}) or {}).get("content", "") or resp.get("content", "")
            if not msg:
                raise RuntimeError("empty response from /api/chat")
            return msg
        except (urllib.error.URLError, TimeoutError, RuntimeError) as e:
            err = e
            if attempt == retries:
                break
            sleep_s = backoff ** attempt
            if args.debug:
                print(f"[retry {attempt}] {e} -> sleep {sleep_s:.1f}s", file=sys.stderr)
            time.sleep(sleep_s)
    raise RuntimeError(f"Ollama chat failed after {retries} attempts: {err}")


def call_ollama_generate(model: str, prompt: str, *, options: Optional[Dict[str, Any]] = None, timeout: int = 600, retries: int = 2, backoff: float = 2.0) -> str:
    """Fallback to the simpler /api/generate endpoint if chat misbehaves."""
    import urllib.error
    url = f"http://{OLLAMA_HOST}/api/generate"
    body: Dict[str, Any] = {"model": model, "prompt": prompt, "stream": False}
    if options:
        body["options"] = options
    err = None
    for attempt in range(1, retries + 1):
        try:
            resp = http_post_json(url, body, timeout=timeout)
            msg = resp.get("response", "")
            if not msg:
                raise RuntimeError("empty response from /api/generate")
            return msg
        except (urllib.error.URLError, TimeoutError, RuntimeError) as e:
            err = e
            if attempt == retries:
                break
            sleep_s = backoff ** attempt
            if args.debug:
                print(f"[retry {attempt}] {e} -> sleep {sleep_s:.1f}s", file=sys.stderr)
            time.sleep(sleep_s)
    raise RuntimeError(f"Ollama generate failed after {retries} attempts: {err}")

# ------------------------ Rules ------------------------

BASE_RULES = [
    {"id": "R-001", "title": "Обязательные реквизиты заполнены (ФИО, ИИН, дата, отделение)", "severity": "high", "criterion": "ФИО, ИИН, даты поступления/выписки (или родов), отделение/профиль."},
    {"id": "R-002", "title": "Диагноз и коды МКБ-10 оформлены корректно", "severity": "high", "criterion": "Есть диагноз и корректные коды МКБ-10."},
    {"id": "R-003", "title": "Назначения и исследования отражены", "severity": "medium", "criterion": "Есть лист назначений (дозы/режим) и назначения/результаты исследований (коды услуг)."},
    {"id": "R-004", "title": "Новорождённые: Апгар и заключительный диагноз", "severity": "high", "criterion": "Для новорождённых присутствуют Апгар и заключительный диагноз."},
    {"id": "R-005", "title": "Хронология дат согласована", "severity": "medium", "criterion": "Поступление ≤ перевод ≤ выписка; даты согласованы."},
]

INF_RULES = [


    {"id": "INF-001", "title": "Профиль: инфекционное заболевание подтверждён/обоснован", "severity": "high", "criterion": "Диапазон МКБ-10 A00–B99 или явное инфекционное заболевание; есть эпиданамнез."},
    {"id": "INF-002", "title": "Эпиданамнез оформлен", "severity": "high", "criterion": "Контакты, поездки/эндемичные зоны, пути передачи, прививки/переболевание."},
    {"id": "INF-003", "title": "Экстренное извещение (форма 058/у) оформлено и направлено в срок", "severity": "high", "criterion": "Отметка об оформлении/направлении в СЭК ≤12 ч; дата/время, №, адресат."},
    {"id": "INF-004", "title": "Режим изоляции/бокса и инфекционный контроль", "severity": "high", "criterion": "Бокс/изоляция, СИЗ, дезрежим/УФ, транспортировка; отметки персонала."},
    {"id": "INF-005", "title": "Забор биоматериала и лабораторная диагностика", "severity": "high", "criterion": "ПЦР/посев/серология с датой/временем, преимущественно до старта этиотропной терапии."},
    {"id": "INF-006", "title": "Маршрутизация и оповещение СЭК/контактных", "severity": "medium", "criterion": "Передача сведений в СЭК, список контактных, эпидрасследование/карта очага при показаниях."},
    {"id": "INF-007", "title": "Этиотропная терапия обоснована", "severity": "medium", "criterion": "Дозы/режим/длительность по диагнозу, коррекция по результатам лаборатории."},
    {"id": "INF-008", "title": "ИСМП (если есть): регистрация и расследование", "severity": "high", "criterion": "Регистрация, извещение, эпидрасследование, дезмероприятия, протокол комиссии по ИК."},
    {"id": "INF-009", "title": "001/у ведётся своевременно", "severity": "medium", "criterion": "Приемный раздел — в день поступления; ежедневная динамика; выписной эпикриз с итоговым МКБ-10."},
    {"id": "INF-010", "title": "Контроль контактов по отделению", "severity": "low", "criterion": "Наблюдение за соседними пациентами/контактами; профилактика при показаниях."},
    {"id": "INF-011", "title": "Памятка/обучение пациента", "severity": "low", "criterion": "Инструктаж по изоляции/гигиене/режиму отмечен в документации."},
]

NORMATIVE_REFERENCES = [
    "Стандарт организации помощи при инфекционных заболеваниях (МЗ РК № 40 от 17.03.2023).",
    "Формы учётной документации: 001/у (№ ҚР ДСМ-175/2020, с изм.).",
    "Регистрация/отчётность по инфекционным заболеваниям (№ ҚР ДСМ-169/2020, ред. 15.07.2024).",
    "Санитарные правила по противоэпидемическим мероприятиям (№ 126, с обновлениями).",
    "Экстренное извещение 058/у — направление в СЭК в течение 12 часов.",
]

# ------------------------ Prompts ------------------------

SYSTEM_FULL = (
    "Вы — аудитор медицинской документации РК. Проверьте заполненность, своевременность и согласованность.\n"
    "Отвечайте СТРОГО одним JSON-объектом: file, doc_type, profile, issues[], score, overall.\n"
    "issues[]: {id, title, severity, status:[pass|warn|fail], explanation, evidence[]}.\n"
    "evidence[] — короткие дословные фрагменты с датами/маркерами.\n"
    "Оценка строгая: нет данных — fail; частично — warn; полно — pass.\n"
    "Никаких <think> и текста вне JSON. Только один JSON-объект."
)

SYSTEM_STATUSES = (
    "Вы — аудитор медицинской документации РК. Задача — выставить статусы для каждого правила.\n"
    "Верните СТРОГО один JSON-объект вида:\n"
    "{\"statuses\": {\"<RULE_ID>\": \"pass|warn|fail\", ...},\n \"notes\": {\"<RULE_ID>\": {\"explanation\": \"кратко почему\", \"evidence\": [\"фрагмент1\",\"фрагмент2\"]}}}\n"
    "Правила обязательны ВСЕ, даже если данных нет: тогда статус = \"fail\" и короткое объяснение типа \"нет сведений\".\n"
    "Никаких <think> и текста вне JSON. Только один JSON-объект."
)

def build_status_schema(rules):
    props = {r["id"]: {"type":"string","enum":["pass","warn","fail"]} for r in rules}
    return {
        "type":"object","additionalProperties":False,
        "properties":{
            "statuses":{
                "type":"object","additionalProperties":False,
                "properties": props,
                "required": list(props.keys())
            }
        },
        "required":["statuses"]
    }

def build_prompt(file: str, doc_type: str, text: str, quick: Dict[str, Any], rules: List[Dict[str, Any]], profile: str, *, chunk_idx: Optional[int] = None, chunk_total: Optional[int] = None, mode: str = "statuses") -> Tuple[str, str]:
    if mode == "statuses":
        rules_light = [{"id": r["id"], "title": r["title"], "severity": r["severity"]} for r in rules]
        payload = {
            "file": file,
            "doc_type": doc_type,
            "profile": profile,
            "norm_refs": NORMATIVE_REFERENCES,
            "rules": rules_light,
            "quick_parse": quick,
            "text": text,
        }
        if chunk_idx is not None:
            payload["chunk"] = {"index": chunk_idx, "total": chunk_total}
        return SYSTEM_STATUSES, json.dumps(payload, ensure_ascii=False, indent=2)
    else:
        payload = {
            "file": file,
            "doc_type": doc_type,
            "profile": profile,
            "norm_refs": NORMATIVE_REFERENCES,
            "rules": rules,
            "quick_parse": quick,
            "text": text,
        }
        if chunk_idx is not None:
            payload["chunk"] = {"index": chunk_idx, "total": chunk_total}
        return SYSTEM_FULL, json.dumps(payload, ensure_ascii=False, indent=2)

# ------------------------ OFFLINE heuristic audit ------------------------

def offline_audit(doc_file: str, text: str, doc_type: str, profile: str, qp: Dict[str, Any], rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    flags = qp["flags"]
    issues: List[Dict[str, Any]] = []

    def push(id_: str, title: str, severity: str, ok: Optional[bool] = None, warn: bool = False, explanation: str = "", evidence: Optional[List[str]] = None):
        if evidence is None:
            evidence = []
        status = "pass" if ok else ("warn" if warn else "fail")
        issues.append({"id": id_, "title": title, "severity": severity, "status": status, "explanation": explanation, "evidence": evidence})

    # R-001
    ok = flags["has_patient_name_hint"] and flags["has_iin_hint"] and flags["has_date_hint"] and flags["has_department_hint"]
    warn_cond = (flags["has_patient_name_hint"] and flags["has_date_hint"]) or (flags["has_iin_hint"] and flags["has_date_hint"])
    push(
        "R-001",
        "Обязательные реквизиты заполнены (ФИО, ИИН, дата, отделение)",
        "high",
        ok=ok,
        warn=(not ok and warn_cond),
        explanation="Грубая проверка: найден ли ИИН, даты, отделение и ФИО/поля пациента.",
        evidence=[
            ("ИИН найден" if flags["has_iin_hint"] else "ИИН не найден"),
            ("Дата найдена" if flags["has_date_hint"] else "Дата не найдена"),
            ("Отделение найдено" if flags["has_department_hint"] else "Отделение не найдено"),
        ],
    )

    # R-002
    ok = len(qp["icd10_candidates"]) > 0
    push(
        "R-002",
        "Диагноз и коды МКБ-10 оформлены корректно",
        "high",
        ok=ok,
        warn=False,
        explanation="Проверка по наличию шаблонов МКБ-10 в тексте.",
        evidence=qp["icd10_candidates"][:3],
    )

    # R-003
    ok = flags["has_orders_sheet"] or len(qp["service_code_candidates"]) > 0
    warn_c = (not ok) and ("назнач" in text.lower())
    push(
        "R-003",
        "Назначения и исследования отражены",
        "medium",
        ok=ok,
        warn=warn_c,
        explanation="Поиск листа назначений и кодов услуг.",
        evidence=(qp["service_code_candidates"][:3] if qp["service_code_candidates"] else []),
    )

    # R-004 (newborn only)
    if doc_type == "newborn":
        has_apgar = ("апгар" in text.lower())
        has_final = ("заключительный диагноз новорожд" in text.lower())
        ok = has_apgar and has_final
        warn_c = has_apgar or has_final
        push(
            "R-004",
            "Новорождённые: Апгар и заключительный диагноз",
            "high",
            ok=ok,
            warn=warn_c,
            explanation="Проверка наличия Апгар и заключительного диагноза новорождённого.",
            evidence=[],
        )

    # R-005 (heuristic chronology)
    dates = RE_DATE.findall(text)
    ok = len(dates) >= 2
    push(
        "R-005",
        "Хронология дат согласована",
        "medium",
        ok=ok,
        warn=(not ok and len(dates) == 1),
        explanation="Эвристика по количеству дат в документе.",
        evidence=[f"{y}-{m}-{d}" for (y, m, d) in dates[:3]],
    )

    # INF profile extras
    if profile == "infektsionka":
        maybe_inf = flags["maybe_infectious"]
        epi = flags["has_epidanamnez"]
        # INF-001
        ok = maybe_inf and epi
        warn_c = maybe_inf or epi
        push(
            "INF-001",
            "Профиль: инфекционное заболевание подтверждён/обоснован",
            "high",
            ok=ok,
            warn=warn_c,
            explanation="Проверка МКБ-10 A/B и наличия эпиданамнеза.",
            evidence=qp["icd10_candidates"][:2],
        )
        # INF-002
        push(
            "INF-002",
            "Эпиданамнез оформлен",
            "high",
            ok=epi,
            warn=False,
            explanation="Поиск раздела/упоминаний эпиданамнеза.",
            evidence=[],
        )
        # INF-003 058/у
        has058 = flags["has_058u"]
        push(
            "INF-003",
            "Экстренное извещение (форма 058/у) оформлено и направлено в срок",
            "high",
            ok=has058,
            warn=False,
            explanation="Поиск 058/у/экстренного извещения/СЭК.",
            evidence=[],
        )
        # INF-004 изоляция
        push(
            "INF-004",
            "Режим изоляции/бокса и инфекционный контроль",
            "high",
            ok=flags["has_isolation"],
            warn=(not flags["has_isolation"]),
            explanation="Поиск слов: изоляция/бокс/кохорт.",
            evidence=[],
        )
        # INF-005 лаб-диагностика
        ok = flags["has_pcr"] or flags["has_bact"]
        push(
            "INF-005",
            "Забор биоматериала и лабораторная диагностика",
            "high",
            ok=ok,
            warn=False,
            explanation="Наличие ПЦР/посев/микробиология.",
            evidence=[],
        )
        # INF-006 оповещение/контакты
        push(
            "INF-006",
            "Маршрутизация и оповещение СЭК/контактных",
            "medium",
            ok=flags["has_contact_list"],
            warn=(not flags["has_contact_list"]),
            explanation="Поиск списка контактных/оповещения.",
            evidence=[],
        )
        # INF-007 этиотропная терапия — эвристика
        push(
            "INF-007",
            "Этиотропная терапия обоснована",
            "medium",
            ok=flags["has_orders_sheet"],
            warn=(not flags["has_orders_sheet"]),
            explanation="Эвристика по наличию листа назначений.",
            evidence=[],
        )
        # INF-008 ИСМП — отметим только если упоминалось
        has_ismp = ("исмп" in text.lower()) or ("внутрибольнич" in text.lower())
        if has_ismp:
            push(
                "INF-008",
                "ИСМП (если есть): регистрация и расследование",
                "high",
                ok=True,
                warn=False,
                explanation="Упоминание ИСМП/внутрибольничной инфекции обнаружено.",
                evidence=[],
            )
        # INF-009 ведение 001/у
        ok = ("приемн" in text.lower()) or ("приёмн" in text.lower())
        push(
            "INF-009",
            "001/у ведётся своевременно",
            "medium",
            ok=ok,
            warn=(not ok),
            explanation="Поиск приёмного раздела/ежедневных записей.",
            evidence=[],
        )
        # INF-010 контроль контактов
        push(
            "INF-010",
            "Контроль контактов по отделению",
            "low",
            ok=flags["has_contact_list"],
            warn=(not flags["has_contact_list"]),
            explanation="Поиск записей о наблюдении контактных.",
            evidence=[],
        )
        # INF-011 памятка/обучение
        educated = any(k in text.lower() for k in ["памятка", "обучен", "инструктаж"])
        push(
            "INF-011",
            "Памятка/обучение пациента",
            "low",
            ok=educated,
            warn=(not educated),
            explanation="Поиск выдачи памятки/инструктажа.",
            evidence=[],
        )

    # scoring
    weights = {"high": 3, "medium": 2, "low": 1}
    total_w = sum(weights[i["severity"]] for i in issues)
    got = 0.0
    for it in issues:
        w = weights[it["severity"]]
        if it["status"] == "pass":
            got += 1.0 * w
        elif it["status"] == "warn":
            got += 0.5 * w
    score = 0 if total_w == 0 else round(100.0 * got / total_w, 1)
    overall = "pass" if score >= 80 else ("warn" if score >= 60 else "fail")

    return {
        "file": os.path.basename(doc_file),
        "doc_type": doc_type,
        "profile": profile,
        "issues": issues,
        "score": score,
        "overall": overall,
    }

# ------------------------ Merging helpers ------------------------

def merge_status_chunks(status_chunks: List[Dict[str, Any]], rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    rank = {"fail": 2, "warn": 1, "pass": 0}
    out_status = {r["id"]: "fail" for r in rules}
    notes_col = {r["id"]: {"explanation": "", "evidence": []} for r in rules}
    for ch in status_chunks:
        st = ch.get("statuses", {})
        nt = ch.get("notes", {})
        for r in rules:
            rid = r["id"]
            stv = st.get(rid, "fail")
            if rank.get(stv, 2) > rank.get(out_status[rid], 2):
                out_status[rid] = stv
            if rid in nt:
                if nt[rid].get("explanation") and not notes_col[rid]["explanation"]:
                    notes_col[rid]["explanation"] = nt[rid]["explanation"]
                for ev in nt[rid].get("evidence", []):
                    if ev not in notes_col[rid]["evidence"]:
                        notes_col[rid]["evidence"].append(ev)
                        if len(notes_col[rid]["evidence"]) >= 5:
                            break
    return {"statuses": out_status, "notes": notes_col}


def statuses_to_final(file: str, doc_type: str, profile: str, merged: Dict[str, Any], rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    weights = {"high": 3, "medium": 2, "low": 1}
    total_w = sum(weights[r["severity"]] for r in rules)
    got = 0.0
    issues: List[Dict[str, Any]] = []
    for r in rules:
        rid = r["id"]
        st = merged["statuses"].get(rid, "fail")
        nt = merged["notes"].get(rid, {"explanation": "", "evidence": []})
        issues.append({
            "id": rid,
            "title": r["title"],
            "severity": r["severity"],
            "status": st,
            "explanation": nt.get("explanation", "")[:800],
            "evidence": nt.get("evidence", [])[:5],
        })
        if st == "pass":
            got += weights[r["severity"]]
        elif st == "warn":
            got += 0.5 * weights[r["severity"]]
    score = 0 if total_w == 0 else round(100.0 * got / total_w, 1)
    overall = "pass" if score >= 80 else ("warn" if score >= 60 else "fail")
    return {"file": file, "doc_type": doc_type, "profile": profile, "issues": issues, "score": score, "overall": overall}


def merge_with_offline(final_from_model: Dict[str, Any], offline_full: Dict[str, Any], rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    rank = {"fail": 2, "warn": 1, "pass": 0}
    by = {r["id"]: {"title": r["title"], "severity": r["severity"], "status": "fail", "explanation": "", "evidence": []} for r in rules}
    for src in (final_from_model, offline_full):
        for it in src.get("issues", []):
            rid = it.get("id")
            if not rid or rid not in by:
                continue
            cur = by[rid]
            st = it.get("status", "fail")
            if st in rank and rank[st] > rank[cur["status"]]:
                cur["status"] = st
            if it.get("explanation") and not cur["explanation"]:
                cur["explanation"] = it["explanation"][:800]
            for ev in it.get("evidence", []):
                if ev not in cur["evidence"]:
                    cur["evidence"].append(ev)
                    if len(cur["evidence"]) >= 5:
                        break
    weights = {"high": 3, "medium": 2, "low": 1}
    total_w = sum(weights[r["severity"]] for r in rules)
    got = 0.0
    issues: List[Dict[str, Any]] = []
    for rid, cur in by.items():
        issues.append({
            "id": rid,
            "title": cur["title"],
            "severity": cur["severity"],
            "status": cur["status"],
            "explanation": cur["explanation"],
            "evidence": cur["evidence"],
        })
        if cur["status"] == "pass":
            got += weights[cur["severity"]]
        elif cur["status"] == "warn":
            got += 0.5 * weights[cur["severity"]]
    score = 0 if total_w == 0 else round(100.0 * got / total_w, 1)
    final = dict(final_from_model)
    final["issues"] = issues
    final["score"] = score
    final["overall"] = "pass" if score >= 80 else ("warn" if score >= 60 else "fail")
    return final


def merge_chunk_results(chunks: List[Dict[str, Any]], rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Precedence fail > warn > pass
    rank = {"fail": 2, "warn": 1, "pass": 0}
    out: Dict[str, Any] = {
        "file": chunks[0].get("file", ""),
        "doc_type": chunks[0].get("doc_type", ""),
        "profile": chunks[0].get("profile", ""),
        "issues": [],
        "score": 0,
        "overall": "fail",
    }
    by_rule: Dict[str, Dict[str, Any]] = {r["id"]: {"title": r["title"], "severity": r["severity"], "status": "fail", "explanation": [], "evidence": []} for r in rules}
    for ch in chunks:
        for it in ch.get("issues", []):
            rid = it.get("id")
            if not rid or rid not in by_rule:
                continue
            cur = by_rule[rid]
            st = it.get("status", "fail")
            if st in rank and cur["status"] in rank and rank[st] > rank[cur["status"]]:
                cur["status"] = st
            if it.get("explanation"):
                cur["explanation"].append(it["explanation"])
            if it.get("evidence"):
                for ev in it["evidence"]:
                    if ev not in cur["evidence"]:
                        cur["evidence"].append(ev)
                        if len(cur["evidence"]) >= 5:
                            break
    issues: List[Dict[str, Any]] = []
    weights = {"high": 3, "medium": 2, "low": 1}
    total_w = sum(weights[r["severity"]] for r in rules)
    got = 0.0
    for rid, cur in by_rule.items():
        status = cur["status"]
        expl = "; ".join(cur["explanation"])[:800]
        issues.append({
            "id": rid,
            "title": cur["title"],
            "severity": cur["severity"],
            "status": status,
            "explanation": expl,
            "evidence": cur["evidence"][:5],
        })
        if status == "pass":
            got += weights[cur["severity"]]
        elif status == "warn":
            got += 0.5 * weights[cur["severity"]]
    score = 0 if total_w == 0 else round(100.0 * got / total_w, 1)
    out["issues"] = issues
    out["score"] = score
    out["overall"] = "pass" if score >= 80 else ("warn" if score >= 60 else "fail")
    return out


def merge_statuses_from_offline(off_chunks: List[Dict[str, Any]], rules: List[Dict[str, Any]], file: str, doc_type: str, profile: str) -> Dict[str, Any]:
    rank = {"fail": 2, "warn": 1, "pass": 0}
    out_status = {r["id"]: "fail" for r in rules}
    notes = {r["id"]: {"explanation": "", "evidence": []} for r in rules}
    for ch in off_chunks:
        for it in ch.get("issues", []):
            rid = it["id"]
            st = it["status"]
            if rank.get(st, 2) > rank.get(out_status[rid], 2):
                out_status[rid] = st
            if it.get("explanation") and not notes[rid]["explanation"]:
                notes[rid]["explanation"] = it["explanation"]
            for ev in it.get("evidence", []):
                if ev not in notes[rid]["evidence"]:
                    notes[rid]["evidence"].append(ev)
    return {"file": file, "doc_type": doc_type, "profile": profile, "statuses": out_status, "notes": notes}

# ------------------------ Runner ------------------------

def main() -> None:
    # expand file globs
    files: List[str] = []
    for pattern in args.inputs:
        if any(ch in pattern for ch in "*?[]"):
            files.extend(glob.glob(pattern))
        else:
            files.append(pattern)
    files = [f for f in files if f.lower().endswith(".docx") and os.path.exists(f)]
    if args.debug:
        print("[debug] matched files:", files, file=sys.stderr)
    if not files:
        print("No .docx files found")
        sys.exit(1)

    jsonl_path = f"{args.out}.jsonl"
    csv_path = f"{args.out}.csv"
    if args.debug:
        print(f"[debug] will write: {jsonl_path}, {csv_path}", file=sys.stderr)
        print(f"[debug] profile={args.profile}, offline={args.offline}, host={OLLAMA_HOST}", file=sys.stderr)

    dump_dir: Optional[str] = None
    if getattr(args, "dump_prompts", False):
        dump_dir = f"{args.out}.prompts"
        os.makedirs(dump_dir, exist_ok=True)

    with open(jsonl_path, "w", encoding="utf-8") as jf, open(csv_path, "w", encoding="utf-8", newline="") as cf:
        cw = csv.writer(cf)
        cw.writerow(["file", "rule_id", "title", "severity", "status", "evidence", "explanation", "score", "overall", "doc_type", "profile"])

        for p in files:
            raw_text = extract_docx_text(p)
            if args.debug:
                print(f"[debug] {os.path.basename(p)} text length: {len(raw_text)}", file=sys.stderr)
            doc_type = detect_doc_type(raw_text)
            qp = quick_parse(raw_text)
            if args.debug:
                print(f"[debug] icd10_candidates={qp['icd10_candidates'][:5]}, services={qp['service_code_candidates'][:3]}", file=sys.stderr)
            use_profile = args.profile if args.profile != "auto" else ("infektsionka" if (qp["flags"].get("maybe_infectious") or "инфекц" in raw_text.lower()) else "generic")
            rules = BASE_RULES + (INF_RULES if use_profile == "infektsionka" else [])

            focused = raw_text if args.offline else focus_text(raw_text, args.max_chars)
            chunks = chunk_text(focused, args.chunk_chars, args.chunk_overlap) if args.chunk_chars > 0 else [focused]
            if args.debug:
                print(f"[debug] {os.path.basename(p)} -> chunks: {len(chunks)}", file=sys.stderr)

            if args.offline:
                # Heuristic-only path
                off_results = [offline_audit(p, ch, doc_type, use_profile, qp, rules) for ch in chunks]
                model_final = merge_chunk_results(off_results, rules)
                final = model_final
            else:
                if args.llm_mode == "statuses":
                    status_chunks: List[Dict[str, Any]] = []
                    for ci, ch in enumerate(chunks, 1):
                        sys_prompt, user_prompt = build_prompt(os.path.basename(p), doc_type, ch, qp, rules, use_profile, chunk_idx=ci, chunk_total=len(chunks), mode="statuses")
                        if dump_dir:
                            try:
                                with open(os.path.join(dump_dir, f"{os.path.basename(p)}.chunk{ci:02d}.json"), "w", encoding="utf-8") as dp:
                                    dp.write(user_prompt)
                            except Exception as e:
                                if args.debug:
                                    print(f"[debug] dump prompt failed: {e}", file=sys.stderr)
                        try:
                            content = call_ollama_chat(
                                model=args.model,
                                messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": user_prompt}],
                                schema=(build_status_schema(rules) if args.llm_mode=="statuses" and args.structured else None),
                                options=with_stops({"temperature": 0.0, "num_ctx": args.num_ctx, "num_predict": args.num_predict}),
                                timeout=args.timeout,
                                retries=args.retries,
                                backoff=args.backoff,
                            )
                            data = parse_json_lenient(content)
                            if "statuses" not in data:
                                # fallback to /api/generate with the same instructions
                                gen_prompt = sys_prompt + "\n\n" + user_prompt
                                content2 = call_ollama_generate(
                                    model=args.model,
                                    prompt=gen_prompt,
                                    options=with_stops({"temperature": 0.0, "num_ctx": args.num_ctx, "num_predict": args.num_predict}),
                                    timeout=args.timeout,
                                    retries=args.retries,
                                    backoff=args.backoff,
                                )
                                data = parse_json_lenient(content2)
                                if "statuses" not in data:
                                    raise RuntimeError("no 'statuses' in model output")
                        except Exception as e:
                            if args.debug:
                                print(f"[debug] chunk {ci} statuses-mode fallback to offline because: {e}", file=sys.stderr)
                            off = offline_audit(p, ch, doc_type, use_profile, qp, rules)
                            data = {
                                "statuses": {it["id"]: it["status"] for it in off["issues"]},
                                "notes": {it["id"]: {"explanation": it["explanation"], "evidence": it["evidence"]} for it in off["issues"]},
                            }
                        status_chunks.append(data)
                    merged = merge_status_chunks(status_chunks, rules)
                    model_final = statuses_to_final(os.path.basename(p), doc_type, use_profile, merged, rules)
                else:
                    # full mode — per-chunk issues[]
                    results: List[Dict[str, Any]] = []
                    for ci, ch in enumerate(chunks, 1):
                        sys_prompt, user_prompt = build_prompt(os.path.basename(p), doc_type, ch, qp, rules, use_profile, chunk_idx=ci, chunk_total=len(chunks), mode="full")
                        if dump_dir:
                            try:
                                with open(os.path.join(dump_dir, f"{os.path.basename(p)}.chunk{ci:02d}.json"), "w", encoding="utf-8") as dp:
                                    dp.write(user_prompt)
                            except Exception as e:
                                if args.debug:
                                    print(f"[debug] dump prompt failed: {e}", file=sys.stderr)
                        try:
                            content = call_ollama_chat(
                                model=args.model,
                                messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": user_prompt}],
                                options=with_stops({"temperature": 0.0, "num_ctx": args.num_ctx, "num_predict": args.num_predict}),
                                timeout=args.timeout,
                                retries=args.retries,
                                backoff=args.backoff,
                            )
                            data = parse_json_lenient(content)
                            if not isinstance(data.get("issues"), list) or len(data["issues"]) == 0:
                                raise RuntimeError("model returned empty issues")
                        except Exception as e:
                            if args.debug:
                                print(f"[debug] chunk {ci} full-mode fallback to offline because: {e}", file=sys.stderr)
                            data = offline_audit(p, ch, doc_type, use_profile, qp, rules)
                        results.append(data)
                    model_final = merge_chunk_results(results, rules)

                # Merge with full-document offline for more conservative final score
                offline_full = offline_audit(p, raw_text, doc_type, use_profile, qp, rules)
                final = merge_with_offline(model_final, offline_full, rules)

            # write JSONL
            jf.write(json.dumps(final, ensure_ascii=False) + "\n")

            # CSV SUMMARY row
            cw.writerow([
                final["file"],
                "SUMMARY",
                "Итог по документу",
                "",
                final.get("overall", ""),
                "",
                f"score={final.get('score', '')}",
                final.get("score", ""),
                final.get("overall", ""),
                final.get("doc_type", ""),
                final.get("profile", ""),
            ])

            # CSV per-rule rows
            for it in final.get("issues", []):
                cw.writerow([
                    final["file"],
                    it.get("id", ""),
                    it.get("title", ""),
                    it.get("severity", ""),
                    it.get("status", ""),
                    " | ".join((it.get("evidence", []) or [])[:3]),
                    it.get("explanation", ""),
                    final.get("score", ""),
                    final.get("overall", ""),
                    final.get("doc_type", ""),
                    final.get("profile", ""),
                ])

    print(f"OK: wrote {jsonl_path} and {csv_path}")
    if getattr(args, "dump_prompts", False):
        print(f"Prompts saved to: {args.out}.prompts")


if __name__ == "__main__":
    main()
