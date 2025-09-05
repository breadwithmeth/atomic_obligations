#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
validate_docx_ollama.py — аудитор медицинских документов (РК).
Функции:
1) Базовые проверки (обязательные поля, ИИН, даты, диагноз и т.д.).
2) Нормативные проверки по YAML (kz_norms.yaml).
3) Интеграция с Ollama (LLM) — строгий JSON, ретраи, таймауты, healthcheck.
4) Фолбэк: если LLM вернул «описание», выполняется авто-коэрсия в JSON.
5) LLM по разделам (каждый раздел чек-листа — отдельный промпт/запрос).
6) LLM-сжатие готового report.md до короткого резюме.

Примеры запуска:
  python3 validate_docx_ollama.py --json инфеция.json --norms kz_norms.yaml --skip-llm
  python3 validate_docx_ollama.py --json инфеция.json --norms kz_norms.yaml --model "llama3.1:latest"
  python3 validate_docx_ollama.py --json инфеция.json --norms kz_norms.yaml \
      --model "llama3.1:latest" --llm-sections llm_sections.yaml --llm-summarize-md \
      --out report.json --md report.md
"""
import argparse, json, os, re, subprocess, sys, yaml
from datetime import date
from typing import Dict, Any, List, Tuple

# ----------------- Алиасы ключей (RU/KZ) -----------------
KEY_ALIASES = {
    "iin": ["ИИН", "ЖСН", "Индивидуальный идентификационный номер"],
    "full_name": ["Фамилия, имя, отчество больного", "ФИО", "Ф.И.О.", "Пациент", "Аты-жөні"],
    "dob": ["Дата рождения", "Туған күні"],
    "sex": ["Пол", "Жынысы"],
    "admit_dt": ["Дата поступления", "Келген күні", "Госпитализация", "Дата госпитализации"],
    "discharge_dt": ["Дата выписки", "Шығарылған күні"],
    "department": ["Отделение", "Бөлімше"],
    "physician": ["Лечащий врач Ф.И.О. (при его наличии), ID", "Лечащий врач", "Дәрігер"],
    "diagnosis": ["Диагноз", "Қойылған диагноз", "Диагноз клинический", "Заключительный диагноз"],
}
REQUIRED_KEYS = ["iin", "full_name", "dob", "sex", "admit_dt", "department", "physician", "diagnosis"]
DATE_PAT = re.compile(r'^\s*(\d{2})\.(\d{2})\.(\d{4})\s*$')

# ----------------- Утилиты -----------------
def parse_ru_date(s: str):
    m = DATE_PAT.match(s or "")
    if not m:
        return None
    d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(y, mth, d)
    except Exception:
        return None

def _normalize_kv(key_values: Dict[str, str]) -> Dict[str, str]:
    norm = {}
    kv = { (k or "").strip(" :\u00A0"): (v or "").strip() for k, v in (key_values or {}).items() }
    for target, aliases in KEY_ALIASES.items():
        for alias in aliases:
            if alias in kv and kv[alias]:
                norm[target] = kv[alias]
                break
    return norm

# ----------------- ИИН (РК) -----------------
def kz_iin_is_valid(iin: str) -> Tuple[bool, str]:
    if not re.fullmatch(r'\d{12}', iin or ""):
        return False, "ИИН должен состоять из 12 цифр"
    digits = list(map(int, iin))
    ctrl = digits[11]
    w1 = [1,2,3,4,5,6,7,8,9,10,11]
    s = sum(digits[i]*w1[i] for i in range(11)) % 11
    if s == 10:
        w2 = [3,4,5,6,7,8,9,10,11,1,2]
        s = sum(digits[i]*w2[i] for i in range(11)) % 11
    if s != ctrl:
        return False, "Некорректная контрольная сумма ИИН"
    yy = digits[0]*10 + digits[1]
    mm = digits[2]*10 + digits[3]
    dd = digits[4]*10 + digits[5]
    cen_code = digits[6]
    if mm == 0 or mm > 12 or dd == 0 or dd > 31:
        return False, "Некорректная дата рождения в ИИН"
    if cen_code in (1,2):
        year = 1800 + yy
    elif cen_code in (3,4):
        year = 1900 + yy
    elif cen_code in (5,6):
        year = 2000 + yy
    else:
        return False, "Некорректный код века/пола в ИИН"
    try:
        _ = date(year, mm, dd)
    except Exception:
        return False, "Несуществующая дата рождения в ИИН"
    return True, ""

def sex_from_iin(iin: str) -> str:
    if not re.fullmatch(r'\d{12}', iin or ""):
        return ""
    cen = int(iin[6])
    if cen in (1,3,5): return "м"
    if cen in (2,4,6): return "ж"
    return ""

def dob_from_iin(iin: str):
    if not re.fullmatch(r'\d{12}', iin or ""):
        return None
    digits = list(map(int, iin))
    yy = digits[0]*10 + digits[1]
    mm = digits[2]*10 + digits[3]
    dd = digits[4]*10 + digits[5]
    cen_code = digits[6]
    if cen_code in (1,2): year = 1800 + yy
    elif cen_code in (3,4): year = 1900 + yy
    elif cen_code in (5,6): year = 2000 + yy
    else: return None
    try: return date(year, mm, dd)
    except Exception: return None

# ----------------- Базовые правила -----------------
def check_rules(doc: Dict[str, Any]) -> Dict[str, Any]:
    kv = doc.get("key_values", {}) or {}
    norm = _normalize_kv(kv)
    errors, warnings = [], []

    for key in REQUIRED_KEYS:
        if key not in norm or not str(norm[key]).strip():
            errors.append({"code":"missing_field","field":key,"msg":f"Отсутствует обязательное поле: {key}"})

    iin = norm.get("iin","")
    if iin:
        ok, why = kz_iin_is_valid(iin)
        if not ok:
            errors.append({"code":"invalid_iin","field":"iin","msg":why})

    sex_val = (norm.get("sex","") or "").strip().lower()
    sex_norm = ""
    if sex_val in ("м", "муж", "мужской", "er", "ер"): sex_norm = "м"
    elif sex_val in ("ж", "жен", "женский", "әйел"): sex_norm = "ж"
    elif sex_val: warnings.append({"code":"unknown_sex_value","field":"sex","msg":f"Неизвестное значение пола: {sex_val}"})
    if iin and sex_norm:
        sex_iin = sex_from_iin(iin)
        if sex_iin and sex_iin != sex_norm:
            errors.append({"code":"sex_mismatch","fields":["sex","iin"],"msg":"Пол не соответствует ИИН"})

    dob_text = norm.get("dob","")
    if dob_text:
        dob = parse_ru_date(dob_text)
        if not dob:
            errors.append({"code":"invalid_date","field":"dob","msg":"Неверный формат даты рождения (ДД.ММ.ГГГГ)"})
        else:
            if dob > date.today():
                errors.append({"code":"future_dob","field":"dob","msg":"Дата рождения в будущем"})
            if iin:
                dob_iin = dob_from_iin(iin)
                if dob_iin and dob_iin != dob:
                    errors.append({"code":"dob_mismatch","fields":["dob","iin"],"msg":"Дата рождения не соответствует ИИН"})

    admit = norm.get("admit_dt",""); discharge = norm.get("discharge_dt","")
    admit_d = parse_ru_date(admit) if admit else None
    disch_d = parse_ru_date(discharge) if discharge else None
    if admit and not admit_d:
        errors.append({"code":"invalid_date","field":"admit_dt","msg":"Неверный формат даты поступления"})
    if discharge and not disch_d:
        errors.append({"code":"invalid_date","field":"discharge_dt","msg":"Неверный формат даты выписки"})
    today = date.today()
    if admit_d and admit_d > today:
        errors.append({"code":"future_date","field":"admit_dt","msg":"Дата поступления в будущем"})
    if disch_d and disch_d > today:
        warnings.append({"code":"future_date","field":"discharge_dt","msg":"Дата выписки в будущем — проверьте"})
    if admit_d and disch_d and disch_d < admit_d:
        errors.append({"code":"date_order","fields":["admit_dt","discharge_dt"],"msg":"Дата выписки раньше даты поступления"})

    if not norm.get("diagnosis"):
        paras = doc.get("paragraphs") or []
        found = None
        icd = re.compile(r'\b([A-Z]\d{2}(?:\.\d)?)\b')
        for p in paras:
            if "Диагноз" in p or "диагноз" in p or icd.search(p):
                found = p.strip(); break
        if found:
            norm["diagnosis"] = found
            warnings.append({"code":"diagnosis_found_in_text","field":"diagnosis","msg":"Диагноз найден в тексте, но отсутствует в key_values","value":found})
        else:
            errors.append({"code":"missing_diagnosis","field":"diagnosis","msg":"Диагноз не найден"})
    if "department" in norm and not norm["department"].strip():
        errors.append({"code":"empty_field","field":"department","msg":"Не указано отделение"})
    if "physician" in norm and not norm["physician"].strip():
        errors.append({"code":"empty_field","field":"physician","msg":"Не указан лечащий врач"})
    return {"normalized": norm, "errors": errors, "warnings": warnings}

# ----------------- Norms (YAML) + расширенные стационарные проверки -----------------
def apply_normative_checks(doc: dict, rule_result: dict, norms_path: str) -> dict:
    import datetime as _dt
    add_errors, add_warnings = [], []
    norm = rule_result.get("normalized", {})
    paragraphs = doc.get("paragraphs") or []
    vitals = doc.get("vitals") or []

    def push(sev, rec):
        (add_errors if sev == "error" else add_warnings).append(rec)

    def has_regex(pattern: str, hay: list) -> bool:
        rx = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
        return any(rx.search(p or "") for p in hay)

    def count_regex(pattern: str, hay: list) -> int:
        rx = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
        return sum(1 for p in hay if rx.search(p or ""))

    def key_exists(target: str) -> bool:
        return target in norm and bool(str(norm[target]).strip())

    def parse_day(s: str):
        m = re.match(r'^\s*(\d{2})\.(\d{2})\.(\d{4})', s or '')
        if not m: return None
        d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(y, mth, d)
        except Exception:
            return None

    def daterange(a: date, b: date):
        cur = a
        while cur <= b:
            yield cur
            cur += _dt.timedelta(days=1)

    def paragraphs_with_date(day_str: str):
        rx = re.compile(r'\b' + re.escape(day_str) + r'\b')
        return [p for p in paragraphs if rx.search(p or "")]

    def day_has_keywords(day: date, pattern: str) -> bool:
        ds = day.strftime("%d.%m.%Y")
        day_paras = paragraphs_with_date(ds)
        if not day_paras:
            return False
        rx = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
        return any(rx.search(p or "") for p in day_paras)

    def day_has_vitals(day: date) -> bool:
        ds = day.strftime("%d.%m.%Y")
        for v in vitals:
            src = v.get("source") or ""
            if ds in src:
                return True
        for p in paragraphs_with_date(ds):
            if re.search(r'\b(АД|Пульс|ЧСС|ЧДД|Температур|Т[°]|SpO2|Сат)\b', p, re.IGNORECASE):
                return True
        return False

    try:
        with open(norms_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception as e:
        return {"errors": [{"code":"norms_load_error","msg": str(e)}], "warnings": []}

    for act in cfg.get("acts", []):
        act_id = act.get("id"); title = act.get("title")
        for ch in act.get("checks", []):
            t = ch.get("type"); sev = ch.get("severity","warning")
            msg = ch.get("message","Нарушение требования акта.")
            rec_base = {"code": f"act:{act_id}", "msg": msg, "act": act_id, "act_title": title}

            if t == "regex_present_in_paragraphs":
                if not has_regex(ch.get("pattern","."), paragraphs):
                    push(sev, rec_base)

            elif t == "key_required_any_alias":
                if not key_exists(ch.get("target","")):
                    push(sev, rec_base)

            elif t == "date_order_rule":
                a, b = ch.get("a"), ch.get("b"); allow_equal = bool(ch.get("allow_equal"))
                da = parse_ru_date(norm.get(a,"")); db = parse_ru_date(norm.get(b,""))
                if da and db:
                    bad = db < da or (db == da and not allow_equal)
                    if bad: push(sev, rec_base)
                else:
                    push("warning", {**rec_base, "msg": msg + " (невозможно проверить — нет дат)"})

            elif t == "conditional_regex_rule":
                if has_regex(ch.get("if_pattern","."), paragraphs):
                    for tch in ch.get("then_checks") or []:
                        if tch.get("type") == "key_required_any_alias" and not key_exists(tch.get("target","")):
                            push(tch.get("severity","warning"), {**rec_base, "msg": tch.get("message","")})
                        if tch.get("type") == "regex_present_in_paragraphs" and not has_regex(tch.get("pattern","."), paragraphs):
                            push(tch.get("severity","warning"), {**rec_base, "msg": tch.get("message","")})

            # -------- Стационарные специфические проверки --------
            elif t == "per_day_keyword_with_date":
                a = parse_ru_date(norm.get("admit_dt",""))
                b = parse_ru_date(norm.get("discharge_dt","")) or date.today()
                if not a:
                    push(sev, {**rec_base, "msg": msg + " (нет даты поступления)"}); continue
                missing = []
                patt = ch.get("pattern", r"(дневник|ежедневн|осмотр|лечащ(ий|его)\s+врач)")
                for day in daterange(a, b):
                    if not day_has_keywords(day, patt):
                        missing.append(day.strftime("%d.%m.%Y"))
                if missing:
                    push(sev, {**rec_base, "missing_dates": missing})

            elif t == "per_day_vitals_presence":
                a = parse_ru_date(norm.get("admit_dt",""))
                b = parse_ru_date(norm.get("discharge_dt","")) or date.today()
                if not a:
                    push(sev, {**rec_base, "msg": msg + " (нет даты поступления)"}); continue
                missing = []
                for day in daterange(a, b):
                    if not day_has_vitals(day):
                        missing.append(day.strftime("%d.%m.%Y"))
                if missing:
                    push(sev, {**rec_base, "missing_dates": missing})

            elif t == "night_duty_notes":
                if has_regex(r'дежур', paragraphs):
                    a = parse_ru_date(norm.get("admit_dt",""))
                    b = parse_ru_date(norm.get("discharge_dt","")) or date.today()
                    if a:
                        night_rx = re.compile(r'\b(2[0-3]|[01]?\d):([0-5]\d)\b')
                        missing = []
                        for day in daterange(a, b):
                            ds = day.strftime("%d.%m.%Y")
                            day_paras = paragraphs_with_date(ds)
                            ok = False
                            for p in day_paras:
                                for m in night_rx.finditer(p):
                                    hh = int(m.group(1))
                                    if hh >= 20 or hh < 8:
                                        ok = True; break
                                if ok: break
                            if not ok:
                                missing.append(ds)
                        if missing:
                            push(sev, {**rec_base, "missing_dates": missing})

            elif t == "head_rounds_schedule":
                a = parse_ru_date(norm.get("admit_dt","")); b = parse_ru_date(norm.get("discharge_dt","")) or date.today()
                if not a:
                    push(sev, {**rec_base, "msg": msg + " (нет даты поступления)"}); continue
                is_severe = has_regex(r'тяж[ёе]л', paragraphs)
                is_medium = has_regex(r'средн(ей|яя)\s+тяжест', paragraphs)
                patt = ch.get("pattern", r'заведующ')
                missing = []
                if is_severe:
                    for day in daterange(a, b):
                        if not day_has_keywords(day, patt):
                            missing.append(day.strftime("%d.%m.%Y"))
                elif is_medium:
                    week_start = a
                    while week_start <= b:
                        week_end = min(week_start + _dt.timedelta(days=6), b)
                        found = any(day_has_keywords(d, patt) for d in daterange(week_start, week_end))
                        if not found:
                            missing.append(f"{week_start.strftime('%d.%m.%Y')}–{week_end.strftime('%d.%m.%Y')}")
                        week_start = week_end + _dt.timedelta(days=1)
                else:
                    if count_regex(patt, paragraphs) == 0:
                        push("warning", {**rec_base, "msg": "Нет отметок об осмотрах заведующим (степень тяжести не выявлена)"} )
                if missing:
                    push(sev, {**rec_base, "missing": missing})

            elif t == "presence_of_epicrisis":
                if not has_regex(r'выписн(ой|ая)\s+эпикриз', paragraphs):
                    push(sev, rec_base)
                else:
                    if not has_regex(r'рекомендац', paragraphs):
                        push("warning", {**rec_base, "msg":"В эпикризе нет явного раздела рекомендаций"})

            elif t == "dosage_patterns_in_prescription":
                if has_regex(r'лист\s+врачебн(ых|ые)\s+назначени', paragraphs):
                    has_dose = has_regex(r'\b(\d+\s*(мг|мл|ЕД|ед\.))\b', paragraphs)
                    has_freq = has_regex(r'(x|×|\bраз(а|ов)?/сут\b|\bкратн)', paragraphs)
                    if not (has_dose and has_freq):
                        push(sev, {**rec_base, "msg":"В листе назначений не обнаружены дозировки/кратность"})

            elif t == "conditional_presence":
                if has_regex(ch.get("if_pattern",""), paragraphs):
                    if not has_regex(ch.get("then_pattern",""), paragraphs):
                        push(sev, rec_base)

            elif t == "age_conditional_presence":
                dob = parse_ru_date(norm.get("dob",""))
                disch = parse_ru_date(norm.get("discharge_dt","")) or date.today()
                if dob:
                    age = disch.year - dob.year - ((disch.month, disch.day) < (dob.month, dob.day))
                    amin = int(ch.get("min", 0)); amax = int(ch.get("max", 200))
                    if amin <= age <= amax:
                        if not has_regex(ch.get("pattern",""), paragraphs):
                            push(sev, {**rec_base, "msg": ch.get("message","") or msg})
                else:
                    push("warning", {**rec_base, "msg":"Возраст не удалось определить (нет корректной даты рождения)"})
            # ------------------------------------------------

    return {"errors": add_errors, "warnings": add_warnings}

# ----------------- Ollama helpers -----------------
def ollama_healthcheck(ollama_url: str) -> dict:
    import urllib.request, json as _json
    info = {"ok": False, "models": [], "version": None, "error": None}
    try:
        try:
            req = urllib.request.Request(ollama_url.rstrip("/") + "/api/version")
            with urllib.request.urlopen(req, timeout=3) as resp:
                info["version"] = resp.read().decode("utf-8").strip()
        except Exception:
            pass
        req = urllib.request.Request(ollama_url.rstrip("/") + "/api/tags")
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = _json.loads(resp.read().decode("utf-8"))
            info["models"] = [m.get("name") for m in data.get("models", [])]
            info["ok"] = True
    except Exception as e:
        info["error"] = str(e)
    return info

def try_parse_json(text: str):
    import json as _json, re as _re
    if not text:
        return None
    text = _re.sub(r"^```json\s*|^```\s*|```\s*$", "", text.strip(), flags=_re.IGNORECASE|_re.MULTILINE)
    start = text.find("{"); end = text.rfind("}")
    if start >= 0 and end > start:
        snippet = text[start:end+1]
        try:
            return _json.loads(snippet)
        except Exception:
            snippet2 = _re.sub(r",\s*}\s*$", "}", snippet)
            snippet2 = _re.sub(r",\s*]", "]", snippet2)
            try:
                return _json.loads(snippet2)
            except Exception:
                return None
    return None

def coerce_llm_to_json(text: str) -> dict:
    out = {
        "errors": [], "warnings": [], "suggestions": [],
        "extracted_fields": {
            "patient": {"name": None, "dob": None, "sex": None, "iin": None},
            "admission": {"type": None, "admit_dt": None},
            "discharge": {"dt": None, "time": None, "reason": None},
            "diagnosis": {"primary": None, "secondary": None}
        }
    }
    if not text: return out
    t = text

    m = re.search(r'(?:Name|Имя|Аты|ФИО)[^:\n]*[:\-]\s*([A-ZА-ЯЁІЇӘӨҮҢҚҒҺ][^\n]*)', t, re.IGNORECASE)
    if m: out["extracted_fields"]["patient"]["name"] = m.group(1).strip()
    m = re.search(r'(?:Date of birth|Дата рождения|Туған күні)[^:\n]*[:\-]\s*(\d{2}[./]\d{2}[./]\d{4})', t, re.IGNORECASE)
    if m: out["extracted_fields"]["patient"]["dob"] = m.group(1).replace("/", ".")
    m = re.search(r'(?:Sex|Пол|Жынысы)[^:\n]*[:\-]\s*(male|female|муж(?:ской)?|жен(?:ский)?|ер|әйел)', t, re.IGNORECASE)
    if m: out["extracted_fields"]["patient"]["sex"] = m.group(1).lower()
    m = re.search(r'(?:IIN|ИИН|ЖСН)[^:\n]*[:\-]\s*(\d{12})', t, re.IGNORECASE)
    if m: out["extracted_fields"]["patient"]["iin"] = m.group(1)

    m = re.search(r'(?:Type of admission|Тип поступления|Түсу түрі)[^:\n]*[:\-]\s*([^\n]+)', t, re.IGNORECASE)
    if m: out["extracted_fields"]["admission"]["type"] = m.group(1).strip()
    m = re.search(r'(?:Date of admission|Дата поступления)[^:\n]*[:\-]\s*(\d{2}[./]\d{2}[./]\d{4})', t, re.IGNORECASE)
    if m: out["extracted_fields"]["admission"]["admit_dt"] = m.group(1).replace("/", ".")

    m = re.search(r'(?:Date of discharge|Дата выписки)[^:\n]*[:\-]\s*(\d{2}[./]\d{2}[./]\d{4})', t, re.IGNORECASE)
    if m: out["extracted_fields"]["discharge"]["dt"] = m.group(1).replace("/", ".")
    m = re.search(r'(?:Time of discharge|Время выписки)[^:\n]*[:\-]\s*([0-2]?\d:[0-5]\d)', t, re.IGNORECASE)
    if m: out["extracted_fields"]["discharge"]["time"] = m.group(1)
    m = re.search(r'(?:Reason for discharge|Причина выписки)[^:\n]*[:\-]\s*([^\n]+)', t, re.IGNORECASE)
    if m: out["extracted_fields"]["discharge"]["reason"] = m.group(1).strip()

    m = re.search(r'(?:Primary diagnosis|Основной диагноз)[^:\n]*[:\-]\s*([^\n]+)', t, re.IGNORECASE)
    if m: out["extracted_fields"]["diagnosis"]["primary"] = m.group(1).strip()
    m = re.search(r'(?:Secondary diagnosis|Сопутствующий|Вторичный диагноз)[^:\n]*[:\-]\s*([^\n]+)', t, re.IGNORECASE)
    if m: out["extracted_fields"]["diagnosis"]["secondary"] = m.group(1).strip()

    if not any([out["extracted_fields"]["patient"]["name"], out["extracted_fields"]["patient"]["dob"],
                out["extracted_fields"]["patient"]["sex"], out["extracted_fields"]["patient"]["iin"],
                out["extracted_fields"]["admission"]["type"], out["extracted_fields"]["discharge"]["dt"],
                out["extracted_fields"]["diagnosis"]["primary"]]):
        out["warnings"].append({"code":"llm_format","msg":"LLM вернул описание вместо JSON; авто-коэрсия не выявила явных полей."})
    else:
        out["warnings"].append({"code":"llm_format","msg":"LLM вернул текст; применена авто-коэрсия в JSON."})
    return out

def make_llm_payload(model: str, doc: Dict[str, Any], norm: Dict[str, Any], max_paragraphs: int = 60, num_ctx: int = 8192, num_predict: int = 512) -> Dict[str, Any]:
    rules = {
        "required_fields": REQUIRED_KEYS,
        "date_format": "ДД.ММ.ГГГГ",
        "checks": [
            "ИИН валиден (контрольная сумма, дата рождения, пол)",
            "Пол согласован с 7-й цифрой ИИН (1/3/5 — муж, 2/4/6 — жен)",
            "Дата рождения не в будущем",
            "Дата выписки не раньше даты поступления",
            "Есть диагноз (строка или код МКБ-10)"
        ]
    }
    system = (
        "Ты — медицинский аудитор. Проверь корректность заполнения карточки пациента по правилам РК (форма 001/у). "
        "ВЫВОДИ ТОЛЬКО ОДИН JSON-ОБЪЕКТ без префиксов/суффиксов, без Markdown. Структура:\n"
        "{\"errors\":[], \"warnings\":[], \"suggestions\":[], \"extracted_fields\":{\"patient\":{\"name\":null,\"dob\":null,\"sex\":null,\"iin\":null},\"admission\":{\"type\":null,\"admit_dt\":null},\"discharge\":{\"dt\":null,\"time\":null,\"reason\":null},\"diagnosis\":{\"primary\":null,\"secondary\":null}}}. "
        "Каждый элемент errors/warnings: {code, msg, field|fields}. Пиши по-русски."
    )
    user = {
        "instruction": "Проверь документ по правилам ниже. Ответ ДОЛЖЕН строго соответствовать указанной структуре JSON. Никакого текста вне JSON.",
        "rules": rules,
        "normalized_fields": norm,
        "key_values": doc.get("key_values", {}),
        "dates_detected": doc.get("dates", []),
        "diagnoses_detected": doc.get("diagnoses", []),
        "sample_paragraphs": (doc.get("paragraphs", [])[:max_paragraphs])
    }
    return {
        "model": model,
        "stream": False,
        "options": {"temperature": 0, "num_ctx": num_ctx, "num_predict": num_predict},
        "messages": [
            {"role":"system", "content": system},
            {"role":"user", "content": json.dumps(user, ensure_ascii=False)}
        ]
    }

def ask_ollama(model: str, payload: dict, ollama_url="http://localhost:11434", timeout_s=180) -> dict:
    import urllib.request, json as _json
    req = urllib.request.Request(
        ollama_url.rstrip("/") + "/api/chat",
        data=_json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        data = _json.loads(resp.read().decode("utf-8"))
    content = data.get("message", {}).get("content", "")
    if isinstance(content, list): content = "".join(content)
    out = try_parse_json(content)
    if out is None:
        out = {"raw": content}
    return out

# ----------------- LLM по разделам -----------------
def _filter_paragraphs_for_section(paragraphs: list, include_patterns: list, max_paragraphs: int) -> list:
    """Вернёт подмножество параграфов, подходящих под include-паттерны (ИЛИ), ограничив количеством."""
    if not paragraphs:
        return []
    if not include_patterns:
        return paragraphs[:max_paragraphs]
    rx = re.compile("|".join(include_patterns), re.IGNORECASE)
    hits = [p for p in paragraphs if p and rx.search(p)]
    return (hits or paragraphs)[:max_paragraphs]

def make_section_payload(model: str, section: dict, doc: dict, norm: dict,
                         num_ctx: int, num_predict: int, max_paragraphs: int) -> dict:
    """Формирует промпт для LLM по конкретному разделу (строгий JSON)."""
    system = (
        "Ты — медицинский аудитор. Проверь ТОЛЬКО указанный раздел стационарной документации: "
        f"{section.get('title','')}. "
        "ВЫВОДИ ТОЛЬКО ОДИН JSON-ОБЪЕКТ: "
        "{\"section_id\":\"\",\"section_title\":\"\",\"errors\":[],\"warnings\":[],\"suggestions\":[],\"extracted_facts\":{}}. "
        "Каждый элемент errors/warnings: {code, msg, field|fields}. Пиши по-русски."
    )
    ctx = _filter_paragraphs_for_section(doc.get("paragraphs", []), section.get("include_patterns", []), max_paragraphs)
    user = {
        "section": {"id": section.get("id"), "title": section.get("title")},
        "instructions": section.get("instructions",""),
        "normalized_fields": norm,
        "date_range": {"admit_dt": norm.get("admit_dt"), "discharge_dt": norm.get("discharge_dt")},
        "context_paragraphs": ctx
    }
    return {
        "model": model,
        "stream": False,
        "options": {"temperature": 0, "num_ctx": num_ctx, "num_predict": num_predict},
        "messages": [
            {"role":"system", "content": system},
            {"role":"user", "content": json.dumps(user, ensure_ascii=False)}
        ]
    }

def run_llm_sections(doc: dict, norm: dict, args) -> dict:
    """Запускает LLM-оценку по секциям и агрегирует результаты."""
    out = {"used": False, "sections": [], "errors": None}
    if not args.llm_sections:
        return out

    # Загружаем список секций
    try:
        import yaml as _yaml
        with open(args.llm_sections, "r", encoding="utf-8") as f:
            cfg = _yaml.safe_load(f) or {}
    except Exception as e:
        out["errors"] = f"Не удалось загрузить {args.llm_sections}: {e}"
        return out

    # Префлайт Ollama
    hc = ollama_healthcheck(args.ollama_url)
    if not hc.get("ok"):
        out["errors"] = f"Ollama недоступен: {hc.get('error','unknown')}"
        return out

    avail = set(hc.get("models") or [])
    def _model_match(req, models):
        if req in models: return True
        if ":" not in req:
            return any((m.split(":",1)[0] == req) for m in models)
        return False
    if not _model_match(args.model, avail):
        out["errors"] = f"Модель '{args.model}' не найдена на сервере; доступно: {sorted(list(avail))}"
        return out

    # Гоним по секциям
    sections = cfg.get("sections") or []
    retries = max(0, args.llm_retries)
    for sec in sections:
        last_err = None
        for attempt in range(retries + 1):
            try:
                payload = make_section_payload(
                    args.model, sec, doc, norm,
                    args.llm_num_ctx, args.llm_num_predict, args.llm_sections_max_paragraphs
                )
                ans = ask_ollama(args.model, payload, args.ollama_url, timeout_s=args.llm_timeout)
                parsed = ans if isinstance(ans, dict) else {"raw": str(ans)}
                if isinstance(parsed, dict) and "raw" in parsed and isinstance(parsed["raw"], str):
                    coerced = coerce_llm_to_json(parsed["raw"])
                    sec_item = {"section_id": sec.get("id"), "title": sec.get("title"), "raw": parsed, "coerced": coerced}
                else:
                    sec_item = {"section_id": sec.get("id"), "title": sec.get("title"), "raw": parsed}
                out["sections"].append(sec_item)
                last_err = None
                break
            except Exception as e:
                last_err = str(e)
        if last_err:
            out["sections"].append({"section_id": sec.get("id"), "title": sec.get("title"), "error": last_err})

    out["used"] = True
    return out

# ----------------- Сжатие готового report.md -----------------
def summarize_md_with_llm(md_text: str, args) -> dict:
    """
    Превращает длинный report.md в короткий, понятный ответ для врача/зав.отделения.
    Возвращает {"used": bool, "summary": str} либо {"used": False, "error": "..."}.
    """
    hc = ollama_healthcheck(args.ollama_url)
    if not hc.get("ok"):
        return {"used": False, "error": f"Ollama недоступен: {hc.get('error','unknown')}"}

    avail = set(hc.get("models") or [])
    def _model_match(req, models):
        if req in models: return True
        if ":" not in req:
            return any((m.split(":",1)[0] == req) for m in models)
        return False
    if not _model_match(args.model, avail):
        return {"used": False, "error": f"Модель '{args.model}' не найдена; доступно: {sorted(list(avail))}"}

    system = (
        "Ты — медицинский аудитор. Суммируй отчет проверки медицинского документа в очень короткий и понятный ответ "
        "для врача/заведующего. Используй простой русский язык, 5–10 лаконичных пунктов максимум: "
        "1) общая оценка (сколько ошибок/предупреждений и где критично), "
        "2) 3–6 главных проблем, "
        "3) 2–3 практических шага. Без лишних деталей, без кода."
    )
    user = {"report_markdown": md_text[:120_000]}
    payload = {
        "model": args.model,
        "stream": False,
        "options": {"temperature": 0, "num_ctx": max(2048, min(32768, getattr(args, "llm_num_ctx", 8192))), "num_predict": min(512, getattr(args, "llm_num_predict", 512))},
        "messages": [
            {"role":"system", "content": system},
            {"role":"user", "content": json.dumps(user, ensure_ascii=False)}
        ]
    }
    try:
        ans = ask_ollama(args.model, payload, args.ollama_url, timeout_s=max(60, getattr(args, "llm_timeout", 180)))
        if isinstance(ans, dict) and "raw" in ans:
            txt = ans["raw"].strip()
        elif isinstance(ans, dict):
            txt = ans.get("summary") or json.dumps(ans, ensure_ascii=False)
        else:
            txt = str(ans)
        return {"used": True, "summary": txt}
    except Exception as e:
        return {"used": False, "error": str(e)}

def write_summary_files(md_path: str, summary_text: str, suggested_path: str="") -> str:
    """Записывает краткое резюме в файл. Если suggested_path не задан — делает <md>_summary.md. Возвращает путь к файлу."""
    if suggested_path:
        outp = suggested_path
    else:
        base = md_path or "report.md"
        outp = re.sub(r"\.md$", "_summary.md", base) if base.endswith(".md") else (base + "_summary.md")
    with open(outp, "w", encoding="utf-8") as f:
        f.write(summary_text.strip() + "\n")
    return outp

# ----------------- Main -----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--docx", help="Путь к .docx (будет вызван extract_docx.py)")
    ap.add_argument("--json", help="Путь к JSON, ранее извлеченному")
    ap.add_argument("--out", default="report.json", help="Куда сохранить отчет JSON")
    ap.add_argument("--md", default="", help="Куда сохранить Markdown отчет (необязательно)")

    ap.add_argument("--model", default="llama3.1", help="Имя модели Ollama (напр., 'llama3.1' или 'qwen2.5:7b')")
    ap.add_argument("--ollama-url", default="http://localhost:11434", help="URL Ollama")
    ap.add_argument("--skip-llm", action="store_true", help="Пропустить этап Ollama")
    ap.add_argument("--llm-timeout", type=int, default=180, help="Таймаут ответа Ollama, сек")
    ap.add_argument("--llm-retries", type=int, default=2, help="Количество повторов при ошибках/таймаутах")
    ap.add_argument("--llm-max-paragraphs", type=int, default=60, help="Сколько параграфов для общего LLM")
    ap.add_argument("--llm-num-predict", type=int, default=512, help="Ограничение длины ответа модели")
    ap.add_argument("--llm-num-ctx", type=int, default=8192, help="Контекст модели")

    ap.add_argument("--norms", default="kz_norms.yaml", help="YAML с нормативными проверками")

    # LLM по разделам
    ap.add_argument("--llm-sections", default="", help="YAML со списком LLM-разделов (персональные промпты)")
    ap.add_argument("--llm-sections-max-paragraphs", type=int, default=200, help="Макс. кол-во параграфов в контексте раздела")

    # LLM-сжатие Markdown-отчёта
    ap.add_argument("--llm-summarize-md", action="store_true", help="Прогнать готовый Markdown-отчет через LLM и получить краткое резюме")
    ap.add_argument("--summary-out", default="", help="Куда сохранить краткое резюме (если не указано — рядом с MD)")

    args = ap.parse_args()

    if not args.docx and not args.json:
        print("Нужно указать --docx или --json", file=sys.stderr)
        sys.exit(2)

    # Получаем JSON документа
    if args.json:
        with open(args.json, "r", encoding="utf-8") as f:
            doc = json.load(f)
    else:
        extractor = os.path.join(os.path.dirname(__file__), "extract_docx.py")
        if not os.path.exists(extractor):
            print("extract_docx.py не найден. Поместите его рядом или используйте --json.", file=sys.stderr)
            sys.exit(2)
        tmp_json = "_doc_extracted.json"
        res = subprocess.run([sys.executable, extractor, args.docx, "--out", tmp_json], capture_output=True, text=True)
        if res.returncode != 0:
            print("extract_docx.py завершился с ошибкой:", res.stderr, file=sys.stderr)
            sys.exit(1)
        with open(tmp_json, "r", encoding="utf-8") as f:
            doc = json.load(f)
        try: os.remove(tmp_json)
        except OSError: pass

    # 1) Базовые правила
    rule_result = check_rules(doc)
    norm = rule_result["normalized"]
    errors = rule_result["errors"]
    warnings = rule_result["warnings"]

    # 2) Нормативы
    norm_issues = apply_normative_checks(doc, rule_result, args.norms)
    errors.extend(norm_issues.get("errors", []))
    warnings.extend(norm_issues.get("warnings", []))

    # 3) LLM — общий проход (если не отключен)
    llm_block = {"used": False, "note": "LLM step skipped"}
    if not args.skip_llm:
        hc = ollama_healthcheck(args.ollama_url)
        if not hc.get("ok"):
            llm_block = {"used": False, "error": f"Ollama недоступен: {hc.get('error', 'unknown')}", "url": args.ollama_url}
        else:
            avail = set(hc.get("models") or [])
            def _model_match(req, models):
                if req in models: return True
                if ":" not in req:
                    return any((m.split(":",1)[0] == req) for m in models)
                return False
            if not _model_match(args.model, avail):
                llm_block = {"used": False,
                             "error": f"Модель '{args.model}' не найдена на сервере",
                             "available": sorted(list(avail)),
                             "hint": "Используйте --model '" + next((m for m in avail if m.split(':',1)[0]==args.model), (args.model+':latest')) + "' или выполните: ollama pull " + args.model}
            else:
                retries = max(0, args.llm_retries)
                last_err = None
                for attempt in range(retries + 1):
                    try:
                        cap = max(10, int(args.llm_max_paragraphs * (0.6 if attempt else 1.0)))
                        payload = make_llm_payload(args.model, doc, norm, max_paragraphs=cap, num_ctx=args.llm_num_ctx, num_predict=args.llm_num_predict)
                        llm_out = ask_ollama(args.model, payload, args.ollama_url, timeout_s=args.llm_timeout)
                        if isinstance(llm_out, dict):
                            if "raw" in llm_out and isinstance(llm_out["raw"], str):
                                coerced = coerce_llm_to_json(llm_out["raw"]) or {}
                                errors.extend(coerced.get("errors") or [])
                                warnings.extend(coerced.get("warnings") or [])
                                llm_block = {"used": True, "raw": llm_out, "coerced": coerced, "attempts": attempt + 1}
                            else:
                                errors.extend(llm_out.get("errors") or [])
                                warnings.extend(llm_out.get("warnings") or [])
                                llm_block = {"used": True, "raw": llm_out, "attempts": attempt + 1}
                        else:
                            llm_block = {"used": True, "raw": {"note": "unexpected response type"}, "attempts": attempt + 1}
                        last_err = None
                        break
                    except Exception as e:
                        last_err = str(e)
                if last_err:
                    llm_block = {"used": False, "error": last_err, "attempts": retries + 1}

    # 3b) LLM по разделам (если задано)
    llm_sections_block = None
    if args.llm_sections:
        llm_sections_block = run_llm_sections(doc, norm, args)

    # Итоговый отчёт (JSON)
    report = {
        "source_file": doc.get("file"),
        "normalized_fields": norm,
        "errors": errors,
        "warnings": warnings,
        "llm": llm_block,
        "llm_sections": llm_sections_block
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # Markdown-отчет
    if args.md:
        md_lines = [
            "# Отчет проверки документа", "",
            f"**Файл**: {report.get('source_file') or '-'}", "",
            "## Нормализованные поля",
            "```json",
            json.dumps(report["normalized_fields"], ensure_ascii=False, indent=2),
            "```",
            "## Ошибки"
        ]
        if report["errors"]:
            for e in report["errors"]:
                fld = e.get('field') or e.get('fields')
                md_lines.append(f"- **{e.get('code')}**: {e.get('msg')} ({fld})")
        else:
            md_lines.append("- Нет ошибок")
        md_lines.append("")
        md_lines.append("## Предупреждения")
        if report["warnings"]:
            for w in report["warnings"]:
                fld = w.get('field') or w.get('fields')
                md_lines.append(f"- **{w.get('code')}**: {w.get('msg')} ({fld})")
        else:
            md_lines.append("- Нет предупреждений")

        if report.get("llm",{}).get("used"):
            md_lines.append("")
            md_lines.append("## Результат LLM (общий проход)")
            md_lines.append("```json")
            md_lines.append(json.dumps(report["llm"].get("raw"), ensure_ascii=False, indent=2))
            if report["llm"].get("coerced"):
                md_lines.append("\n# Coerced JSON\n")
                md_lines.append(json.dumps(report["llm"]["coerced"], ensure_ascii=False, indent=2))
            md_lines.append("```")

        # LLM по разделам
        md_lines.append("")
        md_lines.append("## LLM по разделам (если указаны)")
        if report.get("llm_sections") and report["llm_sections"].get("used"):
            for sec in (report["llm_sections"].get("sections") or []):
                md_lines.append(f"### Раздел: {sec.get('title')} ({sec.get('section_id')})")
                md_lines.append("```json")
                md_lines.append(json.dumps(sec, ensure_ascii=False, indent=2))
                md_lines.append("```")
        else:
            md_lines.append("- Разделы не запрашивались или сервис недоступен.")

        with open(args.md, "w", encoding="utf-8") as f:
            f.write("\n".join(md_lines))

        # Дополнительно: прогнать готовый MD через LLM и получить краткое резюме
        if args.llm_summarize_md:
            try:
                with open(args.md, "r", encoding="utf-8") as fmd:
                    md_text = fmd.read()
                md_summary_block = summarize_md_with_llm(md_text, args)
                if md_summary_block.get("used"):
                    with open(args.md, "a", encoding="utf-8") as fmd:
                        fmd.write("\n\n## Краткое резюме (LLM)\n\n")
                        fmd.write(md_summary_block["summary"] + "\n")
                    summary_path = write_summary_files(args.md, md_summary_block["summary"], args.summary_out)
                    report["llm_md_summary"] = {"used": True, "summary_file": summary_path}
                else:
                    report["llm_md_summary"] = {"used": False, "error": md_summary_block.get("error")}
            except Exception as e:
                report["llm_md_summary"] = {"used": False, "error": str(e)}

            # Обновим JSON, чтобы включить блок резюме
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"OK -> {args.out}" + (f" ; {args.md}" if args.md else ""))

if __name__ == "__main__":
    main()
