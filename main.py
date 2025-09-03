#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
med_parse_llm.py — конвертер медицинских документов (.docx) в единый JSON-кейс
с использованием ТОЛЬКО локальной нейросети (LLM) для извлечения фактов.

• Платформы: macOS, Linux
• Зависимости: python-docx, requests  (pip install python-docx requests)
• LLM: локальный сервер (например, Ollama) на http://localhost:11434
        модель по умолчанию — deepseek-v3 (параметризуется флагом)

Запуск (пример):
  python med_parse_llm.py \
    --case-id KZ-DEMO-1 \
    --out case.json \
    --model deepseek-v3 \
    /mnt/data/инфеция.docx /mnt/data/кесарево род дом.docx

Скрипт НЕ использует регулярки/правила для извлечения медицинских фактов.
Вся семантика берётся из вывода LLM (zero/few-shot). Мы лишь выравниваем
вывод модели в стандартный JSON и сохраняем цитаты как evidence.
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import hashlib
from typing import Any, Dict, List, Optional
from docx import Document 
try:
    import requests  # локальный HTTP к LLM
except Exception as e:
    print("[!] Требуется пакет requests. Установите: pip install requests", file=sys.stderr)
    raise


# ----------------------------- CLI -----------------------------

def parse_args():
    ap = argparse.ArgumentParser(description="DOCX → JSON через локальную нейросеть")
    ap.add_argument("inputs", nargs="+", help="Пути к .docx (один кейс — несколько файлов)")
    ap.add_argument("--case-id", required=True, help="Идентификатор кейса")
    ap.add_argument("--out", required=True, help="Путь к выходному JSON")
    ap.add_argument("--locale", default="ru-KZ", help="Локаль кейса (ru-KZ/kk-KZ)")
    ap.add_argument("--llm-url", default="http://localhost:11434", help="База URL локальной LLM (например, Ollama)")
    ap.add_argument("--model", default="deepseek-v3", help="Имя локальной модели (например: deepseek-v3, deepseek-r1)")
    ap.add_argument("--threshold", type=float, default=0.80, help="Порог confidence фактов от LLM (0..1)")
    return ap.parse_args()

# ----------------------------- Вспомогательные -----------------------------

def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()

# Вытащим из .docx текст и простое представление таблиц (как текст)

def read_docx_as_text(path: str) -> Dict[str, Any]:
    doc = Document(path)
    paragraphs: List[str] = []
    for p in doc.paragraphs:
        t = p.text.strip()
        if t:
            paragraphs.append(t)

    tables_txt: List[str] = []
    for tb in doc.tables:
        rows_txt: List[str] = []
        for r in tb.rows:
            cells = [" ".join(c.text.split()) for c in r.cells]
            rows_txt.append(" | ".join(cells))
        tables_txt.append("\n".join(rows_txt))

    full_text = "\n".join(paragraphs)
    if tables_txt:
        full_text += "\n\n[TABLES]\n" + "\n\n---\n\n".join(tables_txt)

    return {
        "text": full_text,
        "tables_count": len(doc.tables)
    }

# ----------------------------- Промпт для LLM -----------------------------

def build_prompt(doc_text: str) -> str:
    """Инструкция для локальной модели: вернуть ТОЛЬКО JSON по заданной схеме."""
    schema = r"""
{
  "diagnoses": [
    {"code": "A09", "label": "string|null", "date": "YYYY-MM-DD|null", "confidence": 0.0-1.0, "quote": "..."}
  ],
  "consents": [
    {"kind": "surgery|anesthesia|procedure", "signed_dt": "YYYY-MM-DD|null", "confidence": 0.0-1.0, "quote": "..."}
  ],
  "anesthesia": [
    {"asa": "I|II|III|IV|V|null", "dt": "YYYY-MM-DD|null", "confidence": 0.0-1.0, "quote": "..."}
  ],
  "procedures": [
    {"type": "surgery|delivery_cs|delivery_vaginal|biopsy|puncture|endoscopy", "code": "string|null", "dt": "YYYY-MM-DD|null", "confidence": 0.0-1.0, "quote": "..."}
  ],
  "infection_control": [
    {"mode": "isolation|cohort|ppe|sanitation", "note": "string|null", "dt": "YYYY-MM-DD|null", "confidence": 0.0-1.0, "quote": "..."}
  ],
  "newborn": [
    {"apgar_1": 0-10|null, "apgar_5": 0-10|null, "apgar_10": 0-10|null, "breastfeeding_time": "string|null", "confidence": 0.0-1.0, "quote": "..."}
  ],
  "orders": [
    {"kind": "medication", "drug": "string|null", "dose": "string|null", "route": "string|null", "freq": "string|null", "period_start": "YYYY-MM-DD|null", "period_end": "YYYY-MM-DD|null", "confidence": 0.0-1.0, "quote": "..."}
  ],
  "vitals_daily": [
    {"date": "YYYY-MM-DD|null", "temp": "string|null", "pulse": "string|null", "bp_sys": "int|null", "bp_dia": "int|null", "confidence": 0.0-1.0, "quote": "..."}
  ]
}
"""
    instruction = (
        "Ты — система извлечения фактов из медицинских документов на русском/казахском. "
        "Верни ТОЛЬКО JSON строго по схеме ниже, без пояснений и текста вокруг. "
        "Если чего-то нет — опусти соответствующий блок или поставь null.\n\n"
        "СХЕМА JSON:\n" + schema + "\n" 
        "ТЕКСТ ДОКУМЕНТА НИЖЕ. Дай краткие точные цитаты 'quote' для каждого факта.\n"
        "\n<<<\n" + doc_text + "\n>>>\n"
    )
    return instruction

# ----------------------------- Вызов локальной LLM -----------------------------

def call_llm(llm_url: str, model: str, prompt: str) -> str:
    """Вызывает локальную LLM (совместимую с Ollama), возвращает текст ответа."""
    url = llm_url.rstrip("/") + "/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,

        "format": "json",
"options": { "num_ctx": 8192, "temperature": 0.2 }
    }
    resp = requests.post(url, json=payload, timeout=600)
    resp.raise_for_status()
    data = resp.json()
    # Ollama возвращает {'response': '...'}
    return data.get("response", "")

# Вытаскиваем JSON-блок из ответа (если модель прислала лишний текст)

def extract_json_block(s: str) -> str:
    s = s.strip()
    # если уже чистый JSON
    if s.startswith("{") and s.endswith("}"):
        return s
    # ищем первый '{' и последний '}'
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end > start:
        return s[start:end+1]
    # fallback: пустой минимальный JSON
    return "{}"

# ----------------------------- Сборка единого кейса -----------------------------

def empty_case(case_id: str, locale: str) -> Dict[str, Any]:
    return {
        "schema_version": "1.0",
        "case_id": case_id,
        "locale": locale,
        "normative_set": [],
        "patient": {"iin": None, "name": None, "dob": None, "sex": None},
        "encounter": {
            "care_setting": None,
            "department": None,
            "admit_dt": None,
            "discharge_dt": None,
            "links": {"mother_case_id": None, "newborn_case_id": None}
        },
        "documents": [],
        "facts": {
            "diagnoses": [],
            "procedures": [],
            "anesthesia": [],
            "consents": [],
            "orders": [],
            "administrations": [],
            "vitals_daily": [],
            "newborn": [],
            "vaccinations": [],
            "infection_control": []
        },
        "rule_results": [],
        "evidence_index": []
    }

# Объединяем факты одного файла в общий кейс с фильтром confidence

def merge_facts(case: Dict[str, Any], facts: Dict[str, Any], doc_id: str, model: str, thr: float):
    evid = case["evidence_index"]

    def add_with_evidence(path: List[str], item: Dict[str, Any], quote: Optional[str], conf: float):
        ev_id = f"ev_{len(evid)+1:06d}"
        evid.append({
            "evidence_id": ev_id,
            "doc_id": doc_id,
            "loc": {},
            "snippet": (quote or "")[:500],
            "confidence": round(float(conf), 3),
            "source_model": model
        })
        item = dict(item)
        item.setdefault("evidence", []).append(ev_id)
        # аккуратно положим item в case["facts"][path[-1]]
        cur = case["facts"]
        for key in path[:-1]:
            cur = cur[key]
        cur[path[-1]].append(item)

    # Diagnoses
    for d in facts.get("diagnoses", []) or []:
        conf = float(d.get("confidence", 0))
        if conf >= thr and d.get("code"):
            add_with_evidence(["diagnoses"], {"code": d.get("code"), "label": d.get("label"), "dt": d.get("date")}, d.get("quote"), conf)

    # Consents
    for c in facts.get("consents", []) or []:
        conf = float(c.get("confidence", 0))
        if conf >= thr and c.get("kind"):
            add_with_evidence(["consents"], {"kind": c.get("kind"), "signed_dt": c.get("signed_dt")}, c.get("quote"), conf)

    # Anesthesia
    for a in facts.get("anesthesia", []) or []:
        conf = float(a.get("confidence", 0))
        if conf >= thr and (a.get("asa") or a.get("dt")):
            add_with_evidence(["anesthesia"], {"asa": a.get("asa"), "dt": a.get("dt")}, a.get("quote"), conf)

    # Procedures
    for p in facts.get("procedures", []) or []:
        conf = float(p.get("confidence", 0))
        if conf >= thr and p.get("type"):
            add_with_evidence(["procedures"], {"type": p.get("type"), "code": p.get("code"), "dt": p.get("dt")}, p.get("quote"), conf)

    # Infection control
    for ic in facts.get("infection_control", []) or []:
        conf = float(ic.get("confidence", 0))
        if conf >= thr and ic.get("mode"):
            add_with_evidence(["infection_control"], {"mode": ic.get("mode"), "note": ic.get("note"), "dt": ic.get("dt")}, ic.get("quote"), conf)

    # Newborn
    for nb in facts.get("newborn", []) or []:
        conf = float(nb.get("confidence", 0))
        if conf >= thr:
            add_with_evidence(["newborn"], {
                "apgar_1": nb.get("apgar_1"),
                "apgar_5": nb.get("apgar_5"),
                "apgar_10": nb.get("apgar_10"),
                "breastfeeding_time": nb.get("breastfeeding_time")
            }, nb.get("quote"), conf)

    # Orders
    for od in facts.get("orders", []) or []:
        conf = float(od.get("confidence", 0))
        if conf >= thr and (od.get("drug") or od.get("period_start")):
            add_with_evidence(["orders"], {
                "kind": "medication",
                "drug": od.get("drug"),
                "dose": od.get("dose"),
                "route": od.get("route"),
                "freq": od.get("freq"),
                "period": [od.get("period_start"), od.get("period_end")]
            }, od.get("quote"), conf)

    # Vitals daily
    for vt in facts.get("vitals_daily", []) or []:
        conf = float(vt.get("confidence", 0))
        if conf >= thr and any(vt.get(k) for k in ("date","temp","pulse","bp_sys","bp_dia")):
            add_with_evidence(["vitals_daily"], {
                "date": vt.get("date"),
                "temp": vt.get("temp"),
                "pulse": vt.get("pulse"),
                "bp_sys": vt.get("bp_sys"),
                "bp_dia": vt.get("bp_dia")
            }, vt.get("quote"), conf)

# ----------------------------- Основной сценарий -----------------------------

def main():
    args = parse_args()

    case = empty_case(args.case_id, args.locale)

    for path in args.inputs:
        if not os.path.isfile(path) or not path.lower().endswith('.docx'):
            print(f"[skip] не docx: {path}", file=sys.stderr)
            continue

        # Регистрируем документ в списке case["documents"]
        doc_id = f"doc_{len(case['documents'])+1:06d}"
        case["documents"].append({
            "doc_id": doc_id,
            "type": "generic",
            "created_dt": None,
            "source": {"filename": os.path.basename(path), "hash": file_sha256(path)},
            "lang": "ru",
            "text_ref": f"raw://{doc_id}",
            "pages": 1
        })

        # 1) Получаем текстовое представление DOCX
        bundle = read_docx_as_text(path)
        text_blob = bundle["text"]

        # 2) Готовим промпт и вызываем локальную модель
        prompt = build_prompt(text_blob)
        try:
            llm_resp = call_llm(args.llm_url, args.model, prompt)
        except Exception as e:
            print(f"[llm] ошибка запроса: {e}", file=sys.stderr)
            continue

        # 3) Извлекаем JSON-факты из ответа модели
        json_block = extract_json_block(llm_resp)
        try:
            facts = json.loads(json_block) if json_block.strip() else {}
        except Exception as e:
            print("[llm] не удалось распарсить JSON из ответа — сохраняю сырой ответ в evidence", file=sys.stderr)
            # как fallback — сохраним весь ответ как evidence-запись
            case["evidence_index"].append({
                "evidence_id": f"ev_{len(case['evidence_index'])+1:06d}",
                "doc_id": doc_id,
                "loc": {},
                "snippet": llm_resp[:500],
                "confidence": 0.0,
                "source_model": args.model
            })
            continue

        # 4) Мёрджим факты в единый кейс с порогом confidence
        if isinstance(facts, dict):
            merge_facts(case, facts, doc_id, args.model, args.threshold)
        else:
            print("[llm] Ответ модели не объект JSON — пропуск", file=sys.stderr)

    # Запись результата
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(case, f, ensure_ascii=False, indent=2)
    print(f"[ok] Сохранено: {args.out}")


if __name__ == "__main__":
    main()
