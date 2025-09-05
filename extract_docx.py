
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_docx.py — извлечение данных из .docx БЕЗ внешних зависимостей.
- Тянет параграфы и таблицы из word/document.xml (+ header*/footer*).
- Строит JSON с параграфами, таблицами, ключ-значениями, датами, диагнозами, витальными показателями.
Запуск:
  python extract_docx.py /path/to/file.docx --out out.json
"""
import argparse, json, re, zipfile
from xml.etree import ElementTree as ET
from typing import List, Dict, Any

NS = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}

def _text_from(el) -> str:
    # Собираем весь текст из w:t
    parts = []
    for t in el.findall('.//w:t', NS):
        parts.append(t.text or '')
    # Учитываем разрывы строк w:br
    brs = el.findall('.//w:br', NS)
    text = ''.join(parts)
    if brs and parts:
        # упрощенно, заменим <w:br/> на \n между runs — не идеально, но помогает
        text = re.sub(r'(?<!\n)(\s{2,})', '\n', text)
    return text.strip()

def parse_paragraphs(root) -> List[str]:
    paras = []
    for p in root.findall('.//w:p', NS):
        txt = _text_from(p)
        if txt:
            paras.append(txt)
    return paras

def parse_tables(root) -> List[List[List[str]]]:
    all_tables = []
    for tbl in root.findall('.//w:tbl', NS):
        table = []
        for tr in tbl.findall('.//w:tr', NS):
            row = []
            # В merged ячейках Word дублирует текст в первой ячейке — берем как есть
            for tc in tr.findall('.//w:tc', NS):
                row.append(_text_from(tc))
            if row:
                table.append(row)
        if table:
            all_tables.append(table)
    return all_tables

def load_xml_from_docx(zf: zipfile.ZipFile, name: str):
    try:
        data = zf.read(name)
    except KeyError:
        return None
    return ET.fromstring(data)

def extract_all(docx_path: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "file": docx_path,
        "paragraphs": [],
        "tables": [],
        "headers": [],
        "footers": [],
        "key_values": {},
        "vitals": [],
        "dates": [],
        "diagnoses": []
    }
    with zipfile.ZipFile(docx_path, 'r') as zf:
        # Основной документ
        main = load_xml_from_docx(zf, 'word/document.xml')
        if main is not None:
            result["paragraphs"] = parse_paragraphs(main)
            result["tables"] = parse_tables(main)

        # Хедеры/футеры (если есть)
        for name in zf.namelist():
            if re.match(r'word/header\d*\.xml$', name):
                h = load_xml_from_docx(zf, name)
                if h is not None:
                    result["headers"].extend(parse_paragraphs(h))
            if re.match(r'word/footer\d*\.xml$', name):
                f = load_xml_from_docx(zf, name)
                if f is not None:
                    result["footers"].extend(parse_paragraphs(f))

    # Выделим key-values из таблиц (простые пары)
    kv = {}
    for table in result["tables"]:
        for row in table:
            # 2 колонки — частый кейс "ключ | значение"
            if len(row) == 2:
                k, v = row[0].strip(' :\u00A0'), row[1].strip()
                if k and v and k not in kv:
                    kv[k] = v
            # Четное число колонок <= 8 — парсим попарно
            elif len(row) % 2 == 0 and len(row) <= 8:
                for i in range(0, len(row), 2):
                    k, v = row[i].strip(' :\u00A0'), row[i+1].strip()
                    if k and v and k not in kv:
                        kv[k] = v
    # Доп. пары из параграфов вида "Ключ: значение"
    for para in result["paragraphs"]:
        m = re.match(r'^\s*([\w\s.,«»"()/-]{2,80})\s*[:\-–]\s*(.+)$', para)
        if m:
            k = m.group(1).strip(' :\u00A0')
            v = m.group(2).strip()
            if k and v and k not in kv:
                kv[k] = v
    result["key_values"] = kv

    # Вытянем даты/время
    text_blob = "\n".join(result["paragraphs"])
    date_pat = re.compile(r'\b(\d{2}\.\d{2}\.\d{4})\b|\b(\d{4}\.\d{2}\.\d{2})\b')
    time_pat = re.compile(r'\b([01]?\d|2[0-3]):([0-5]\d)\b')
    dates = set()
    for m in date_pat.finditer(text_blob):
        dates.add(next(g for g in m.groups() if g))
    # Дополнительно вытянем сочетания "дата время" из встреч
    for dm in re.finditer(r'(\d{2}\.\d{2}\.\d{4})\s+([01]?\d|2[0-3]):([0-5]\d)', text_blob):
        dates.add(f"{dm.group(1)} {dm.group(2)}:{dm.group(3)}")
    result["dates"] = sorted(dates)

    # Диагнозы (ключевые слова или коды МКБ)
    diagnoses = set()
    icd_pat = re.compile(r'\(([A-Z]\d{2}\.\d)\)')
    for para in result["paragraphs"]:
        if 'Диагноз' in para or 'диагноз' in para or icd_pat.search(para):
            # сохраним полноценные строки-носители
            diagnoses.add(para)
    result["diagnoses"] = sorted(diagnoses)

    # Витальные показатели: АД, Пульс/ЧСС, Темп (Т/Т°, Температура), ЧДД, SpO2/Сат
    vitals = []
    vit_patts = [
        (re.compile(r'\bАД\s*(\d{2,3})\s*/\s*(\d{2,3})'), lambda m: {"bp_sys": int(m.group(1)), "bp_dia": int(m.group(2))}),
        (re.compile(r'\bСАД\s*(\d{2,3})'), lambda m: {"bp_sys": int(m.group(1))}),
        (re.compile(r'\bДАД\s*(\d{2,3})'), lambda m: {"bp_dia": int(m.group(1))}),
        (re.compile(r'\b(?:Пульс|ЧСС)\s*[:\s]*([0-9]{2,3})'), lambda m: {"pulse": int(m.group(1))}),
        (re.compile(r'\b(?:ЧДД)\s*[:\s]*([0-9]{1,2})'), lambda m: {"rr": int(m.group(1))}),
        (re.compile(r'\b(?:Т|Т[°]|Температура)\s*[:\s]*([0-9]{2}(?:[.,]\d)?)'), lambda m: {"temp_c": float(m.group(1).replace(",", "."))}),
        (re.compile(r'\b(?:Сат|SpO2)\s*[:\s]*([0-9]{2,3})\s*%'), lambda m: {"spo2": int(m.group(1))}),
    ]
    for para in result["paragraphs"]:
        found = {}
        for patt, make in vit_patts:
            for m in patt.finditer(para):
                found.update(make(m))
        if found:
            found["source"] = para
            vitals.append(found)
    result["vitals"] = vitals

    return result

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("docx", help="Путь к .docx")
    ap.add_argument("--out", help="Куда сохранить JSON", default="out.json")
    args = ap.parse_args()
    data = extract_all(args.docx)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"OK -> {args.out}")

if __name__ == "__main__":
    main()
