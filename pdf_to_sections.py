#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pdf_to_sections.py — принимает PDF, извлекает текст (с опциональным OCR) и режет на разделы.
Выход — JSON с массивами блоков по каждому найденному заголовку.

Пример:
    python pdf_to_sections.py input.pdf output.json
    # c авто-попыткой OCR, если текста мало и установлен ocrmypdf:
    python pdf_to_sections.py input.pdf output.json --auto-ocr --ocr-lang rus+eng
    # принудительный OCR:
    python pdf_to_sections.py input.pdf output.json --force-ocr --ocr-lang rus+eng
"""

import argparse, re, json, sys, os, shutil, subprocess, tempfile, pathlib

# -------------------- Извлечение текста из PDF --------------------

def extract_text_from_pdf(pdf_path: str) -> str:
    # 1) PyMuPDF
    try:
        import fitz  # PyMuPDF
        parts = []
        with fitz.open(pdf_path) as doc:
            for page in doc:
                parts.append(page.get_text("text"))
        return "\n".join(parts)
    except Exception:
        pass

    # 2) pypdf
    try:
        from pypdf import PdfReader
        reader = PdfReader(str(pdf_path))
        parts = []
        for page in reader.pages:
            txt = page.extract_text() or ""
            parts.append(txt)
        return "\n".join(parts)
    except Exception:
        pass

    # 3) pdfminer.six
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
        return pdfminer_extract(str(pdf_path))
    except Exception:
        return ""

def ocr_and_extract(pdf_path: str, langs: str = "rus+eng") -> str:
    """Использует ocrmypdf, затем извлекает текст любым доступным способом."""
    if shutil.which("ocrmypdf") is None:
        raise RuntimeError("ocrmypdf не найден в системе. Установите его либо уберите флаг OCR.")
    with tempfile.TemporaryDirectory() as td:
        out_pdf = os.path.join(td, "ocr.pdf")
        cmd = [
            "ocrmypdf",
            "-l", langs,
            "--rotate-pages", "--deskew",
            "--force-ocr",
            pdf_path, out_pdf
        ]
        # скрываем шум
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return extract_text_from_pdf(out_pdf)

# -------------------- Разделение на секции --------------------

def normalize(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text

def compile_specs():
    """Список (ключ, regex) — дополняйте под свои формы."""
    return [
        # ФОРМА № 001/У (и /у, /е), допускаем вариативности
        ("ФОРМА_001У",
         r"(?im)^\s*(?:медицинская\s+документация\s*)?форма\s*№\s*0*01\s*/\s*[уеe]\b"),
        # Лист врачебных назначений (без уточнения «лекарственных средств»)
        ("ЛИСТ_ВРАЧЕБНЫХ_НАЗНАЧЕНИЙ",
         r"(?im)^\s*лист\s+врачебных\s+назначений\b(?!.*лекарствен)"),
        # Лист врачебных назначений лекарственных средств
        ("ЛИСТ_НАЗНАЧЕНИЙ_ЛЕКАРСТВЕННЫХ_СРЕДСТВ",
         r"(?im)^\s*лист\s+врачебных\s+назначений.*лекарствен\w*\s+средств\b"),
        # Лист назначений на исследование/исследования
        ("ЛИСТ_НАЗНАЧЕНИЙ_НА_ИССЛЕДОВАНИЯ",
         r"(?im)^\s*лист\s+назначени[йя]\s+на\s+исследовани[ея]\b"),
        # Лист назначений на консультации
        ("ЛИСТ_НАЗНАЧЕНИЙ_НА_КОНСУЛЬТАЦИИ",
         r"(?im)^\s*лист\s+назначени[йя]\s+на\s+консультац\w*\b"),
        # Титул/обложка (каз/рус)
        ("СТАЦИОНАРНАЯ_КАРТА_ТИТУЛ",
         r"(?im)^\s*\"?стационарл[ыі]қ\s+науқастың\s+медицинал[ыі]қ\s+картасы\"?\b|^\s*\"?стационарная\s+карта\s+больного\"?\b"),
    ]

def split_sections(text: str, specs):
    text = normalize(text)
    matches = []
    for key, pat in specs:
        for m in re.finditer(pat, text):
            matches.append((m.start(), m.end(), key, m.group(0)))
    matches.sort(key=lambda x: x[0])
    if not matches:
        return {}
    sections = {}
    bounds = matches + [(len(text), len(text), "_END_", "")]
    for i, (s, e, key, label) in enumerate(matches):
        nxt = bounds[i+1][0]
        chunk = text[s:nxt].strip("\n")
        sections.setdefault(key, []).append(chunk)
    return sections

# -------------------- CLI --------------------

def main():
    ap = argparse.ArgumentParser(description="PDF → JSON секции по медицинским заголовкам.")
    ap.add_argument("pdf", help="Входной PDF файл")
    ap.add_argument("out", help="Выходной JSON")
    ap.add_argument("--min-chars", type=int, default=500,
                    help="Минимум символов для признания извлечения успешным (по текстовому слою).")
    ap.add_argument("--force-ocr", action="store_true",
                    help="Принудительно выполнить OCR через ocrmypdf.")
    ap.add_argument("--auto-ocr", action="store_true",
                    help="Автоматически пробовать OCR, если текстового слоя мало (< --min-chars).")
    ap.add_argument("--ocr-lang", default="rus+eng",
                    help="Языки OCR для tesseract/ocrmypdf, например: rus+eng")
    args = ap.parse_args()

    pdf_path = args.pdf
    out_path = args.out

    text = ""

    if args.force_ocr:
        text = ocr_and_extract(pdf_path, args.ocr_lang)
    else:
        text = extract_text_from_pdf(pdf_path)
        if args.auto_ocr and len(text.strip()) < args.min_chars:
            try:
                text = ocr_and_extract(pdf_path, args.ocr_lang)
            except Exception as e:
                # не фейлим пайплайн — просто продолжаем с тем что есть
                sys.stderr.write(f"[auto-ocr] пропущен: {e}\n")

    specs = compile_specs()
    sections = split_sections(text, specs)

    # Метаданные
    result = {
        "source_pdf": os.path.abspath(pdf_path),
        "extracted_chars": len(text),
        "sections": sections,
        "keys": list(sections.keys())
    }

    pathlib.Path(out_path).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK: извлечено символов={len(text)}; найдено ключей={len(sections)} → {out_path}")

if __name__ == "__main__":
    main()
