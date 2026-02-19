#!/usr/bin/env python3
"""
ETL Pipeline: PDF Manuals -> Markdown Chunks (v2 - Page-Aware)

Processa manuais tecnicos de comissarios de voo (GOL, LATAM, Azul)
e gera arquivos .md otimizados para aplicacao de simulados Q&A.

Estrategia:
  1. Extrai texto pagina a pagina (PyMuPDF)
  2. Detecta capitulo de cada pagina via codigos MCmsV-{cap}-{pag}
     ou headers "Chapter N:" presentes no rodape/cabecalho
  3. Limpa ruido agressivamente (cabecalhos, rodapes, TOC, datas)
  4. Agrupa paginas limpas por capitulo
  5. Gera .md com frontmatter YAML por capitulo

Uso:
    python etl_pdf_to_md.py --input data/pdfs/azul/ --airline azul
    python etl_pdf_to_md.py --input data/pdfs/ --all
    python etl_pdf_to_md.py --input manual.pdf --airline gol --engine marker
"""

import re
import sys
import os
import argparse
import unicodedata
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

# Forca UTF-8 no stdout/stderr (Windows)
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    os.environ.setdefault("PYTHONUTF8", "1")

# ---------------------------------------------------------------------------
# Configuracao por companhia
# ---------------------------------------------------------------------------

AIRLINE_CONFIG = {
    "gol": {
        "codename": "Oragen",
        "output_dir": "oragen",
    },
    "latam": {
        "codename": "Red",
        "output_dir": "red",
    },
    "azul": {
        "codename": "Blue",
        "output_dir": "blue",
    },
}

# ---------------------------------------------------------------------------
# Padroes de ruido (comuns a todos os manuais MCMSV)
# ---------------------------------------------------------------------------

NOISE_LINE_PATTERNS = [
    # === AZUL (Blue) ===
    # Codigos de pagina: MCmsV-2-28, MCMS-19, MCMSV-38
    r"^MCmsV?-?\d+[-\d]*$",
    r"^MCMS-?\d+$",
    r"^MCMSV?-?\d+[-\d]*$",
    # Titulo repetido do manual (Azul)
    r"^MANUAL DE COMISS.RIOS DE VOO\s*\(?MCMSV\)?.*$",
    # Codigo do documento (Azul)
    r"^C.DIGO:\s*M-OPS-\d+.*$",
    # Chapter/Section headers Azul (repeticao no rodape)
    r"^Chapter\s+\d+\s*:.*$",
    r"^Section\s*:.*$",
    # Navegacao Azul
    r"^Voltar para o .ndice$",
    r"^Gloss.rio$",

    # === GOL (Oragen) ===
    # Titulo repetido GOL
    r"(?i)^MANUAL DO COMISS.RIO DE VOO$",
    # Paginacao GOL: "Pág:1-4", "Pag:3-30"
    r"(?i)^P.g:\d+-\d+$",
    # Referencia documento GOL
    r"^DR-CAB-TC-\d+.*Manual do Comiss.rio de Voo.*$",
    # Secao no header GOL (linha isolada com nome da secao)
    # Handled separately - not noise, used for detection
    # Revisao GOL: "Rev 10.00"
    r"(?i)^Rev\s+\d+\.\d+$",
    # Disclaimer GOL (wraps across 2 lines)
    r"(?i)^USU.RIO\s*-\s*Fora do ambiente SGED.*$",
    r"(?i)^este documento$",
    # LEP GOL
    r"(?i)^LEP$",
    r"(?i)^Sum.rio Geral$",

    # === LATAM (Red) ===
    # Titulo repetido LATAM
    r"(?i)^Manual do MCmsV Comiss.rio de Voo$",
    # Mes/Ano LATAM: "Julho/2025", "Janeiro/2025"
    r"(?i)^(?:Janeiro|Fevereiro|Mar.o|Abril|Maio|Junho|Julho|Agosto|Setembro|Outubro|Novembro|Dezembro)/\d{4}$",
    # Revisao LATAM: "Revisao 14.01"
    r"(?i)^Revis.o\s+\d+\.\d+$",

    # === GOL / COMUNS: Sumario interno com tracejados ===
    # "7.1 GENERALIDADES - - - - - 7-3" ou "SUMÁRIO 7 PROCEDIMENTOS..."
    r"^.*- - - - .*$",
    # Titulo de secao repetido isolado (GOL header echo)
    # e.g. "Procedimentos de Emergência" sozinho numa linha, sem numero de secao
    r"(?i)^Generalidades$",
    r"(?i)^Normas e Procedimentos$",
    r"(?i)^Padroniza\S+o Operacional$",
    r"(?i)^Equipamentos de Emerg\S+ncia$",
    r"(?i)^Procedimentos de Emerg\S+ncia$",
    r"(?i)^Procedimentos Operacionais$",
    r"(?i)^Sobreviv\S+ncia$",
    r"(?i)^Primeiros Socorros$",
    r"(?i)^Artigos Perigosos$",
    r"(?i)^Security$",
    r"(?i)^Safety$",
    # GOL "SUMÁRIO" header line
    r"(?i)^SUM.RIO$",
    # GOL chapter number header: "7  PROCEDIMENTOS DE EMERGÊNCIA"
    r"^\d{1,2}\s{2,}\S.*$",

    # === COMUNS ===
    # Datas isoladas (dd/mm/yyyy)
    r"^\d{2}/\d{2}/\d{4}$",
    # Pagina em branco
    r"(?i)^P.GINA INTENCIONALMENTE EM BRANCO$",
    # Lista de paginas efetivas
    r"^LISTA DE P.GINAS EFETIVAS.*$",
    # Documento nao controlado
    r"(?i)^documento\s+n.o\s+controlado.*$",
    # Confidencial / Uso interno
    r"(?i)^confidencial$",
    r"(?i)^uso\s+interno$",
    r"(?i)^propriet.rio$",
    r"(?i)^c.pia\s+controlada$",
    # Linhas com apenas numeros (paginas)
    r"^\d{1,4}$",
    # "Pagina X de Y"
    r"(?i)^p.gina\s+\d+\s+de\s+\d+$",
    # Revisao generica: "Revisão: 12", "Revisão: 12.00", "Revisão 3"
    r"(?i)^revis.o\s*:?\s*\d{1,3}(\.\d+)?$",
    r"(?i)^data\s+de\s+revis.o\s*:?\s*\d{2}[/\-]\d{2}[/\-]\d{2,4}$",
    # "Cap. N:" header repetido no corpo (Azul rodape)
    r"(?i)^Cap\.\s*\d+\s*:.*$",
    # Nomes das companhias (linhas soltas)
    r"(?i)^gol\s+linhas\s+a.reas.*$",
    r"(?i)^latam\s+(airlines?|brasil).*$",
    r"(?i)^azul\s+linhas\s+a.reas.*$",
    # Referencia ao manual (isolada)
    r"(?i)^manual\s+de\s+comiss.rios?(\s+de\s+voo)?(\s*[-\u2013(].*)?$",
    # Linhas "Pagina Revisao Data"
    r"(?i)^P.gina\s+Revis.o\s+Data$",
]

NOISE_LINE_RE = [re.compile(p, re.IGNORECASE) for p in NOISE_LINE_PATTERNS]


# ---------------------------------------------------------------------------
# Modelos de dados
# ---------------------------------------------------------------------------

@dataclass
class PageInfo:
    page_num: int
    raw_text: str
    chapter_num: int = -1
    chapter_title: str = ""
    is_toc: bool = False
    is_front_matter: bool = False
    tables_md: list = field(default_factory=list)


@dataclass
class Chapter:
    number: int
    title: str
    content: str = ""
    page_start: int = 0
    page_end: int = 0
    section_titles: list = field(default_factory=list)


@dataclass
class Section:
    chapter_number: int
    chapter_title: str
    section_id: str       # "7.1" ou "intro"
    section_title: str
    content: str
    page_start: int = 0
    page_end: int = 0


# ---------------------------------------------------------------------------
# Extracao de PDF
# ---------------------------------------------------------------------------

def extract_pages(pdf_path: Path) -> list[PageInfo]:
    try:
        import fitz
    except ImportError:
        print("ERRO: PyMuPDF nao instalado. Execute: pip install PyMuPDF")
        sys.exit(1)

    doc = fitz.open(str(pdf_path))
    pages = []

    for idx, page in enumerate(doc):
        text = page.get_text("text")
        tables_md = []

        try:
            tables = page.find_tables()
            for table in tables:
                md = _table_to_markdown(table.extract())
                if md:
                    tables_md.append(md)
        except AttributeError:
            pass

        pages.append(PageInfo(page_num=idx, raw_text=text, tables_md=tables_md))

    doc.close()
    return pages


_HEADER_TABLE_PATTERNS = [
    re.compile(r"(?i)MANUAL DO COMISS.RIO"),
    re.compile(r"(?i)DR-CAB-TC-\d+"),
    re.compile(r"(?i)P.g:\d+-\d+"),
    re.compile(r"(?i)^Rev\s+\d+"),
    re.compile(r"(?i)Manual do MCmsV"),
    re.compile(r"(?i)^Revis.o\s+\d+"),
    re.compile(r"(?i)^M-OPS-\d+"),
]


def _is_header_table(cleaned_rows: list[list[str]]) -> bool:
    """Detect tables that are just page headers/footers (GOL, LATAM, Azul)."""
    all_text = " ".join(c for row in cleaned_rows for c in row if c)
    matches = sum(1 for p in _HEADER_TABLE_PATTERNS if p.search(all_text))
    return matches >= 2


def _table_to_markdown(table_data: list[list]) -> Optional[str]:
    if not table_data or len(table_data) < 2:
        return None

    cleaned = []
    for row in table_data:
        cleaned.append([_clean_cell(c) for c in row])

    # Skip header/footer tables (GOL repeating header on every page)
    if _is_header_table(cleaned):
        return None

    non_empty = sum(
        1 for row in cleaned for c in row
        if c and not re.match(r"^\d{2}/\d{2}/\d{4}$", c)
    )
    if non_empty < 3:
        return None

    header = cleaned[0]
    num_cols = len(header)
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(["---"] * num_cols) + " |",
    ]
    for row in cleaned[1:]:
        while len(row) < num_cols:
            row.append("")
        row = row[:num_cols]
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines)


def _clean_cell(cell) -> str:
    if cell is None:
        return ""
    text = str(cell).strip()
    text = re.sub(r"\s*\n\s*", " ", text)
    text = text.replace("|", "\\|")
    return text


# ---------------------------------------------------------------------------
# Analise de paginas
# ---------------------------------------------------------------------------

# === AZUL patterns ===
RE_PAGE_CODE = re.compile(
    r"MCmsV?-?(\d+|Apx|App)[-\s]*(\w*)?[-\s]*(\d+)?",
    re.IGNORECASE,
)
RE_CHAPTER_HEADER = re.compile(
    r"Chapter\s+(\d+)\s*:\s*(.+)",
    re.IGNORECASE,
)

# === GOL patterns ===
# "Pág:1-4", "Pag:3-30" -> section X, page Y
RE_GOL_PAGE = re.compile(r"P[aá]g:(\d+)-(\d+)", re.IGNORECASE)
# GOL LEP page codes: "LEP-1", "LEP-6"
RE_GOL_LEP = re.compile(r"^LEP-\d+$", re.IGNORECASE)
# GOL header noise lines (to filter when extracting section title)
GOL_HEADER_NOISE = [
    re.compile(r"(?i)^MANUAL DO COMISS.RIO DE VOO"),
    re.compile(r"(?i)^P.g:\d+-\d+"),
    re.compile(r"(?i)^DR-CAB-TC-\d+"),
    re.compile(r"(?i)^USU.RIO\s*-"),
    re.compile(r"(?i)^este documento"),
    re.compile(r"(?i)^Rev\s+\d+"),
    re.compile(r"^\d{2}/\d{2}/\d{4}"),
    re.compile(r"(?i)^Sum.rio"),
    re.compile(r"(?i)^LEP"),
    re.compile(r"(?i)^Lista de P.ginas"),
    re.compile(r"(?i)^Efetivas$"),
]

# === LATAM patterns ===
# "01. Generalidades", "04. Standard Operating Procedures (SOP)"
RE_LATAM_SECTION = re.compile(
    r"^(\d{2})\.\s+(.+)$",
)

# === Common patterns ===
RE_CAPITULO_TOC = re.compile(
    r"CAP.TULO\s*:?\s*(\d+)\s+(.+)",
    re.IGNORECASE,
)
RE_TOC_ENTRY = re.compile(r"\.{4,}\s*\d+\s*$")
RE_SECTION = re.compile(r"^(\d+)\.(\d+)(?:\.\d+)*\.?\s*(.+)")
RE_TOP_SECTION = re.compile(r"^(\d+)\.(\d+)(?!\.\d)\.?\s+(.+)")


def analyze_page(page: PageInfo, airline: str = "") -> None:
    lines = page.raw_text.split("\n")

    # --- Strategy 1: AZUL - "Chapter N: Title" header ---
    if airline == "azul":
        for line in lines:
            m = RE_CHAPTER_HEADER.match(line.strip())
            if m:
                page.chapter_num = int(m.group(1))
                page.chapter_title = m.group(2).strip()
                break

    # --- Strategy 2: AZUL - MCmsV page code ---
    if page.chapter_num == -1 and airline == "azul":
        for line in lines:
            m = RE_PAGE_CODE.search(line.strip())
            if m:
                cap_str = m.group(1)
                if cap_str.isdigit():
                    page.chapter_num = int(cap_str)
                break

    # --- Strategy 3: GOL - "Pág:X-Y" (X = section number) ---
    if page.chapter_num == -1 and airline == "gol":
        for line in lines:
            m = RE_GOL_PAGE.search(line.strip())
            if m:
                page.chapter_num = int(m.group(1))
                break

    # --- Strategy 3b: GOL - LEP pages are front matter ---
    if page.chapter_num == -1 and airline == "gol":
        for line in lines:
            if RE_GOL_LEP.match(line.strip()):
                page.is_front_matter = True
                break

    # --- Strategy 3c: GOL - extract section title from header lines ---
    # Only for GOL pages (detected by Pág: pattern match above)
    if page.chapter_num != -1 and not page.chapter_title and airline == "gol":
        title_parts = []
        for line in lines[:8]:
            stripped = line.strip()
            if not stripped:
                continue
            is_header_noise = any(p.match(stripped) for p in GOL_HEADER_NOISE)
            if is_header_noise:
                continue
            # Content lines (start with section numbers like "3.4.2")
            if re.match(r"^\d+\.\d+", stripped):
                break
            # Short title-like lines (not content paragraphs)
            if len(stripped) < 40:
                title_parts.append(stripped)
        if title_parts:
            page.chapter_title = " ".join(title_parts)

    # --- Strategy 4: LATAM - "XX. Section Name" in header ---
    # Only for LATAM airline (avoid false positives on Azul/GOL content)
    if page.chapter_num == -1 and not page.is_front_matter and airline == "latam":
        for idx, line in enumerate(lines[:10]):  # Only check first 10 lines (header area)
            m = RE_LATAM_SECTION.match(line.strip())
            if m:
                sec_num = int(m.group(1))
                sec_title = m.group(2).strip()
                # Check if title continues on next line (wrapping)
                if idx + 1 < len(lines):
                    next_line = lines[idx + 1].strip()
                    # Next line is continuation if it's short, not empty,
                    # not a date/revision, and not another section header
                    if (
                        next_line
                        and len(next_line) < 60
                        and not RE_LATAM_SECTION.match(next_line)
                        and not re.match(r"(?i)^(?:Janeiro|Fevereiro|Mar|Abril|Maio|Junho|Julho|Agosto|Setembro|Outubro|Novembro|Dezembro)", next_line)
                        and not re.match(r"(?i)^Revis.o", next_line)
                        and not re.match(r"^\d", next_line)
                        and not re.match(r"(?i)^Manual", next_line)
                    ):
                        sec_title = sec_title + " " + next_line
                page.chapter_num = sec_num
                page.chapter_title = sec_title
                break

    # --- TOC detection ---
    # Azul/LATAM: dots leader "....  42"
    toc_count = sum(1 for line in lines if RE_TOC_ENTRY.search(line))
    if toc_count >= 3:
        page.is_toc = True
    # GOL: dash leader "7.1 GENERALIDADES - - - - - - 7-3"
    if not page.is_toc:
        dash_toc_count = sum(
            1 for line in lines if re.search(r"- - - - .*\d+-\d+", line)
        )
        if dash_toc_count >= 3:
            page.is_toc = True

    # --- Front matter detection ---
    if page.chapter_num == -1 and page.page_num < 50:
        date_lines = sum(
            1 for l in lines
            if re.match(r"^\s*\d{2}/\d{2}/\d{4}\s*$", l.strip())
        )
        if date_lines > len(lines) * 0.3:
            page.is_front_matter = True

    # GOL: pages with LEP / Sumario are front matter
    if page.chapter_num == -1 and airline == "gol":
        text_upper = page.raw_text.upper()
        if "LISTA DE PAGINAS EFETIVAS" in text_upper or "LEP" in text_upper.split():
            page.is_front_matter = True
        if "SUMARIO GERAL" in text_upper or "INDICE GERAL" in text_upper:
            page.is_toc = True


def analyze_all_pages(pages: list[PageInfo], airline: str = "") -> None:
    for page in pages:
        analyze_page(page, airline)

    # Propagate chapter to undetected pages (between pages of same chapter)
    last_chapter = -1
    last_title = ""
    for page in pages:
        if page.chapter_num != -1:
            last_chapter = page.chapter_num
            if page.chapter_title:
                last_title = page.chapter_title
        elif not page.is_front_matter and not page.is_toc:
            page.chapter_num = last_chapter
            if not page.chapter_title:
                page.chapter_title = last_title


# ---------------------------------------------------------------------------
# Limpeza de ruido por pagina
# ---------------------------------------------------------------------------

def clean_page(page: PageInfo, airline: str = "") -> str:
    lines = page.raw_text.split("\n")

    # GOL: strip header block (first ~9 lines are MANUAL, Pag, Rev, DR-CAB,
    # section title, USUARIO disclaimer, date). These lines are NOT content.
    if airline == "gol":
        lines = _strip_gol_header(lines)

    cleaned = []

    for line in lines:
        stripped = line.strip()

        if not stripped:
            if not cleaned or cleaned[-1] != "":
                cleaned.append("")
            continue

        if _is_noise_line(stripped):
            continue

        if re.match(r"^[.\-_=]{5,}$", stripped):
            continue

        cleaned.append(stripped)

    text = "\n".join(cleaned)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
    return text.strip()


def _strip_gol_header(lines: list[str]) -> list[str]:
    """Remove GOL page header block. The header has these markers scattered
    in the first ~12 lines: MANUAL DO COMISSÁRIO, Pág:, Rev, DR-CAB-TC,
    USUÁRIO, date, section title fragments. We find the last header line
    and skip everything before it."""
    header_end = 0
    for i, line in enumerate(lines[:12]):
        s = line.strip()
        if not s:
            continue
        is_header = any(p.match(s) for p in GOL_HEADER_NOISE)
        # Also catch short non-numbered lines that are title fragments
        if not is_header and len(s) < 30 and not re.match(r"^\d+\.?\d*\s", s):
            # Could be a title fragment like "Procedimentos" or "de Emergência"
            # Only skip if surrounded by other header lines
            if i > 0 and header_end >= i - 1:
                is_header = True
        if is_header:
            header_end = i
    return lines[header_end + 1:] if header_end > 0 else lines


def _is_noise_line(line: str) -> bool:
    # Normalize to NFC so composed/decomposed accents match consistently
    normalized = unicodedata.normalize("NFC", line)
    for pattern in NOISE_LINE_RE:
        if pattern.match(normalized):
            return True
    return False


# ---------------------------------------------------------------------------
# Mapa de capitulos
# ---------------------------------------------------------------------------

def extract_chapter_map(pages: list[PageInfo], airline: str = "") -> dict[int, str]:
    chapter_map: dict[int, str] = {}

    for page in pages:
        # Collect titles from page analysis (works for all 3 formats)
        if page.chapter_num != -1 and page.chapter_title:
            if page.chapter_num not in chapter_map:
                chapter_map[page.chapter_num] = page.chapter_title

        # Azul TOC: "CAPITULO:N  TITULO"
        if page.is_toc and airline != "latam":
            for line in page.raw_text.split("\n"):
                m = RE_CAPITULO_TOC.match(line.strip())
                if m:
                    num = int(m.group(1))
                    title = m.group(2).strip()
                    title = re.sub(r"\.{2,}.*$", "", title).strip()
                    if title and num not in chapter_map:
                        chapter_map[num] = title

        # LATAM TOC: "XX. Section Name" - ONLY for LATAM
        if page.is_toc and airline == "latam":
            for line in page.raw_text.split("\n"):
                m = RE_LATAM_SECTION.match(line.strip())
                if m:
                    num = int(m.group(1))
                    title = m.group(2).strip()
                    title = re.sub(r"\.{2,}.*$", "", title).strip()
                    if title and num not in chapter_map:
                        chapter_map[num] = title

    return chapter_map


# ---------------------------------------------------------------------------
# Agrupamento por capitulo
# ---------------------------------------------------------------------------

def group_by_chapter(
    pages: list[PageInfo],
    chapter_map: dict[int, str],
    airline: str = "",
) -> list[Chapter]:
    chapter_content: dict[int, list[str]] = {}
    chapter_pages: dict[int, list[int]] = {}
    chapter_sections: dict[int, list[str]] = {}

    for page in pages:
        if page.is_front_matter or page.is_toc or page.chapter_num == -1:
            continue

        clean = clean_page(page, airline)
        if not clean or len(clean) < 20:
            continue

        cap = page.chapter_num
        chapter_content.setdefault(cap, []).append(clean)
        chapter_pages.setdefault(cap, []).append(page.page_num + 1)

        for line in clean.split("\n"):
            m = RE_SECTION.match(line.strip())
            if m and int(m.group(1)) == cap:
                sec_title = m.group(3).strip()
                if sec_title:
                    chapter_sections.setdefault(cap, []).append(
                        f"{m.group(1)}.{m.group(2)}. {sec_title}"
                    )

        for tbl in page.tables_md:
            chapter_content[cap].append(f"\n{tbl}\n")

    chapters = []
    for cap_num in sorted(chapter_content.keys()):
        # If we have a chapter_map, skip chapters not in it (phantom chapters
        # from page codes in TOC/index pages, e.g. Azul MCmsV-11 on page 13)
        if chapter_map and cap_num not in chapter_map:
            continue

        content = "\n\n".join(chapter_content[cap_num])
        pg_list = chapter_pages.get(cap_num, [])
        title = chapter_map.get(cap_num, f"Capitulo {cap_num}")
        sections = chapter_sections.get(cap_num, [])

        seen = set()
        unique = []
        for s in sections:
            if s not in seen:
                seen.add(s)
                unique.append(s)

        chapters.append(Chapter(
            number=cap_num,
            title=title,
            content=content,
            page_start=min(pg_list) if pg_list else 0,
            page_end=max(pg_list) if pg_list else 0,
            section_titles=unique[:20],
        ))

    return chapters


# ---------------------------------------------------------------------------
# Sub-chunking por secao
# ---------------------------------------------------------------------------

def split_chapter_into_sections(chapter: Chapter, min_chars: int = 100) -> list[Section]:
    """Split a chapter into top-level sections (X.Y) for finer-grained RAG chunks."""
    lines = chapter.content.split("\n")
    cut_points: list[tuple[int, str, str]] = []  # (line_idx, section_id, section_title)

    for i, line in enumerate(lines):
        m = RE_TOP_SECTION.match(line.strip())
        if m and int(m.group(1)) == chapter.number:
            sec_id = f"{m.group(1)}.{m.group(2)}"
            sec_title = m.group(3).strip()
            cut_points.append((i, sec_id, sec_title))

    if not cut_points:
        return [Section(
            chapter_number=chapter.number,
            chapter_title=chapter.title,
            section_id="intro",
            section_title=chapter.title,
            content=chapter.content,
            page_start=chapter.page_start,
            page_end=chapter.page_end,
        )]

    sections: list[Section] = []

    # Intro: content before the first section
    if cut_points[0][0] > 0:
        intro_lines = lines[:cut_points[0][0]]
        intro_content = "\n".join(intro_lines).strip()
        if len(intro_content) >= min_chars:
            sections.append(Section(
                chapter_number=chapter.number,
                chapter_title=chapter.title,
                section_id="intro",
                section_title=chapter.title,
                content=intro_content,
                page_start=chapter.page_start,
                page_end=chapter.page_start,
            ))

    # Each section from cut_point to the next
    for idx, (line_idx, sec_id, sec_title) in enumerate(cut_points):
        if idx + 1 < len(cut_points):
            end_idx = cut_points[idx + 1][0]
        else:
            end_idx = len(lines)

        sec_content = "\n".join(lines[line_idx:end_idx]).strip()

        # Merge tiny sections with the next one
        if len(sec_content) < min_chars and idx + 1 < len(cut_points):
            # Prepend to next section's range by adjusting cut_points
            next_line_idx, next_sec_id, next_sec_title = cut_points[idx + 1]
            cut_points[idx + 1] = (line_idx, next_sec_id, next_sec_title)
            continue

        sections.append(Section(
            chapter_number=chapter.number,
            chapter_title=chapter.title,
            section_id=sec_id,
            section_title=sec_title,
            content=sec_content,
            page_start=chapter.page_start,
            page_end=chapter.page_end,
        ))

    return sections


# Regex para sub-secao X.Y.Z (qualquer profundidade)
RE_SUBSECTION = re.compile(r"^(\d+)\.(\d+)\.\d+")


def split_large_section(section: Section, max_chars: int = 15000) -> list[Section]:
    """Split an oversized section into smaller parts.

    Strategy:
      1. Try splitting at X.Y.Z sub-section headers
      2. Fallback: split at paragraph boundaries (double newline)
    Parts keep the original section_id with _p1, _p2, ... suffix.
    """
    if len(section.content) <= max_chars:
        return [section]

    lines = section.content.split("\n")

    # --- Strategy 1: split at sub-section headers (X.Y.Z) ---
    cut_indices = [0]
    for i, line in enumerate(lines):
        m = RE_SUBSECTION.match(line.strip())
        if m and i > 0:
            parent_id = f"{m.group(1)}.{m.group(2)}"
            if parent_id == section.section_id or section.section_id == "intro":
                cut_indices.append(i)

    if len(cut_indices) > 1:
        # Build chunks from sub-section cut points, merging small ones
        raw_chunks: list[str] = []
        for ci in range(len(cut_indices)):
            start = cut_indices[ci]
            end = cut_indices[ci + 1] if ci + 1 < len(cut_indices) else len(lines)
            raw_chunks.append("\n".join(lines[start:end]).strip())

        # Greedily merge consecutive chunks to stay under max_chars
        merged: list[str] = []
        buf = raw_chunks[0]
        for chunk in raw_chunks[1:]:
            if len(buf) + len(chunk) + 2 <= max_chars:
                buf = buf + "\n\n" + chunk
            else:
                merged.append(buf)
                buf = chunk
        merged.append(buf)

        if len(merged) > 1:
            parts = []
            for pi, part_content in enumerate(merged, 1):
                parts.append(Section(
                    chapter_number=section.chapter_number,
                    chapter_title=section.chapter_title,
                    section_id=f"{section.section_id}_p{pi}",
                    section_title=section.section_title,
                    content=part_content,
                    page_start=section.page_start,
                    page_end=section.page_end,
                ))
            # Recursively split any parts still over max_chars (paragraph fallback)
            final = []
            for part in parts:
                if len(part.content) > max_chars:
                    final.extend(_split_by_paragraphs(part, max_chars))
                else:
                    final.append(part)
            # Re-number if sub-splits happened
            if len(final) != len(parts):
                base_id = section.section_id
                for i, p in enumerate(final, 1):
                    p.section_id = f"{base_id}_p{i}"
            return final

    # --- Strategy 2: split at paragraph boundaries ---
    return _split_by_paragraphs(section, max_chars)


def _split_by_paragraphs(section: Section, max_chars: int) -> list[Section]:
    """Split a section at paragraph boundaries (double newline)."""
    paragraphs = re.split(r"\n{2,}", section.content)
    merged_parts: list[str] = []
    buf = ""
    for para in paragraphs:
        if buf and len(buf) + len(para) + 2 > max_chars:
            merged_parts.append(buf.strip())
            buf = para
        else:
            buf = buf + "\n\n" + para if buf else para
    if buf.strip():
        merged_parts.append(buf.strip())

    if len(merged_parts) <= 1:
        return [section]

    # Use base section_id (strip existing _pN suffix for re-splits)
    base_id = re.sub(r"_p\d+$", "", section.section_id)
    parts = []
    for pi, part_content in enumerate(merged_parts, 1):
        parts.append(Section(
            chapter_number=section.chapter_number,
            chapter_title=section.chapter_title,
            section_id=f"{base_id}_p{pi}",
            section_title=section.section_title,
            content=part_content,
            page_start=section.page_start,
            page_end=section.page_end,
        ))
    return parts


# ---------------------------------------------------------------------------
# Pos-processamento
# ---------------------------------------------------------------------------

def _is_paragraph_end(line: str) -> bool:
    """Detect if a line looks like the END of a paragraph (complete sentence)."""
    if not line:
        return True
    # Ends with sentence-ending punctuation
    if line.endswith((".", ";", "!", "?", ":")):
        return True
    # Table row
    if line.endswith("|"):
        return True
    return False


def _is_structural_line(line: str) -> bool:
    """Detect lines that should NEVER be joined to adjacent lines."""
    if not line:
        return True
    # Section headers: "2.3.1. Titulo"
    if re.match(r"^\d+\.\d+", line):
        return True
    # Markdown structural
    if line.startswith(("#", "|", "---")):
        return True
    # NOTA, ATENÇÃO, IMPORTANTE blocks
    if re.match(r"^(?:NOTA|ATEN.+O|IMPORTANTE|AVISO|CUIDADO)\b", line):
        return True
    # Bullet/list items (already formed)
    if re.match(r"^[-*]\s", line):
        return True
    # Figure/Figura references
    if re.match(r"(?i)^Figura\s+\d+", line):
        return True
    return False


def postprocess_content(text: str) -> str:
    lines = text.split("\n")

    # --- Phase 1: Deduplicate consecutive identical lines ---
    deduped = []
    prev = None
    for line in lines:
        stripped = line.strip()
        if stripped == prev and stripped:
            continue
        deduped.append(stripped)
        prev = stripped

    # --- Phase 2: Merge isolated bullets with their text ---
    # "•\n actual text" -> "• actual text"
    merged = []
    i = 0
    while i < len(deduped):
        line = deduped[i]
        if line in ("•", "●", "◦", "○", "▪") and i + 1 < len(deduped) and deduped[i + 1]:
            merged.append(f"- {deduped[i + 1]}")
            i += 2
            continue
        # Replace standalone bullet markers at start of line
        if re.match(r"^[•●◦○▪]\s+", line):
            merged.append("- " + re.sub(r"^[•●◦○▪]\s+", "", line))
        else:
            merged.append(line)
        i += 1

    # --- Phase 3: Remove false blank lines from page breaks ---
    # PDF page breaks insert empty lines mid-paragraph. Remove them when:
    #   prev line does NOT end with sentence punctuation
    #   blank line(s)
    #   next line starts with lowercase or is clearly a continuation
    no_false_blanks = []
    i = 0
    while i < len(merged):
        line = merged[i]

        # Check for blank line(s) between two content lines
        if not line and no_false_blanks:
            prev = no_false_blanks[-1] if no_false_blanks else ""
            # Count consecutive blank lines
            blank_count = 0
            j = i
            while j < len(merged) and not merged[j]:
                blank_count += 1
                j += 1

            # Only bridge single blank lines (multi-blank = intentional break)
            if blank_count == 1 and j < len(merged):
                next_line = merged[j]
                prev_incomplete = prev and not _is_paragraph_end(prev) and not _is_structural_line(prev)
                next_continues = (
                    next_line
                    and not _is_structural_line(next_line)
                    and not next_line.startswith(("- ", "* "))
                    and (
                        # Starts with lowercase = obvious continuation
                        (next_line[0].islower())
                        # Starts with opening paren/bracket = continuation
                        or next_line[0] in ("(", "[", "–", "—")
                    )
                )
                if prev_incomplete and next_continues:
                    # Skip the blank line - it was a false page break
                    i += 1
                    continue

            # Keep blank lines as-is
            no_false_blanks.append("")
            i += 1
            continue

        no_false_blanks.append(line)
        i += 1

    # --- Phase 4: Reconstruct broken paragraphs ---
    # PDF extraction splits paragraphs at line boundaries. Join lines that
    # are clearly a continuation of the previous line.
    rebuilt = []
    i = 0
    while i < len(no_false_blanks):
        line = no_false_blanks[i]

        if not line or _is_structural_line(line):
            rebuilt.append(line)
            i += 1
            continue

        # Accumulate continuation lines
        accumulator = line
        while i + 1 < len(no_false_blanks):
            next_line = no_false_blanks[i + 1]

            # Stop if next line is empty or structural
            if not next_line or _is_structural_line(next_line):
                break

            # Stop if current accumulated text already ends a paragraph
            if _is_paragraph_end(accumulator):
                break

            # Stop if next line starts a new bullet/list item
            if next_line.startswith(("- ", "* ")):
                break

            # Stop if next line looks like a new standalone heading (ALL CAPS, short)
            if next_line.isupper() and len(next_line) < 50:
                break

            # Otherwise, join as continuation
            accumulator = accumulator + " " + next_line
            i += 1

        rebuilt.append(accumulator)
        i += 1

    text = "\n".join(rebuilt)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ---------------------------------------------------------------------------
# Geracao de Markdown
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = text.strip("-")
    return text[:50] if text else "sem-titulo"


def generate_markdown(airline: str, chapter: Chapter) -> str:
    config = AIRLINE_CONFIG[airline]
    codename = config["codename"]
    tema = chapter.title.replace('"', '\\"')
    cap_str = str(chapter.number).zfill(2)

    parts = [
        "---",
        f'empresa: "{codename}"',
        f'tema: "{tema}"',
        f'capitulo: "{cap_str}"',
        f'paginas: "{chapter.page_start}-{chapter.page_end}"',
        f'fonte: "{airline.upper()} - Manual de Comissarios"',
        "---",
        "",
        f"# {chapter.title}",
        "",
    ]

    if chapter.section_titles:
        parts.append("## Indice de Secoes")
        parts.append("")
        for sec in chapter.section_titles:
            parts.append(f"- {sec}")
        parts.append("")
        parts.append("---")
        parts.append("")

    content = postprocess_content(chapter.content)
    parts.append(content)
    parts.append("")

    return "\n".join(parts)


def generate_section_markdown(airline: str, section: Section) -> str:
    config = AIRLINE_CONFIG[airline]
    codename = config["codename"]
    tema = section.chapter_title.replace('"', '\\"')
    cap_str = str(section.chapter_number).zfill(2)
    sec_titulo = section.section_title.replace('"', '\\"')

    parts = [
        "---",
        f'empresa: "{codename}"',
        f'tema: "{tema}"',
        f'capitulo: "{cap_str}"',
        f'secao: "{section.section_id}"',
        f'secao_titulo: "{sec_titulo}"',
        f'paginas: "{section.page_start}-{section.page_end}"',
        f'fonte: "{airline.upper()} - Manual de Comissarios"',
        "---",
        "",
        f"# {section.section_title}",
        "",
    ]

    content = postprocess_content(section.content)
    parts.append(content)
    parts.append("")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def process_pdf(
    pdf_path: Path,
    airline: str,
    output_base: Path,
    engine: str = "pymupdf",
    sections: bool = False,
) -> list[Path]:
    config = AIRLINE_CONFIG[airline]
    output_dir = output_base / config["output_dir"]
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  Processando: {pdf_path.name}")
    print(f"  Companhia:   {config['codename']} ({airline.upper()})")
    print(f"{'='*60}")

    print("\n[1/5] Extraindo paginas do PDF...")
    pages = extract_pages(pdf_path)
    print(f"  {len(pages)} paginas extraidas")

    table_count = sum(len(p.tables_md) for p in pages)
    if table_count:
        print(f"  {table_count} tabelas detectadas")

    print("\n[2/5] Analisando estrutura...")
    analyze_all_pages(pages, airline)

    fm = sum(1 for p in pages if p.is_front_matter)
    toc = sum(1 for p in pages if p.is_toc)
    ct = sum(1 for p in pages if not p.is_front_matter and not p.is_toc and p.chapter_num != -1)
    unk = sum(1 for p in pages if p.chapter_num == -1 and not p.is_front_matter and not p.is_toc)

    print(f"  Preliminares: {fm} | Indice: {toc} | Conteudo: {ct} | Sem cap: {unk}")

    print("\n[3/5] Mapa de capitulos...")
    chapter_map = extract_chapter_map(pages, airline)
    for num, title in sorted(chapter_map.items()):
        print(f"  Cap {num:2d}: {title}")

    if not chapter_map:
        print("  AVISO: fallback por secoes numeradas")
        for page in pages:
            if page.chapter_num != -1 and page.chapter_num not in chapter_map:
                chapter_map[page.chapter_num] = f"Capitulo {page.chapter_num}"

    print("\n[4/5] Agrupando por capitulo...")
    chapters = group_by_chapter(pages, chapter_map, airline)
    print(f"  {len(chapters)} capitulos")

    for ch in chapters:
        chars = len(ch.content)
        print(f"  Cap {ch.number:2d}: {chars:>8,} chars | pp. {ch.page_start}-{ch.page_end} | {ch.title}")

    print(f"\n[5/5] Gerando Markdown{' (secoes)' if sections else ''}...")
    generated = []

    if sections:
        for chapter in chapters:
            if len(chapter.content.strip()) < 50:
                print(f"  SKIP Cap {chapter.number} (curto)")
                continue

            cap_str = str(chapter.number).zfill(2)
            secs = split_chapter_into_sections(chapter)
            # Second pass: split oversized sections
            final_secs: list[Section] = []
            for sec in secs:
                final_secs.extend(split_large_section(sec))
            secs = final_secs
            print(f"  Cap {cap_str}: {len(secs)} secoes")

            for sec in secs:
                if sec.section_id == "intro":
                    sec_tag = "sec0"
                    slug = "intro"
                else:
                    sec_tag = f"sec{sec.section_id}"
                    slug = slugify(sec.section_title)

                filename = f"{airline}_cap{cap_str}_{sec_tag}_{slug}.md"
                filepath = output_dir / filename

                md = generate_section_markdown(airline, sec)
                filepath.write_text(md, encoding="utf-8")
                generated.append(filepath)
                print(f"    -> {filename} ({len(sec.content):,} chars)")
    else:
        for chapter in chapters:
            if len(chapter.content.strip()) < 50:
                print(f"  SKIP Cap {chapter.number} (curto)")
                continue

            cap_str = str(chapter.number).zfill(2)
            slug = slugify(chapter.title)
            filename = f"{airline}_cap{cap_str}_{slug}.md"
            filepath = output_dir / filename

            md = generate_markdown(airline, chapter)
            filepath.write_text(md, encoding="utf-8")
            generated.append(filepath)
            print(f"  -> {filename}")

    print(f"\n  Total: {len(generated)} arquivos em {output_dir}/")
    return generated


# ---------------------------------------------------------------------------
# Descoberta de PDFs
# ---------------------------------------------------------------------------

def find_pdfs(input_path: Path, airline: Optional[str] = None) -> list[tuple[Path, str]]:
    pairs = []

    if input_path.is_file() and input_path.suffix.lower() == ".pdf":
        if not airline:
            print("ERRO: --airline obrigatorio para arquivo individual.")
            sys.exit(1)
        pairs.append((input_path, airline))

    elif input_path.is_dir():
        for pdf in sorted(input_path.rglob("*.pdf")):
            det = airline
            if not det:
                parts = [p.lower() for p in pdf.relative_to(input_path).parts]
                for part in parts:
                    for key in ["gol", "latam", "azul"]:
                        if key in part:
                            det = key
                            break
                    if det:
                        break
                if not det:
                    fname = pdf.stem.lower()
                    for key in ["gol", "latam", "azul"]:
                        if key in fname:
                            det = key
                            break
            if det:
                pairs.append((pdf, det))
            else:
                print(f"  AVISO: Companhia nao detectada: {pdf.name}")
    else:
        print(f"ERRO: Caminho nao encontrado: {input_path}")
        sys.exit(1)

    return pairs


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ETL v2: Manuais de Comissarios -> Markdown (Page-Aware)",
    )
    parser.add_argument("--input", "-i", required=True, type=Path)
    parser.add_argument("--airline", "-a", choices=["gol", "latam", "azul"])
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--output", "-o", type=Path, default=Path("output/chunks"))
    parser.add_argument("--engine", "-e", choices=["pymupdf", "marker"], default="pymupdf")
    parser.add_argument("--sections", "-s", action="store_true",
                        help="Sub-chunk capitulos em secoes (X.Y) para RAG")

    args = parser.parse_args()
    if not args.airline and not args.all:
        parser.error("Use --airline ou --all")

    print("=" * 60)
    print("  ETL v2: Manuais -> Markdown (Page-Aware)")
    print("=" * 60)

    pdf_pairs = find_pdfs(args.input, args.airline)
    if not pdf_pairs:
        print("\nNenhum PDF encontrado.")
        sys.exit(1)

    print(f"\n{len(pdf_pairs)} PDF(s):")
    for p, a in pdf_pairs:
        print(f"  - {p.name} -> {AIRLINE_CONFIG[a]['codename']}")

    all_files = []
    for pdf_path, arl in pdf_pairs:
        files = process_pdf(pdf_path, arl, args.output, args.engine, args.sections)
        all_files.extend(files)

    print(f"\n{'='*60}")
    print("  CONCLUIDO")
    print(f"  Total: {len(all_files)} arquivos")
    print(f"  Saida: {args.output.resolve()}")
    print(f"{'='*60}")

    by_dir = {}
    for f in all_files:
        by_dir.setdefault(f.parent.name, []).append(f)
    for d, files in sorted(by_dir.items()):
        print(f"\n  {d}/  ({len(files)} arquivos)")
        for f in files:
            print(f"    {f.name}")


if __name__ == "__main__":
    main()
