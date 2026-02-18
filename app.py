
import os
import sqlite3
import unicodedata
from pathlib import Path
from typing import Optional, Tuple, List

from fastapi import FastAPI, Query, HTTPException, UploadFile, File, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import openpyxl

APP_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))

STORAGE_DIR = Path(os.getenv("TDFC_STORAGE_DIR", APP_DIR / "storage"))
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

DATA_FILE = STORAGE_DIR / "current.xlsx"
DB_PATH = STORAGE_DIR / "tdfc_cache.sqlite"

SHEET_NAME_DEFAULT = os.getenv("TDFC_SHEET", "2026")
ADMIN_KEY = os.getenv("TDFC_ADMIN_KEY", "")

app = FastAPI(title="TDFC Lookup", version="2.0")

def norm(s) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    s = s.replace("\u00a0", " ")
    while "  " in s:
        s = s.replace("  ", " ")
    return s

def ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entries (
          imprime TEXT NOT NULL,
          codeedi TEXT NOT NULL,
          libelle TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entries ON entries(imprime, codeedi)")
    conn.commit()

def get_file_sig(sheet_name: str) -> str:
    if not DATA_FILE.exists():
        return ""
    st = DATA_FILE.stat()
    return f"{DATA_FILE.resolve()}|{int(st.st_mtime)}|{st.st_size}|{sheet_name}"

def find_header(ws, max_scan_rows: int = 40) -> Tuple[int, int, int, int]:
    def pick(hm, *keys) -> Optional[int]:
        for k in keys:
            nk = norm(k)
            if nk in hm:
                return hm[nk]
        return None

    max_r = min(max_scan_rows, ws.max_row or 0)
    for r in range(1, max_r + 1):
        hm = {}
        for idx, cell in enumerate(ws[r], start=1):
            t = norm(cell.value)
            if t and t not in hm:
                hm[t] = idx

        ci = pick(hm, "imprimé", "imprime")
        cc = pick(hm, "code edi", "code_edi", "codeedi")
        cl = pick(hm, "libellé", "libelle")

        if ci and cc and cl:
            return r, ci, cc, cl

    raise RuntimeError("En-têtes introuvables")

def rebuild_cache(sheet_name: str) -> None:
    if not DATA_FILE.exists():
        raise RuntimeError("Aucun fichier uploadé")

    with sqlite3.connect(DB_PATH) as conn:
        ensure_db(conn)
        file_sig = get_file_sig(sheet_name)

        conn.execute("DELETE FROM entries;")
        conn.execute("DELETE FROM meta;")
        conn.execute("INSERT INTO meta(key, value) VALUES(?, ?)", ("file_sig", file_sig))
        conn.commit()

        wb = openpyxl.load_workbook(DATA_FILE, read_only=True, data_only=True)
        if sheet_name not in wb.sheetnames:
            raise RuntimeError(f"Onglet '{sheet_name}' introuvable.")
        ws = wb[sheet_name]

        header_row, col_imprime, col_codeedi, col_libelle = find_header(ws)

        batch = []
        for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
            if col_imprime - 1 >= len(row) or col_codeedi - 1 >= len(row):
                continue
            imp = norm(row[col_imprime - 1])
            edi = norm(row[col_codeedi - 1])
            if not imp or not edi:
                continue
            lib = ""
            if col_libelle - 1 < len(row) and row[col_libelle - 1] is not None:
                lib = str(row[col_libelle - 1]).strip()
            if not lib:
                continue
            batch.append((imp, edi, lib))

        conn.executemany("INSERT INTO entries(imprime, codeedi, libelle) VALUES(?, ?, ?)", batch)
        conn.commit()

def ensure_cache(sheet: str):
    with sqlite3.connect(DB_PATH) as conn:
        ensure_db(conn)
        cur = conn.cursor()
        cur.execute("SELECT value FROM meta WHERE key='file_sig'")
        row = cur.fetchone()
        if not row or row[0] != get_file_sig(sheet):
            rebuild_cache(sheet)

def require_admin(key: str):
    if ADMIN_KEY and key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/upload")
async def upload(file: UploadFile = File(...), key: str = Query("")):
    require_admin(key)
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Format invalide (.xlsx uniquement)")
    content = await file.read()
    DATA_FILE.write_bytes(content)
    rebuild_cache(SHEET_NAME_DEFAULT)
    return {"ok": True, "message": "Fichier uploadé et indexé"}

@app.get("/lookup")
def lookup(imprime: str, codeedi: str, all: bool = False):
    ensure_cache(SHEET_NAME_DEFAULT)
    imp = norm(imprime)
    edi = norm(codeedi)

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        if not all:
            cur.execute("SELECT libelle FROM entries WHERE imprime=? AND codeedi=? LIMIT 1", (imp, edi))
            r = cur.fetchone()
            return {"found": bool(r), "libelle": r[0] if r else ""}
        cur.execute("SELECT libelle FROM entries WHERE imprime=? AND codeedi=?", (imp, edi))
        rows = [x[0] for x in cur.fetchall()]
        return {"found": bool(rows), "libelles": rows}
