import os
import sqlite3
import unicodedata
from pathlib import Path
from typing import Optional, Tuple, List

from fastapi import FastAPI, Query, HTTPException, UploadFile, File, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

import openpyxl


# =========================
# CONFIG
# =========================
APP_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))

STORAGE_DIR = Path(os.getenv("TDFC_STORAGE_DIR", APP_DIR / "storage"))
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

DATA_FILE = STORAGE_DIR / "current.xlsx"
DB_PATH = STORAGE_DIR / "tdfc_cache.sqlite"

SHEET_NAME = os.getenv("TDFC_SHEET", "2026")
ADMIN_KEY = os.getenv("TDFC_ADMIN_KEY", "")


# =========================
# APP
# =========================
app = FastAPI(title="TDFC Dico", version="5.0")

STATIC_DIR = APP_DIR / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# =========================
# UTILS
# =========================
def norm(s) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")
    s = s.replace("\u00a0", " ")
    while "  " in s:
        s = s.replace("  ", " ")
    return s


def require_admin(key: str) -> None:
    if ADMIN_KEY and key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


def ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
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


def get_file_sig() -> str:
    if not DATA_FILE.exists():
        return ""
    st = DATA_FILE.stat()
    return f"{DATA_FILE.resolve()}|{int(st.st_mtime)}|{st.st_size}|{SHEET_NAME}"


def find_header(ws):
    for r in range(1, 40):
        hm = {}
        for idx, cell in enumerate(ws[r], start=1):
            val = norm(cell.value)
            if val:
                hm[val] = idx

        ci = hm.get("imprimé") or hm.get("imprime")
        cc = hm.get("code edi") or hm.get("codeedi")
        cl = hm.get("libellé") or hm.get("libelle")

        if ci and cc and cl:
            return r, ci, cc, cl

    raise RuntimeError("En-têtes introuvables")


def rebuild_cache():
    with sqlite3.connect(DB_PATH) as conn:
        ensure_db(conn)

        conn.execute("DELETE FROM entries;")
        conn.execute("DELETE FROM meta;")
        conn.execute("INSERT INTO meta(key,value) VALUES(?,?)",
                     ("file_sig", get_file_sig()))
        conn.commit()

        wb = openpyxl.load_workbook(DATA_FILE, read_only=True, data_only=True)
        ws = wb[SHEET_NAME]

        header_row, ci, cc, cl = find_header(ws)

        batch = []
        for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
            imp = norm(row[ci - 1])
            edi = norm(row[cc - 1])
            if not imp or not edi:
                continue

            lib = ""
            if row[cl - 1]:
                lib = str(row[cl - 1]).strip()
            if not lib:
                continue

            batch.append((imp, edi, lib))

        conn.executemany(
            "INSERT INTO entries(imprime, codeedi, libelle) VALUES(?,?,?)",
            batch
        )
        conn.commit()


def ensure_cache():
    if not DATA_FILE.exists():
        raise RuntimeError("Aucun fichier uploadé.")

    with sqlite3.connect(DB_PATH) as conn:
        ensure_db(conn)
        cur = conn.cursor()
        cur.execute("SELECT value FROM meta WHERE key='file_sig'")
        row = cur.fetchone()

        if not row or row[0] != get_file_sig():
            rebuild_cache()


# =========================
# ROUTES
# =========================
@app.get("/", response_class=HTMLResponse)
def home(request: Request, admin: int = Query(0)):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "has_file": DATA_FILE.exists(),
            "is_admin_ui": admin == 1,
        },
    )


@app.post("/upload")
async def upload_excel(file: UploadFile = File(...), key: str = Query("")):
    require_admin(key)

    content = await file.read()
    tmp = STORAGE_DIR / "upload.tmp.xlsx"
    tmp.write_bytes(content)
    tmp.replace(DATA_FILE)

    rebuild_cache()
    return {"ok": True}


@app.get("/lookup")
def lookup(imprime: str, codeedi: str, all: bool = False):
    ensure_cache()

    imp = norm(imprime)
    edi = norm(codeedi)

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        if not all:
            cur.execute(
                "SELECT libelle FROM entries WHERE imprime=? AND codeedi=? LIMIT 1",
                (imp, edi)
            )
            r = cur.fetchone()
            return {"found": bool(r), "libelle": r[0] if r else ""}

        cur.execute(
            "SELECT libelle FROM entries WHERE imprime=? AND codeedi=?",
            (imp, edi)
        )
        rows = [x[0] for x in cur.fetchall()]
        return {"found": bool(rows), "libelles": rows}
