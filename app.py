#!/usr/bin/env python3
"""Stok Yönetimi - yerel, bağımsız (sadece Python standart kütüphanesi). Çalıştır: python3 app.py"""
import csv, io, json, os, re, shutil, sqlite3, sys, tempfile, threading, time, uuid, webbrowser, zipfile
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BASE = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))  # paketlenmiş dosyalar (index.html)
if getattr(sys, "frozen", False):  # exe/app: veriler kullanıcı klasöründe
    DATA = os.path.join(os.path.expanduser("~"), "StokYonetimi-Veri")
else:
    DATA = os.path.dirname(os.path.abspath(__file__))
os.makedirs(DATA, exist_ok=True)
DB = os.path.join(DATA, "stok.db")
IMG = os.path.join(DATA, "images")
os.makedirs(IMG, exist_ok=True)
PORT = int(os.environ.get("PORT", 8765))
LOCK = threading.RLock()
BK = os.path.join(DATA, "yedekler"); os.makedirs(BK, exist_ok=True)
CFG = os.path.join(DATA, "config.json")
STATE = {"dirty": False, "last": None, "mirror_err": None, "last_bk_ts": 0}

SCHEMA = """
CREATE TABLE IF NOT EXISTS products(id INTEGER PRIMARY KEY, name TEXT NOT NULL, brand TEXT, category TEXT,
  unit TEXT DEFAULT 'adet', min_stock INTEGER DEFAULT 0, track_expiry INTEGER DEFAULT 1, image TEXT,
  attrs TEXT DEFAULT '{}', notes TEXT, active INTEGER DEFAULT 1, created TEXT);
CREATE TABLE IF NOT EXISTS skus(id INTEGER PRIMARY KEY, product_id INTEGER NOT NULL REFERENCES products(id),
  name TEXT DEFAULT 'Standart', barcode TEXT UNIQUE NOT NULL, weight_g REAL, attrs TEXT DEFAULT '{}');
CREATE TABLE IF NOT EXISTS batches(id INTEGER PRIMARY KEY, sku_id INTEGER NOT NULL REFERENCES skus(id),
  lot TEXT, expiry TEXT, qty INTEGER NOT NULL DEFAULT 0, attrs TEXT DEFAULT '{}', created TEXT);
CREATE TABLE IF NOT EXISTS movements(id INTEGER PRIMARY KEY, type TEXT NOT NULL, sku_id INTEGER, batch_id INTEGER,
  qty INTEGER NOT NULL, ref TEXT, note TEXT, ts TEXT, undone INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS fields(id INTEGER PRIMARY KEY, scope TEXT NOT NULL, name TEXT NOT NULL, UNIQUE(scope,name));
"""

class _Conn:
    def __init__(self, c): self.c = c
    def __enter__(self): return self.c
    def __exit__(self, et, ev, tb):
        try: (self.c.commit() if et is None else self.c.rollback())
        finally: self.c.close()
        return False

def db():
    return _Conn(_db())

def _db():
    c = sqlite3.connect(DB, timeout=30); c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys=ON"); c.execute("PRAGMA journal_mode=WAL"); c.execute("PRAGMA synchronous=FULL"); return c

def now(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def cfg():
    try: return json.load(open(CFG, encoding="utf-8"))
    except Exception: return {}

def make_backup(reason="otomatik"):
    """Tutarlı anlık görüntü (db + görseller) -> yedekler/*.zip, ayrıca ek klasöre kopya."""
    with LOCK:
        tmp = os.path.join(tempfile.mkdtemp(), "stok.db")
        src = sqlite3.connect(DB); dst = sqlite3.connect(tmp); src.backup(dst); dst.close(); src.close()
        name = datetime.now().strftime("stok-%Y%m%d-%H%M%S") + ("-" + reason if reason != "otomatik" else "") + ".zip"
        path = os.path.join(BK, name)
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
            z.write(tmp, "stok.db")
            for f in os.listdir(IMG): z.write(os.path.join(IMG, f), "images/" + f)
        shutil.rmtree(os.path.dirname(tmp), ignore_errors=True)
    STATE.update(dirty=False, last=now(), last_bk_ts=time.time(), mirror_err=None)
    m = (cfg().get("mirror_dir") or "").strip()
    if m:
        try: os.makedirs(m, exist_ok=True); shutil.copy2(path, os.path.join(m, name))
        except Exception as e: STATE["mirror_err"] = str(e)
    prune()
    return name

def prune():
    fs = sorted(os.listdir(BK), reverse=True); keep = set(fs[:50]); days = set()
    for f in fs:
        d = f[5:13]
        if d not in days and len(days) < 90: days.add(d); keep.add(f)
    for f in fs:
        if f not in keep: os.remove(os.path.join(BK, f))

def restore_zip(path, safety=True):
    if safety: make_backup("geri-yukleme-oncesi")
    with LOCK, zipfile.ZipFile(path) as z:
        if "stok.db" not in z.namelist(): raise Err("Geçersiz yedek dosyası")
        tmpdir = tempfile.mkdtemp(); z.extract("stok.db", tmpdir)
        t = sqlite3.connect(os.path.join(tmpdir, "stok.db"))
        if t.execute("PRAGMA integrity_check").fetchone()[0] != "ok": raise Err("Yedek dosyası bozuk")
        t.close()
        for ext in ("-wal", "-shm"):
            if os.path.exists(DB + ext): os.remove(DB + ext)
        shutil.copy2(os.path.join(tmpdir, "stok.db"), DB)
        for n in z.namelist():
            if n.startswith("images/") and not n.endswith("/"): open(os.path.join(IMG, os.path.basename(n)), "wb").write(z.read(n))
        shutil.rmtree(tmpdir, ignore_errors=True)

def startup_check():
    if os.path.exists(DB):
        try:
            ok = sqlite3.connect(DB).execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        except Exception: ok = False
        if not ok:
            bad = DB + ".bozuk-" + datetime.now().strftime("%Y%m%d%H%M%S"); os.replace(DB, bad)
            fs = sorted(f for f in os.listdir(BK) if f.endswith(".zip"))
            if fs: restore_zip(os.path.join(BK, fs[-1]), False); print("Veritabanı bozuktu, son yedekten geri yüklendi:", fs[-1])

def bg_backup():
    while True:
        time.sleep(30)
        try:
            if STATE["dirty"] and time.time() - STATE["last_bk_ts"] > 120: make_backup()
        except Exception as e: print("yedek hatası:", e)

startup_check()
with db() as c: c.executescript(SCHEMA)

def rows(c, q, a=()): return [dict(r) for r in c.execute(q, a).fetchall()]
class Err(Exception): pass

def load_attrs(d):
    try: d["attrs"] = json.loads(d.get("attrs") or "{}")
    except Exception: d["attrs"] = {}
    return d

def products_list(c, pid=None):
    q = "SELECT * FROM products WHERE active=1" + (" AND id=?" if pid else "") + " ORDER BY name"
    out = []
    for p in rows(c, q, (pid,) if pid else ()):
        load_attrs(p)
        p["skus"] = [load_attrs(s) for s in rows(c, "SELECT * FROM skus WHERE product_id=? ORDER BY id", (p["id"],))]
        total = 0
        for s in p["skus"]:
            s["stock"] = c.execute("SELECT COALESCE(SUM(qty),0) FROM batches WHERE sku_id=?", (s["id"],)).fetchone()[0]
            total += s["stock"]
        p["stock"] = total
        out.append(p)
    return out

def save_product(c, d, pid=None):
    name = (d.get("name") or "").strip()
    if not name: raise Err("Ürün adı zorunlu")
    vals = (name, d.get("brand"), d.get("category"), d.get("unit") or "adet", int(d.get("min_stock") or 0),
            1 if d.get("track_expiry", True) else 0, json.dumps(d.get("attrs") or {}, ensure_ascii=False), d.get("notes"))
    if pid:
        c.execute("UPDATE products SET name=?,brand=?,category=?,unit=?,min_stock=?,track_expiry=?,attrs=?,notes=? WHERE id=?", vals + (pid,))
    else:
        pid = c.execute("INSERT INTO products(name,brand,category,unit,min_stock,track_expiry,attrs,notes,created) VALUES(?,?,?,?,?,?,?,?,?)",
                        vals + (now(),)).lastrowid
    seen = []
    for s in d.get("skus") or []:
        bc = (s.get("barcode") or "").strip()
        if not bc: raise Err("Her varyantın barkodu olmalı")
        w = float(s["weight_g"]) if s.get("weight_g") not in (None, "") else None
        a = json.dumps(s.get("attrs") or {}, ensure_ascii=False)
        ex = c.execute("SELECT id, product_id FROM skus WHERE barcode=?", (bc,)).fetchone()
        if s.get("id"):
            if ex and ex["id"] != s["id"]: raise Err(f"Barkod zaten kayıtlı: {bc}")
            c.execute("UPDATE skus SET name=?,barcode=?,weight_g=?,attrs=? WHERE id=? AND product_id=?", (s.get("name") or "Standart", bc, w, a, s["id"], pid))
            seen.append(s["id"])
        else:
            if ex: raise Err(f"Barkod zaten kayıtlı: {bc}")
            seen.append(c.execute("INSERT INTO skus(product_id,name,barcode,weight_g,attrs) VALUES(?,?,?,?,?)", (pid, s.get("name") or "Standart", bc, w, a)).lastrowid)
    return pid

def lookup(c, bc):
    s = c.execute("SELECT s.*, p.name pname, p.image, p.track_expiry, p.unit FROM skus s JOIN products p ON p.id=s.product_id WHERE s.barcode=? AND p.active=1", (bc.strip(),)).fetchone()
    return dict(s) if s else None

def inbound(c, d):
    s = lookup(c, d.get("barcode", ""))
    if not s: raise Err("NOT_FOUND")
    qty = int(d.get("qty") or 1)
    if qty < 1: raise Err("Adet 1 veya fazla olmalı")
    lot = (d.get("lot") or "").strip() or None
    exp = (d.get("expiry") or "").strip() or None
    if s["track_expiry"] and not exp: raise Err("Bu ürün için SKT zorunlu")
    if exp:
        try: datetime.strptime(exp, "%Y-%m-%d")
        except ValueError: raise Err("SKT biçimi YYYY-AA-GG olmalı")
    b = c.execute("SELECT id FROM batches WHERE sku_id=? AND IFNULL(lot,'')=IFNULL(?,'') AND IFNULL(expiry,'')=IFNULL(?,'')", (s["id"], lot, exp)).fetchone()
    if b:
        bid = b["id"]; c.execute("UPDATE batches SET qty=qty+? WHERE id=?", (qty, bid))
    else:
        bid = c.execute("INSERT INTO batches(sku_id,lot,expiry,qty,attrs,created) VALUES(?,?,?,?,?,?)",
                        (s["id"], lot, exp, qty, json.dumps(d.get("attrs") or {}, ensure_ascii=False), now())).lastrowid
    mid = c.execute("INSERT INTO movements(type,sku_id,batch_id,qty,ref,note,ts) VALUES('in',?,?,?,?,?,?)",
                    (s["id"], bid, qty, d.get("ref"), d.get("note"), now())).lastrowid
    return {"movement_id": mid, "product": s["pname"], "sku": s["name"], "qty": qty, "lot": lot, "expiry": exp, "weight_g": s["weight_g"]}

def outbound(c, d):
    # Tek okutma = tek adet. Çoklu çıkış yok. FEFO: en yakın SKT önce.
    ref = (d.get("ref") or "").strip()
    if not ref: raise Err("Sipariş numarası zorunlu")
    s = lookup(c, d.get("barcode", ""))
    if not s: raise Err("Barkod tanımlı değil")
    today = date.today().isoformat()
    b = c.execute("SELECT * FROM batches WHERE sku_id=? AND qty>0 AND (expiry IS NULL OR expiry>=?) ORDER BY expiry IS NULL, expiry, id LIMIT 1", (s["id"], today)).fetchone()
    if not b:
        tot = c.execute("SELECT COALESCE(SUM(qty),0) FROM batches WHERE sku_id=?", (s["id"],)).fetchone()[0]
        raise Err("Stok yok" if tot == 0 else "Kalan stok SKT'si geçmiş - çıkış engellendi")
    c.execute("UPDATE batches SET qty=qty-1 WHERE id=?", (b["id"],))
    mid = c.execute("INSERT INTO movements(type,sku_id,batch_id,qty,ref,note,ts) VALUES('out',?,?,1,?,?,?)", (s["id"], b["id"], ref, d.get("note"), now())).lastrowid
    left = c.execute("SELECT COALESCE(SUM(qty),0) FROM batches WHERE sku_id=?", (s["id"],)).fetchone()[0]
    return {"movement_id": mid, "product": s["pname"], "sku": s["name"], "lot": b["lot"], "expiry": b["expiry"], "left": left, "weight_g": s["weight_g"]}

def undo(c, mid):
    m = c.execute("SELECT * FROM movements WHERE id=?", (mid,)).fetchone()
    if not m or m["undone"]: raise Err("Hareket geri alınamaz")
    if m["type"] not in ("in", "out"): raise Err("Bu hareket geri alınamaz")
    delta = m["qty"] if m["type"] == "out" else -m["qty"]
    b = c.execute("SELECT qty FROM batches WHERE id=?", (m["batch_id"],)).fetchone()
    if b["qty"] + delta < 0: raise Err("Geri alma stoğu negatife düşürür")
    c.execute("UPDATE batches SET qty=qty+? WHERE id=?", (delta, m["batch_id"]))
    c.execute("UPDATE movements SET undone=1 WHERE id=?", (mid,))
    c.execute("INSERT INTO movements(type,sku_id,batch_id,qty,ref,note,ts) VALUES('undo',?,?,?,?,?,?)",
              (m["sku_id"], m["batch_id"], m["qty"], m["ref"], f"#{mid} geri alındı", now()))
    return {"ok": True}

def adjust(c, d):
    reason = (d.get("reason") or "").strip()
    if not reason: raise Err("Gerekçe zorunlu")
    b = c.execute("SELECT * FROM batches WHERE id=?", (int(d["batch_id"]),)).fetchone()
    if not b: raise Err("Parti bulunamadı")
    new = int(d["new_qty"])
    if new < 0: raise Err("Adet negatif olamaz")
    c.execute("UPDATE batches SET qty=? WHERE id=?", (new, b["id"]))
    c.execute("INSERT INTO movements(type,sku_id,batch_id,qty,ref,note,ts) VALUES('adj',?,?,?,?,?,?)",
              (b["sku_id"], b["id"], new - b["qty"], d.get("kind") or "sayim", reason, now()))
    return {"ok": True, "delta": new - b["qty"]}

def dashboard(c):
    today = date.today(); soon = (today + timedelta(days=30)).isoformat(); t = today.isoformat()
    base = "SELECT b.*, s.barcode, s.name sname, p.name pname FROM batches b JOIN skus s ON s.id=b.sku_id JOIN products p ON p.id=s.product_id WHERE b.qty>0 AND p.active=1 AND b.expiry"
    low = [p for p in products_list(c) if p["stock"] <= p["min_stock"]]
    return {"product_count": c.execute("SELECT COUNT(*) FROM products WHERE active=1").fetchone()[0],
            "total_stock": c.execute("SELECT COALESCE(SUM(qty),0) FROM batches").fetchone()[0],
            "expired": rows(c, base + "<? ORDER BY b.expiry", (t,)),
            "expiring": rows(c, base + ">=? AND b.expiry<=? ORDER BY b.expiry", (t, soon)),
            "low": [{"id": p["id"], "name": p["name"], "stock": p["stock"], "min_stock": p["min_stock"]} for p in low],
            "today_in": c.execute("SELECT COALESCE(SUM(qty),0) FROM movements WHERE type='in' AND undone=0 AND ts>=?", (t,)).fetchone()[0],
            "today_out": c.execute("SELECT COALESCE(SUM(qty),0) FROM movements WHERE type='out' AND undone=0 AND ts>=?", (t,)).fetchone()[0]}

def csv_out(header, data):
    f = io.StringIO(); w = csv.writer(f, delimiter=";"); w.writerow(header); w.writerows(data)
    return ("\ufeff" + f.getvalue()).encode("utf-8")

def import_csv(c, text):
    # sütunlar: urun;marka;kategori;varyant;barkod;gramaj
    rd = csv.DictReader(io.StringIO(text), delimiter=";" if ";" in text.split("\n")[0] else ",")
    n = 0
    for r in rd:
        r = {(k or "").strip().lower(): (v or "").strip() for k, v in r.items()}
        name, bc = r.get("urun") or r.get("ürün"), r.get("barkod")
        if not name or not bc: continue
        ex = c.execute("SELECT id FROM skus WHERE barcode=?", (bc,)).fetchone()
        if ex: continue
        p = c.execute("SELECT id FROM products WHERE name=? AND active=1", (name,)).fetchone()
        sku = {"name": r.get("varyant") or "Standart", "barcode": bc, "weight_g": (r.get("gramaj") or "").replace(",", ".") or None}
        if p: save_product(c, {"name": name, "skus": [sku]}, None) if False else c.execute(
            "INSERT INTO skus(product_id,name,barcode,weight_g) VALUES(?,?,?,?)", (p["id"], sku["name"], bc, float(sku["weight_g"]) if sku["weight_g"] else None))
        else: save_product(c, {"name": name, "brand": r.get("marka"), "category": r.get("kategori"), "skus": [sku]})
        n += 1
    return {"imported": n}

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def send(self, code, body, ctype="application/json", extra=None):
        if isinstance(body, (dict, list)): body = json.dumps(body, ensure_ascii=False).encode()
        elif isinstance(body, str): body = body.encode()
        self.send_response(code); self.send_header("Content-Type", ctype + ("; charset=utf-8" if "text" in ctype or "json" in ctype else ""))
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items(): self.send_header(k, v)
        self.end_headers(); self.wfile.write(body)

    def handle_any(self, method):
        u = urlparse(self.path); path = u.path; qs = {k: v[0] for k, v in parse_qs(u.query).items()}
        n = int(self.headers.get("Content-Length") or 0); raw = self.rfile.read(n) if n else b""
        try:
            if method == "GET" and path == "/": return self.send(200, open(os.path.join(BASE, "index.html"), encoding="utf-8").read(), "text/html")
            if method == "GET" and path.startswith("/img/"):
                f = os.path.join(IMG, os.path.basename(path)); 
                if not os.path.exists(f): return self.send(404, "")
                ext = f.rsplit(".", 1)[-1].lower()
                return self.send(200, open(f, "rb").read(), {"png": "image/png", "webp": "image/webp", "gif": "image/gif"}.get(ext, "image/jpeg"), {"Cache-Control": "max-age=3600"})
            if method != "GET": STATE["dirty"] = True
            if method == "GET" and path == "/api/backup/list":
                fs = sorted(os.listdir(BK), reverse=True)
                return self.send(200, {"files": [{"name": f, "kb": os.path.getsize(os.path.join(BK, f)) // 1024} for f in fs], "last": STATE["last"], "mirror_dir": cfg().get("mirror_dir", ""), "mirror_err": STATE["mirror_err"], "data_dir": DATA})
            if method == "GET" and path == "/api/backup/download":
                f = os.path.join(BK, os.path.basename(qs.get("name", "")))
                if not os.path.exists(f): return self.send(404, "")
                return self.send(200, open(f, "rb").read(), "application/zip", {"Content-Disposition": "attachment; filename=" + os.path.basename(f)})
            if method == "POST" and path == "/api/backup/now": return self.send(200, {"name": make_backup("elle")})
            if method == "POST" and path == "/api/backup/config":
                j = json.loads(raw); json.dump({"mirror_dir": (j.get("mirror_dir") or "").strip()}, open(CFG, "w", encoding="utf-8")); return self.send(200, {"name": make_backup("elle")})
            if method == "POST" and path == "/api/backup/restore":
                tf = os.path.join(tempfile.mkdtemp(), "r.zip"); open(tf, "wb").write(raw); restore_zip(tf); return self.send(200, {"ok": True})
            if method == "POST" and path == "/api/backup/restore-local":
                restore_zip(os.path.join(BK, os.path.basename(json.loads(raw)["name"]))); return self.send(200, {"ok": True})
            data = json.loads(raw) if raw and self.headers.get("Content-Type", "").startswith("application/json") else {}
            with LOCK, db() as c:
                if method == "GET":
                    if path == "/api/products": return self.send(200, products_list(c))
                    if path == "/api/lookup":
                        s = lookup(c, qs.get("barcode", "")); return self.send(200, s or {"error": "NOT_FOUND"}, ) if s else self.send(404, {"error": "NOT_FOUND"})
                    if path == "/api/dashboard": return self.send(200, dashboard(c))
                    if path == "/api/fields": return self.send(200, rows(c, "SELECT * FROM fields ORDER BY scope,name"))
                    if path == "/api/batches":
                        return self.send(200, rows(c, "SELECT b.*, s.barcode, s.name sname, p.name pname FROM batches b JOIN skus s ON s.id=b.sku_id JOIN products p ON p.id=s.product_id WHERE b.qty>0 ORDER BY b.expiry IS NULL, b.expiry"))
                    if path == "/api/movements":
                        q = "SELECT m.*, s.barcode, p.name pname, s.name sname, b.lot, b.expiry FROM movements m LEFT JOIN skus s ON s.id=m.sku_id LEFT JOIN products p ON p.id=s.product_id LEFT JOIN batches b ON b.id=m.batch_id"
                        a = []
                        if qs.get("type"): q += " WHERE m.type=?"; a.append(qs["type"])
                        if qs.get("ref"): q += (" AND" if a else " WHERE") + " m.ref LIKE ?"; a.append("%" + qs["ref"] + "%")
                        return self.send(200, rows(c, q + " ORDER BY m.id DESC LIMIT ?", a + [int(qs.get("limit", 200))]))
                    if path == "/api/export/stok.csv":
                        d = [[b["pname"], b["sname"], b["barcode"], b["lot"], b["expiry"], b["qty"]] for b in rows(c, "SELECT b.*, s.barcode, s.name sname, p.name pname FROM batches b JOIN skus s ON s.id=b.sku_id JOIN products p ON p.id=s.product_id WHERE b.qty>0")]
                        return self.send(200, csv_out(["Ürün", "Varyant", "Barkod", "Parti", "SKT", "Adet"], d), "text/csv", {"Content-Disposition": "attachment; filename=stok.csv"})
                    if path == "/api/export/hareketler.csv":
                        d = [[m["ts"], m["type"], m["pname"], m["sname"], m["barcode"], m["lot"], m["expiry"], m["qty"], m["ref"], m["undone"]] for m in rows(c, "SELECT m.*, s.barcode, p.name pname, s.name sname, b.lot, b.expiry FROM movements m LEFT JOIN skus s ON s.id=m.sku_id LEFT JOIN products p ON p.id=s.product_id LEFT JOIN batches b ON b.id=m.batch_id ORDER BY m.id")]
                        return self.send(200, csv_out(["Zaman", "Tip", "Ürün", "Varyant", "Barkod", "Parti", "SKT", "Adet", "Referans", "Geri alındı"], d), "text/csv", {"Content-Disposition": "attachment; filename=hareketler.csv"})
                elif method == "POST":
                    if path == "/api/products": return self.send(200, {"id": save_product(c, data)})
                    if path == "/api/inbound": return self.send(200, inbound(c, data))
                    if path == "/api/outbound": return self.send(200, outbound(c, data))
                    if path == "/api/adjust": return self.send(200, adjust(c, data))
                    if path == "/api/import": return self.send(200, import_csv(c, raw.decode("utf-8-sig")))
                    if path == "/api/fields":
                        c.execute("INSERT OR IGNORE INTO fields(scope,name) VALUES(?,?)", (data["scope"], data["name"].strip())); return self.send(200, {"ok": True})
                    m = re.match(r"/api/undo/(\d+)$", path)
                    if m: return self.send(200, undo(c, int(m[1])))
                    m = re.match(r"/api/products/(\d+)/image$", path)
                    if m:
                        ext = {"image/png": "png", "image/webp": "webp", "image/gif": "gif"}.get(self.headers.get("Content-Type"), "jpg")
                        fn = f"{uuid.uuid4().hex}.{ext}"; open(os.path.join(IMG, fn), "wb").write(raw)
                        c.execute("UPDATE products SET image=? WHERE id=?", (fn, int(m[1]))); return self.send(200, {"image": fn})
                elif method == "PUT":
                    m = re.match(r"/api/products/(\d+)$", path)
                    if m: save_product(c, data, int(m[1])); return self.send(200, {"ok": True})
                elif method == "DELETE":
                    m = re.match(r"/api/products/(\d+)$", path)
                    if m: c.execute("UPDATE products SET active=0 WHERE id=?", (int(m[1]),)); return self.send(200, {"ok": True})
                    m = re.match(r"/api/fields/(\d+)$", path)
                    if m: c.execute("DELETE FROM fields WHERE id=?", (int(m[1]),)); return self.send(200, {"ok": True})
            self.send(404, {"error": "bulunamadı"})
        except Err as e: self.send(400, {"error": str(e)})
        except sqlite3.IntegrityError as e: self.send(400, {"error": "Veri hatası: " + str(e)})
        except Exception as e: self.send(500, {"error": repr(e)})

    def do_GET(self): self.handle_any("GET")
    def do_POST(self): self.handle_any("POST")
    def do_PUT(self): self.handle_any("PUT")
    def do_DELETE(self): self.handle_any("DELETE")

def already_running(port):
    try:
        import urllib.request
        return "data_dir" in json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/backup/list", timeout=2))
    except Exception: return False

if __name__ == "__main__":
    for st in (sys.stdout, sys.stderr):
        try: st.reconfigure(encoding="utf-8", errors="replace")
        except Exception: pass
    srv, port = None, PORT
    for p in range(PORT, PORT + 20):
        try: srv = ThreadingHTTPServer(("127.0.0.1", p), H); port = p; break
        except OSError:
            if already_running(p):
                print("Uygulama zaten açık, tarayıcıda açılıyor…"); webbrowser.open(f"http://127.0.0.1:{p}"); sys.exit(0)
    if not srv: sys.exit("Boş port bulunamadı")
    url = f"http://127.0.0.1:{port}"
    print("Stok Yonetimi calisiyor:", url)
    print("Veri klasoru:", DATA)
    print("Bu pencereyi KAPATMAYIN. Kapatmak icin pencereyi kapatin (kapanista otomatik yedek alinir).")
    try: make_backup("acilis")
    except Exception as e: print("acilis yedegi alinamadi:", e)
    threading.Thread(target=bg_backup, daemon=True).start()
    if "--no-browser" not in sys.argv: threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try: srv.serve_forever()
    except KeyboardInterrupt: pass
    try: make_backup("kapanis")
    except Exception: pass
