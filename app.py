#!/usr/bin/env python3
"""Stok Yönetimi - yerel, bağımsız (sadece Python standart kütüphanesi). Çalıştır: python3 app.py"""
import csv, hashlib, io, json, os, re, secrets, shutil, socket, ssl, sqlite3, sys, tempfile, threading, time, uuid, webbrowser, zipfile
from datetime import date, datetime, timedelta, timezone
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

def save_cfg(upd):
    c = cfg(); c.update(upd)
    json.dump(c, open(CFG, "w", encoding="utf-8"), ensure_ascii=False)

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
    return {"movement_id": mid, "product": s["pname"], "sku": s["name"], "qty": qty, "lot": lot, "expiry": exp, "weight_g": s["weight_g"], "image": s["image"]}

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
    return {"movement_id": mid, "product": s["pname"], "sku": s["name"], "lot": b["lot"], "expiry": b["expiry"], "left": left, "weight_g": s["weight_g"], "image": s["image"], "barcode": s["barcode"]}

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

def order_undo(c, d):
    ref = (d.get("ref") or "").strip(); s = lookup(c, d.get("barcode", ""))
    if not ref: raise Err("Sipariş numarası zorunlu")
    if not s: raise Err("Barkod tanımlı değil")
    m = c.execute("SELECT id FROM movements WHERE type='out' AND undone=0 AND ref=? AND sku_id=? ORDER BY id DESC LIMIT 1", (ref, s["id"])).fetchone()
    if not m: raise Err("Bu siparişte bu üründen geri alınacak çıkış yok")
    undo(c, m["id"])
    return {"product": s["pname"], "sku": s["name"], "image": s["image"]}

def _norm(h): return (h or "").strip().lower().translate(str.maketrans("üıöşçğâî", "uioscgai"))

def import_csv(c, text):
    text = text.lstrip("\ufeff")
    first = text.split("\n")[0]
    rd = csv.reader(io.StringIO(text), delimiter=";" if first.count(";") >= first.count(",") else ",")
    head = next(rd, None)
    if not head: raise Err("Boş dosya")
    keys = [_norm(h) for h in head]
    res = {"imported": 0, "skipped": 0, "errors": []}
    for ln, row in enumerate(rd, start=2):
        if not any(x.strip() for x in row): continue
        r = {}; pa = {}; sa = {}
        for h, k, v in zip(head, keys, row):
            v = v.strip()
            if v.startswith('="') and v.endswith('"'): v = v[2:-1]
            if k.startswith("urun_ozel:"):
                if v: pa[h.split(":", 1)[1].strip()] = v
            elif k.startswith("varyant_ozel:"):
                if v: sa[h.split(":", 1)[1].strip()] = v
            else: r[k] = v
        name, bc = r.get("urun"), r.get("barkod")
        if not name or not bc: res["errors"].append(f"Satır {ln}: ürün adı ve barkod zorunlu"); continue
        if c.execute("SELECT 1 FROM skus WHERE barcode=?", (bc,)).fetchone(): res["skipped"] += 1; continue
        try: w = float(r["gramaj"].replace(",", ".")) if r.get("gramaj") else None
        except ValueError: res["errors"].append(f"Satır {ln}: gramaj sayı olmalı"); continue
        for scope, a in (("product", pa), ("sku", sa)):
            for fname in a: c.execute("INSERT OR IGNORE INTO fields(scope,name) VALUES(?,?)", (scope, fname))
        sku = (r.get("varyant") or "Standart", bc, w, json.dumps(sa, ensure_ascii=False))
        p = c.execute("SELECT id FROM products WHERE name=? AND active=1", (name,)).fetchone()
        if p: c.execute("INSERT INTO skus(product_id,name,barcode,weight_g,attrs) VALUES(?,?,?,?,?)", (p["id"],) + sku)
        else:
            skt = r.get("skt_takibi", "").lower() not in ("hayir", "hayır", "0", "yok", "no")
            try: ms = int(float(r.get("min_stok") or 0))
            except ValueError: ms = 0
            pid = c.execute("INSERT INTO products(name,brand,category,unit,min_stock,track_expiry,attrs,notes,created) VALUES(?,?,?,?,?,?,?,?,?)",
                (name, r.get("marka"), r.get("kategori"), r.get("birim") or "adet", ms, 1 if skt else 0, json.dumps(pa, ensure_ascii=False), r.get("notlar"), now())).lastrowid
            c.execute("INSERT INTO skus(product_id,name,barcode,weight_g,attrs) VALUES(?,?,?,?,?)", (pid,) + sku)
        res["imported"] += 1
    return res

def xl(v):
    """Excel barkodu bilimsel gösterime çevirmesin diye ="..." biçimi (içe aktarma bunu geri çözer)."""
    return f'="{v}"' if v not in (None, "") else ""

def export_products(c):
    fl = [f for f in rows(c, "SELECT * FROM fields ORDER BY scope,name") if f["scope"] in ("product", "sku")]
    head = ["urun", "marka", "kategori", "varyant", "barkod", "gramaj", "birim", "min_stok", "skt_takibi", "notlar",
            "mevcut_stok", "urun_toplam_stok", "stok_durumu", "en_yakin_skt", "parti_sayisi", "skt_gecmis_adet", "gorsel"]
    head += [("urun_ozel:" if f["scope"] == "product" else "varyant_ozel:") + f["name"] for f in fl]
    t = date.today().isoformat(); out = []
    for p in products_list(c):
        durum = "Stokta yok" if p["stock"] == 0 else ("Düşük" if p["stock"] <= p["min_stock"] else "Yeterli")
        for s in p["skus"]:
            b = rows(c, "SELECT expiry, qty FROM batches WHERE sku_id=? AND qty>0", (s["id"],))
            near = min([x["expiry"] for x in b if x["expiry"]], default="")
            exp_q = sum(x["qty"] for x in b if x["expiry"] and x["expiry"] < t)
            r = [p["name"], p["brand"], p["category"], s["name"], xl(s["barcode"]), s["weight_g"], p["unit"], p["min_stock"],
                 "evet" if p["track_expiry"] else "hayir", p["notes"], s["stock"], p["stock"], durum, near, len(b), exp_q, "evet" if p["image"] else "hayir"]
            r += [(p["attrs"] if f["scope"] == "product" else s["attrs"]).get(f["name"], "") for f in fl]
            out.append(r)
    return csv_out(head, out)

def export_batches(c):
    t = date.today()
    out = []
    for b in rows(c, "SELECT b.*, s.barcode, s.name sname, s.weight_g, p.name pname, p.brand, p.category, p.unit FROM batches b JOIN skus s ON s.id=b.sku_id JOIN products p ON p.id=s.product_id WHERE b.qty>0 AND p.active=1 ORDER BY b.expiry IS NULL, b.expiry, p.name"):
        left = (date.fromisoformat(b["expiry"]) - t).days if b["expiry"] else ""
        st = "" if left == "" else ("SKT geçmiş" if left < 0 else ("30 gün içinde" if left <= 30 else "Normal"))
        out.append([b["pname"], b["brand"], b["category"], b["sname"], xl(b["barcode"]), b["weight_g"], b["unit"], b["lot"], b["expiry"], left, st, b["qty"], b["created"]])
    return csv_out(["Ürün", "Marka", "Kategori", "Varyant", "Barkod", "Gramaj (g)", "Birim", "Parti", "SKT", "Kalan gün", "SKT durumu", "Adet", "İlk giriş"], out)

def export_movements(c):
    ms = rows(c, "SELECT m.*, s.barcode, s.name sname, p.name pname, p.brand, p.category, b.lot, b.expiry FROM movements m LEFT JOIN skus s ON s.id=m.sku_id LEFT JOIN products p ON p.id=s.product_id LEFT JOIN batches b ON b.id=m.batch_id ORDER BY m.id")
    types = {m["id"]: m["type"] for m in ms}; out = []
    for m in ms:
        tr = {"in": "Giriş", "out": "Çıkış", "undo": "Geri alma", "adj": "Düzeltme"}.get(m["type"], m["type"])
        if m["type"] == "in": eff = m["qty"]
        elif m["type"] == "out": eff = -m["qty"]
        elif m["type"] == "adj": eff = m["qty"]
        else:
            mm = re.match(r"#(\d+)", m["note"] or ""); orig = types.get(int(mm[1])) if mm else None
            eff = m["qty"] if orig == "out" else -m["qty"]
        ref, note = (m["ref"], m["note"]) if m["type"] != "adj" else ("", f"{m['ref']}: {m['note']}")
        out.append([m["ts"], tr, m["pname"], m["brand"], m["category"], m["sname"], xl(m["barcode"]), m["lot"], m["expiry"], m["qty"], eff, ref, note, "evet" if m["undone"] else ""])
    return csv_out(["Zaman", "Tür", "Ürün", "Marka", "Kategori", "Varyant", "Barkod", "Parti", "SKT", "Adet", "Stok etkisi", "Sipariş / referans", "Not", "Geri alındı"], out)

# ---------------- Ağ modu (telefon): HTTPS + PIN ----------------
LAN = {"srv": None, "port": None, "err": None, "tls": False, "ips": []}
SESS = {}; FAILS = {}
LOGIN_HTML = """<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Giriş</title>
<style>body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;background:#0f172a;color:#fff;display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0}
.b{background:#1e293b;padding:24px;border-radius:20px;width:min(94vw,340px)}h2{margin:0 0 6px}p{margin:0 0 12px;color:#94a3b8;font-size:13px}
input{width:100%;padding:14px;font-size:22px;border-radius:12px;border:0;text-align:center;letter-spacing:6px;box-sizing:border-box}
.k{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:12px}.k button{padding:16px 0;font-size:22px;border:0;border-radius:12px;background:#334155;color:#fff}
.k button:active{background:#475569}button.go{width:100%;margin-top:10px;padding:15px;font-size:17px;border:0;border-radius:12px;background:#6366f1;color:#fff;font-weight:600}#e{color:#fca5a5;margin-top:10px;min-height:20px;text-align:center}</style>
<div class=b><h2>Stok Yönetimi</h2><p>PIN’i yazın, yapıştırın ya da tuşları kullanın</p><input id=pin type=password autocomplete=off autocapitalize=off>
<div class=k></div><button class=go onclick=go()>Giriş</button><div id=e></div></div>
<script>const P=document.getElementById('pin'),K=document.querySelector('.k');
['1','2','3','4','5','6','7','8','9','C','0','⌫'].forEach(t=>{const b=document.createElement('button');b.textContent=t;b.type='button';b.onclick=()=>{if(t=='C')P.value='';else if(t=='⌫')P.value=P.value.slice(0,-1);else P.value+=t};K.appendChild(b)});
P.onkeydown=ev=>{if(ev.key=='Enter')go()};
async function go(){const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({pin:P.value})});if(r.ok)location.reload();else{document.getElementById('e').textContent=(await r.json()).error;P.value=''}}</script>"""

def hash_pin(pin, salt): return hashlib.pbkdf2_hmac("sha256", pin.encode(), bytes.fromhex(salt), 120000).hex()

def local_ips():
    found = []
    for target in ("8.8.8.8", "10.255.255.255", "172.20.10.1"):
        try:
            t = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); t.connect((target, 9)); found.append(t.getsockname()[0]); t.close()
        except Exception: pass
    try:
        for i in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET): found.append(i[4][0])
    except Exception: pass
    out = []
    for ip in found:
        if ip and not ip.startswith(("127.", "169.254.", "0.")) and ip not in out: out.append(ip)
    return out

def ensure_cert(ips):
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    import ipaddress
    cp, kp, mp = (os.path.join(DATA, n) for n in ("cert.pem", "key.pem", "cert.json"))
    known = []
    try: known = json.load(open(mp))["ips"]
    except Exception: pass
    if os.path.exists(cp) and os.path.exists(kp) and all(i in known for i in ips): return cp, kp
    allips = sorted(set(known) | set(ips))
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Stok Yonetimi")])
    san = [x509.DNSName("localhost")] + [x509.IPAddress(ipaddress.ip_address(i)) for i in allips]
    nb = datetime.now(timezone.utc) - timedelta(days=1)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(nb).not_valid_after(nb + timedelta(days=3650)).add_extension(x509.SubjectAlternativeName(san), False).sign(key, hashes.SHA256()))
    open(cp, "wb").write(cert.public_bytes(serialization.Encoding.PEM))
    open(kp, "wb").write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
    json.dump({"ips": allips}, open(mp, "w"))
    return cp, kp

class QuietServer(ThreadingHTTPServer):
    daemon_threads = True
    def handle_error(self, request, client_address): pass

def stop_lan():
    if LAN["srv"]:
        try: LAN["srv"].shutdown(); LAN["srv"].server_close()
        except Exception: pass
    LAN.update(srv=None, port=None)
    SESS.clear()

def start_lan(base_port):
    stop_lan(); LAN.update(err=None, tls=False); LAN["ips"] = local_ips()
    ctx = None
    try:
        cp, kp = ensure_cert(LAN["ips"] or ["127.0.0.1"])
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); ctx.load_cert_chain(cp, kp)
    except Exception as e:
        LAN["err"] = "HTTPS sertifikası üretilemedi (kamera telefonda çalışmayabilir): " + repr(e)
    for p in range(base_port + 1, base_port + 21):
        try: srv = QuietServer(("0.0.0.0", p), HL); break
        except OSError: srv = None
    if not srv: LAN["err"] = "Ağ portu açılamadı"; return
    srv.tls = bool(ctx)
    if ctx: srv.socket = ctx.wrap_socket(srv.socket, server_side=True, do_handshake_on_connect=False); LAN["tls"] = True
    LAN.update(srv=srv, port=p)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

def lan_status():
    c = cfg()
    sch = "https" if LAN["tls"] else "http"
    return {"enabled": bool(LAN["srv"]), "has_pin": bool(c.get("pin_hash")), "tls": LAN["tls"], "err": LAN["err"], "port": LAN["port"],
            "urls": [f"{sch}://{ip}:{LAN['port']}" for ip in (LAN["ips"] if LAN["srv"] else local_ips())] if LAN["srv"] else [], "sessions": len(SESS)}

def lan_set(d):
    c = cfg(); pin = (d.get("pin") or "").strip()
    if pin:
        if len(pin) < 6: raise Err("PIN en az 6 karakter olmalı")
        salt = secrets.token_hex(8); save_cfg({"pin_salt": salt, "pin_hash": hash_pin(pin, salt)}); SESS.clear()
    if d.get("enabled"):
        if not cfg().get("pin_hash"): raise Err("Önce bir PIN belirleyin")
        save_cfg({"lan": True}); start_lan(PORT_ACTUAL[0])
    elif "enabled" in d:
        save_cfg({"lan": False}); stop_lan()
    return lan_status()

PORT_ACTUAL = [PORT]

def branding():
    c = cfg()
    return {"accent": c.get("brand_accent") or "#6366f1", "name": c.get("brand_name") or "Stok Yönetimi", "logo": c.get("brand_logo"), "mode": c.get("brand_mode") or "auto"}

def set_branding(d):
    upd = {}
    if "accent" in d:
        if not re.fullmatch(r"#[0-9a-fA-F]{6}", d["accent"] or ""): raise Err("Renk #RRGGBB biçiminde olmalı")
        upd["brand_accent"] = d["accent"]
    if "name" in d: upd["brand_name"] = (d["name"] or "").strip()[:40]
    if "mode" in d:
        if d["mode"] not in ("auto", "light", "dark"): raise Err("Geçersiz tema")
        upd["brand_mode"] = d["mode"]
    save_cfg(upd); return branding()

def drop_logo():
    old = cfg().get("brand_logo")
    if old:
        try: os.remove(os.path.join(IMG, os.path.basename(old)))
        except OSError: pass
    save_cfg({"brand_logo": None})

class H(BaseHTTPRequestHandler):
    lan = False
    def log_message(self, *a): pass
    def setup(self):
        if getattr(self.server, "tls", False):
            self.request.settimeout(10); self.request.do_handshake()
        super().setup()
    def sid_ok(self):
        for part in (self.headers.get("Cookie") or "").split(";"):
            k, _, v = part.strip().partition("=")
            if k == "sid" and SESS.get(v, 0) > time.time(): return True
        return False
    def send(self, code, body, ctype="application/json", extra=None):
        if isinstance(body, (dict, list)): body = json.dumps(body, ensure_ascii=False).encode()
        elif isinstance(body, str): body = body.encode()
        self.send_response(code); self.send_header("Content-Type", ctype + ("; charset=utf-8" if "text" in ctype or "json" in ctype or "javascript" in ctype else ""))
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items(): self.send_header(k, v)
        self.end_headers(); self.wfile.write(body)

    def handle_any(self, method):
        u = urlparse(self.path); path = u.path; qs = {k: v[0] for k, v in parse_qs(u.query).items()}
        n = int(self.headers.get("Content-Length") or 0); raw = self.rfile.read(n) if n else b""
        try:
            host = self.headers.get("Host", ""); org = self.headers.get("Origin")
            if org and urlparse(org).netloc != host: return self.send(403, {"error": "Geçersiz istek kaynağı"})
            if self.lan:
                ip = self.client_address[0]
                if path == "/api/login" and method == "POST":
                    f = FAILS.get(ip, [0, 0])
                    if f[1] > time.time(): return self.send(429, {"error": "Çok fazla hatalı deneme, 1 dakika bekleyin"})
                    c0 = cfg(); pin = (json.loads(raw or b"{}").get("pin") or "")
                    if c0.get("pin_hash") and secrets.compare_digest(hash_pin(pin, c0["pin_salt"]), c0["pin_hash"]):
                        FAILS.pop(ip, None); tok = secrets.token_urlsafe(24); SESS[tok] = time.time() + 30 * 86400
                        return self.send(200, {"ok": True}, extra={"Set-Cookie": f"sid={tok}; Path=/; HttpOnly; SameSite=Strict; Max-Age=2592000" + ("; Secure" if LAN["tls"] else "")})
                    f[0] += 1
                    if f[0] >= 5: f = [0, time.time() + 60]
                    FAILS[ip] = f; time.sleep(0.5)
                    return self.send(401, {"error": "PIN hatalı"})
                if not self.sid_ok():
                    if method == "GET" and path == "/": return self.send(200, LOGIN_HTML, "text/html")
                    return self.send(401, {"error": "Giriş gerekli"})
                if path.startswith(("/api/lan", "/api/backup/")): return self.send(403, {"error": "Bu işlem yalnızca bilgisayardan yapılabilir"})
            if method == "GET" and path.startswith("/static/"):
                f = os.path.join(BASE, "static", os.path.basename(path))
                if not os.path.exists(f): return self.send(404, "")
                return self.send(200, open(f, "rb").read(), "application/javascript", {"Cache-Control": "max-age=86400"})
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
                j = json.loads(raw); save_cfg({"mirror_dir": (j.get("mirror_dir") or "").strip()}); return self.send(200, {"name": make_backup("elle")})
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
                    if path == "/api/lan": return self.send(200, lan_status())
                    if path == "/api/orders/recent": return self.send(200, rows(c, "SELECT ref, COUNT(*) n, SUM(undone) undone, MAX(ts) last FROM movements WHERE type='out' AND ref IS NOT NULL GROUP BY ref ORDER BY MAX(id) DESC LIMIT 15"))
                    if path == "/api/export/urunler.csv": return self.send(200, export_products(c), "text/csv", {"Content-Disposition": "attachment; filename=urunler.csv"})
                    if path == "/api/fields": return self.send(200, rows(c, "SELECT * FROM fields ORDER BY scope,name"))
                    if path == "/api/batches":
                        return self.send(200, rows(c, "SELECT b.*, s.barcode, s.name sname, p.id pid, p.category, p.brand, p.image, p.name pname FROM batches b JOIN skus s ON s.id=b.sku_id JOIN products p ON p.id=s.product_id WHERE b.qty>0 AND p.active=1 ORDER BY b.expiry IS NULL, b.expiry"))
                    if path == "/api/movements":
                        q = "SELECT m.*, s.barcode, p.id pid, p.category, p.brand, p.name pname, s.name sname, b.lot, b.expiry FROM movements m LEFT JOIN skus s ON s.id=m.sku_id LEFT JOIN products p ON p.id=s.product_id LEFT JOIN batches b ON b.id=m.batch_id"
                        w, a = [], []
                        if qs.get("type"): w.append("m.type=?"); a.append(qs["type"])
                        if qs.get("ref"): (w.append("m.ref=?"), a.append(qs["ref"])) if qs.get("exact") else (w.append("m.ref LIKE ?"), a.append("%" + qs["ref"] + "%"))
                        if qs.get("from"): w.append("m.ts>=?"); a.append(qs["from"] + " 00:00:00")
                        if qs.get("to"): w.append("m.ts<=?"); a.append(qs["to"] + " 23:59:59")
                        return self.send(200, rows(c, q + (" WHERE " + " AND ".join(w) if w else "") + " ORDER BY m.id DESC LIMIT ?", a + [int(qs.get("limit", 200))]))
                    if path == "/api/export/stok.csv": return self.send(200, export_batches(c), "text/csv", {"Content-Disposition": "attachment; filename=stok-partiler.csv"})
                    if path == "/api/export/hareketler.csv": return self.send(200, export_movements(c), "text/csv", {"Content-Disposition": "attachment; filename=hareketler.csv"})
                    if path == "/api/branding": return self.send(200, branding())
                elif method == "POST":
                    if path == "/api/products": return self.send(200, {"id": save_product(c, data)})
                    if path == "/api/inbound": return self.send(200, inbound(c, data))
                    if path == "/api/outbound": return self.send(200, outbound(c, data))
                    if path == "/api/lan": return self.send(200, lan_set(data))
                    if path == "/api/branding": return self.send(200, set_branding(data))
                    if path == "/api/branding/logo":
                        ext = {"image/png": "png", "image/webp": "webp", "image/gif": "gif"}.get(self.headers.get("Content-Type"), "jpg")
                        drop_logo(); fn = f"logo-{uuid.uuid4().hex[:8]}.{ext}"; open(os.path.join(IMG, fn), "wb").write(raw)
                        save_cfg({"brand_logo": fn}); return self.send(200, branding())
                    if path == "/api/order-undo": return self.send(200, order_undo(c, data))
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
                    if path == "/api/branding/logo": drop_logo(); return self.send(200, branding())
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

class HL(H):
    lan = True

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
        try: srv = QuietServer(("127.0.0.1", p), H); port = p; break
        except OSError:
            if already_running(p):
                print("Uygulama zaten açık, tarayıcıda açılıyor…"); webbrowser.open(f"http://127.0.0.1:{p}"); sys.exit(0)
    if not srv: sys.exit("Boş port bulunamadı")
    url = f"http://127.0.0.1:{port}"; PORT_ACTUAL[0] = port
    if cfg().get("lan") and cfg().get("pin_hash"): start_lan(port)
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
