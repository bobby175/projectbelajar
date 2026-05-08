"""
app.py — Backend Flask Aplikasi Perpustakaan
=============================================
Versi web dari belajarproject3_4.py (customtkinter).

Perbedaan utama dari versi desktop:
- Tidak ada GUI Tkinter sama sekali
- Setiap "halaman" menjadi sebuah route Flask
- Logika yang dulu di fungsi Python sekarang
  dikembalikan sebagai HTML lewat render_template()
- Login memakai Flask session (seperti cookie)
- Ekspor CSV dikirim langsung sebagai file download
"""

import sqlite3
import hashlib
import hmac
import csv
import io
import os
from functools import wraps
from flask import (
    Flask, render_template, request, redirect,
    url_for, session, flash, g, send_file
)

app = Flask(__name__)

# Secret key — baca dari environment variable Railway.
# Kalau tidak ada, pakai default (hanya untuk lokal/testing).
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-ganti-di-railway")

# ── Path database ──
# Simpan di folder yang sama dengan app.py menggunakan path absolut.
# os.path.dirname(__file__) = lokasi app.py, tidak bergantung pada
# direktori kerja saat Railway menjalankan gunicorn.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE  = os.path.join(BASE_DIR, "perpustakaan.db")

LOGIN_USERNAME = "bubby"
# Hash SHA-256 dari password "wulan"
PASSWORD_HASH  = "85232dbc45c9d4b7a42a37df9c8770033dbb661642333f4a6e2c2b4497780c47"


# ══════════════════════════════════════════════
# KONEKSI DATABASE
# Di Flask, koneksi database dibuka per-request
# (bukan sekali saat startup seperti di desktop).
# Flask menyediakan objek 'g' sebagai tempat
# menyimpan data sementara selama satu request.
# ══════════════════════════════════════════════
def get_db():
    """Membuka koneksi database untuk request ini. Buat tabel jika belum ada."""
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row  # hasil query bisa diakses seperti dict
        g.db.execute("""
            CREATE TABLE IF NOT EXISTS buku (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                judul   TEXT NOT NULL,
                penulis TEXT NOT NULL,
                tahun   TEXT NOT NULL
            )
        """)
        g.db.commit()
    return g.db

@app.teardown_appcontext
def close_db(error):
    """Tutup koneksi database di akhir setiap request secara otomatis."""
    db = g.pop("db", None)
    if db is not None:
        db.close()


# ══════════════════════════════════════════════
# HELPER FUNGSI
# ══════════════════════════════════════════════
def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def is_password_valid(password: str) -> bool:
    return hmac.compare_digest(hash_password(password), PASSWORD_HASH)

def is_tahun_valid(tahun: str) -> bool:
    """Tahun harus 4 digit angka antara 1000–2099."""
    return tahun.isdigit() and 1000 <= int(tahun) <= 2099

def login_required(f):
    """
    Decorator untuk melindungi route yang butuh login.
    Jika belum login, user diarahkan ke halaman login.
    Ini menggantikan pengecekan manual di setiap fungsi.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            flash("Silakan login terlebih dahulu.", "warning")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


# ══════════════════════════════════════════════
# ERROR HANDLERS
# Menampilkan halaman yang jelas saat terjadi error
# ══════════════════════════════════════════════
@app.errorhandler(500)
def internal_error(e):
    return render_template("error.html", kode=500,
        pesan="Terjadi kesalahan pada server.",
        detail=str(e)), 500

@app.errorhandler(404)
def not_found(e):
    return render_template("error.html", kode=404,
        pesan="Halaman tidak ditemukan.",
        detail=str(e)), 404


# ══════════════════════════════════════════════
# ROUTE: LOGIN
# GET  /login  → tampilkan form login
# POST /login  → proses login
# ══════════════════════════════════════════════
@app.route("/", methods=["GET", "POST"])
@app.route("/login", methods=["GET", "POST"])
def login():
    """Halaman login. Jika sudah login, langsung ke dashboard."""
    if session.get("logged_in"):
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        if not username:
            flash("Username wajib diisi.", "danger")
        elif not password:
            flash("Password wajib diisi.", "danger")
        elif username.lower() == LOGIN_USERNAME and is_password_valid(password):
            session["logged_in"] = True
            session["username"] = username
            flash(f"Selamat datang, {username}!", "success")
            return redirect(url_for("dashboard"))
        else:
            flash("Username atau password salah.", "danger")

    return render_template("login.html")


@app.route("/logout")
def logout():
    """Hapus session dan kembali ke halaman login."""
    session.clear()
    flash("Anda telah logout.", "info")
    return redirect(url_for("login"))


# ══════════════════════════════════════════════
# ROUTE: DASHBOARD
# GET /dashboard → statistik + 5 buku terakhir
# ══════════════════════════════════════════════
@app.route("/dashboard")
@login_required
def dashboard():
    try:
        db = get_db()
        total_buku    = db.execute("SELECT COUNT(*) FROM buku").fetchone()[0]
        total_penulis = db.execute("SELECT COUNT(DISTINCT penulis) FROM buku").fetchone()[0]
        row           = db.execute("SELECT MIN(tahun), MAX(tahun) FROM buku").fetchone()
        tahun_min     = row[0] or "-"
        tahun_max     = row[1] or "-"
        buku_terbaru  = db.execute(
            "SELECT judul, penulis, tahun FROM buku ORDER BY id DESC LIMIT 5"
        ).fetchall()
    except Exception as e:
        flash(f"Gagal memuat data: {e}", "danger")
        total_buku = total_penulis = 0
        tahun_min = tahun_max = "-"
        buku_terbaru = []

    return render_template("dashboard.html",
        total_buku=total_buku,
        total_penulis=total_penulis,
        tahun_min=tahun_min,
        tahun_max=tahun_max,
        buku_terbaru=buku_terbaru
    )


# ══════════════════════════════════════════════
# ROUTE: DAFTAR BUKU + PENCARIAN
# GET /buku          → semua buku
# GET /buku?q=judul  → filter pencarian
# ══════════════════════════════════════════════
@app.route("/buku")
@login_required
def daftar_buku():
    db      = get_db()
    keyword = request.args.get("q", "").strip()

    if keyword:
        like = f"%{keyword}%"
        data = db.execute(
            "SELECT * FROM buku WHERE judul LIKE ? OR penulis LIKE ? OR tahun LIKE ? ORDER BY id ASC",
            (like, like, like)
        ).fetchall()
    else:
        data = db.execute("SELECT * FROM buku ORDER BY id ASC").fetchall()

    return render_template("daftar_buku.html", buku_list=data, keyword=keyword)


# ══════════════════════════════════════════════
# ROUTE: TAMBAH BUKU
# GET  /buku/tambah → tampilkan form
# POST /buku/tambah → simpan ke database,
#                     TETAP di halaman tambah
#                     (sesuai perilaku versi desktop)
# ══════════════════════════════════════════════
@app.route("/buku/tambah", methods=["GET", "POST"])
@login_required
def tambah_buku():
    buku_terakhir = None   # akan diisi setelah berhasil simpan

    if request.method == "POST":
        judul   = request.form.get("judul", "").strip()
        penulis = request.form.get("penulis", "").strip()
        tahun   = request.form.get("tahun", "").strip()

        error = False
        if not judul:
            flash("Judul buku wajib diisi.", "danger")
            error = True
        if not penulis:
            flash("Penulis buku wajib diisi.", "danger")
            error = True
        if not tahun:
            flash("Tahun terbit wajib diisi.", "danger")
            error = True
        elif not is_tahun_valid(tahun):
            flash("Format tahun tidak valid. Gunakan angka 4 digit (1000–2099).", "danger")
            error = True

        if not error:
            db = get_db()
            db.execute(
                "INSERT INTO buku (judul, penulis, tahun) VALUES (?, ?, ?)",
                (judul.upper(), penulis.upper(), tahun)
            )
            db.commit()
            # Simpan buku terakhir untuk ditampilkan di bawah form
            buku_terakhir = {"judul": judul.upper(), "penulis": penulis.upper(), "tahun": tahun}
            flash("Buku berhasil ditambahkan!", "success")
            # Tidak redirect — tetap di halaman tambah (PRG tidak dipakai di sini)

    return render_template("tambah_buku.html", buku_terakhir=buku_terakhir)


# ══════════════════════════════════════════════
# ROUTE: EDIT BUKU
# GET  /buku/<id>/edit → form berisi data lama
# POST /buku/<id>/edit → update database
# ══════════════════════════════════════════════
@app.route("/buku/<int:id_buku>/edit", methods=["GET", "POST"])
@login_required
def edit_buku(id_buku):
    db   = get_db()
    buku = db.execute("SELECT * FROM buku WHERE id = ?", (id_buku,)).fetchone()

    if not buku:
        flash("Data buku tidak ditemukan.", "danger")
        return redirect(url_for("daftar_buku"))

    if request.method == "POST":
        judul   = request.form.get("judul", "").strip()
        penulis = request.form.get("penulis", "").strip()
        tahun   = request.form.get("tahun", "").strip()

        error = False
        if not judul:
            flash("Judul buku wajib diisi.", "danger"); error = True
        if not penulis:
            flash("Penulis buku wajib diisi.", "danger"); error = True
        if not tahun:
            flash("Tahun terbit wajib diisi.", "danger"); error = True
        elif not is_tahun_valid(tahun):
            flash("Format tahun tidak valid (1000–2099).", "danger"); error = True

        if not error:
            db.execute(
                "UPDATE buku SET judul = ?, penulis = ?, tahun = ? WHERE id = ?",
                (judul.upper(), penulis.upper(), tahun, id_buku)
            )
            db.commit()
            flash("Data buku berhasil diperbarui.", "success")
            return redirect(url_for("daftar_buku"))

    return render_template("edit_buku.html", buku=buku)


# ══════════════════════════════════════════════
# ROUTE: HAPUS BUKU
# POST /buku/<id>/hapus → hapus dari database
# (Pakai POST bukan GET agar tidak bisa
#  dihapus hanya dengan mengunjungi URL)
# ══════════════════════════════════════════════
@app.route("/buku/<int:id_buku>/hapus", methods=["POST"])
@login_required
def hapus_buku(id_buku):
    db = get_db()
    db.execute("DELETE FROM buku WHERE id = ?", (id_buku,))
    db.commit()
    flash("Data buku berhasil dihapus.", "success")
    return redirect(url_for("daftar_buku"))


# ══════════════════════════════════════════════
# ROUTE: EKSPOR CSV
# GET /ekspor-csv → download file CSV langsung
# ══════════════════════════════════════════════
@app.route("/ekspor-csv")
@login_required
def ekspor_csv():
    """
    Mengirim seluruh data buku sebagai file CSV yang
    langsung ter-download di browser / HP user.
    Tidak perlu dialog save-as seperti di versi desktop.
    """
    db   = get_db()
    data = db.execute("SELECT id, judul, penulis, tahun FROM buku ORDER BY id ASC").fetchall()

    # Tulis CSV ke memory (bukan ke file disk)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["No", "Judul", "Penulis", "Tahun"])
    for nomor, row in enumerate(data, start=1):
        writer.writerow([nomor, row["judul"], row["penulis"], row["tahun"]])

    output.seek(0)
    # Encode ke bytes agar bisa dikirim, dengan BOM utf-8-sig untuk Excel
    bytes_output = io.BytesIO(("\ufeff" + output.getvalue()).encode("utf-8"))
    return send_file(
        bytes_output,
        mimetype="text/csv",
        as_attachment=True,
        download_name="data_perpustakaan.csv"
    )


if __name__ == "__main__":
    # debug=True hanya untuk development — matikan di production
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
