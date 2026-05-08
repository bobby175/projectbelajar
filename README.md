# Aplikasi Perpustakaan — Flask

Versi web dari aplikasi perpustakaan customtkinter.
Bisa diakses dari HP, tablet, dan komputer lewat browser.

## Struktur Folder

```
perpustakaan_flask/
├── app.py              ← Backend utama (semua route & logika)
├── requirements.txt    ← Daftar library yang dibutuhkan
├── perpustakaan.db     ← Database SQLite (dibuat otomatis)
└── templates/
    ├── base.html       ← Layout dasar + navbar
    ├── login.html      ← Halaman login
    ├── dashboard.html  ← Dashboard + statistik
    ├── daftar_buku.html← Daftar + pencarian buku
    ├── tambah_buku.html← Form tambah buku
    └── edit_buku.html  ← Form edit buku
```

---

## Cara Menjalankan di Komputer (Lokal)

```bash
# 1. Masuk ke folder
cd perpustakaan_flask

# 2. Install Flask
pip install -r requirements.txt

# 3. Jalankan
python app.py
```

Buka browser: http://localhost:5000
Login: username = bubby | password = wulan

---

## Cara Akses dari HP (LAN / WiFi Sama)

1. Jalankan app.py di komputer
2. Cari IP komputer kamu:
   - Windows: `ipconfig` → cari IPv4
   - Mac/Linux: `ifconfig` → cari inet
3. Buka di HP: `http://192.168.x.x:5000`
   (ganti dengan IP komputer kamu)

---

## Deploy ke Internet (Gratis) — Pakai Railway

1. Buat akun di https://railway.app
2. Install Railway CLI atau pakai GitHub
3. Tambahkan file `Procfile`:
   ```
   web: python app.py
   ```
4. Ubah baris terakhir app.py menjadi:
   ```python
   port = int(os.environ.get("PORT", 5000))
   app.run(host="0.0.0.0", port=port)
   ```
5. Push ke GitHub → connect di Railway → Deploy

Setelah deploy, kamu dapat URL seperti:
`https://perpustakaan-xxx.railway.app`
→ bisa dibuka dari HP manapun di seluruh dunia.

---

## Catatan Keamanan Sebelum Deploy

1. Ganti `secret_key` di app.py:
   ```python
   app.secret_key = "buat-string-acak-panjang-di-sini"
   ```
2. Untuk production, gunakan PostgreSQL (bukan SQLite)
   karena Railway mereset filesystem-nya.
3. Simpan secret_key di environment variable Railway,
   jangan ditulis langsung di kode.
