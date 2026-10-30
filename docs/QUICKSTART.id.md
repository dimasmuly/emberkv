# Panduan memasang EmberKV di aplikasimu

*English: [QUICKSTART.md](QUICKSTART.md)*

EmberKV berjalan sebagai service terpisah di samping backend aplikasimu. Ia menyimpan data kecil yang sering dipakai dan berumur pendek: session login, OTP, rate limit, kunci, dan cache. Tidak perlu file konfigurasi: satu environment variable sudah cukup untuk menyalakannya dengan aman.

**Yang dibutuhkan:** Docker, dan backend aplikasi dalam bahasa apa saja.

## Langkah 1: Buat token rahasia

Token ini berfungsi seperti password antara aplikasimu dan EmberKV.

```bash
openssl rand -hex 24
# contoh hasil: 3f9a1c...e07b  (minimal 16 karakter)
```

Simpan di file `.env` aplikasimu:

```env
EMBERKV_URL=http://127.0.0.1:8080
EMBERKV_TOKEN=3f9a1c...e07b
```

## Langkah 2: Jalankan server EmberKV

**Cara A, satu perintah:**

```bash
docker run -d --name emberkv --restart unless-stopped \
  -p 127.0.0.1:8080:8080 \
  -v emberkv-data:/data \
  -e EMBERKV_ADMIN_TOKEN=3f9a1c...e07b \
  ghcr.io/dimasmuly/emberkv:latest
```

**Cara B, kalau aplikasimu sudah memakai Docker Compose.** Tambahkan ke `compose.yaml`:

```yaml
services:
  emberkv:
    image: ghcr.io/dimasmuly/emberkv:latest
    restart: unless-stopped
    environment:
      EMBERKV_ADMIN_TOKEN: ${EMBERKV_TOKEN}
    volumes: [emberkv-data:/data]
    ports: ["127.0.0.1:8080:8080"]

volumes:
  emberkv-data:
```

Lalu jalankan `docker compose up -d emberkv`.

Penjelasan bagian-bagiannya:

* `127.0.0.1:8080` berarti server hanya bisa diakses dari mesin yang sama. EmberKV memakai HTTP biasa, jadi jangan dibuka ke internet.
* `-v emberkv-data:/data` menyimpan data di volume Docker, sehingga data tidak hilang kalau container di-restart atau di-update.
* `EMBERKV_ADMIN_TOKEN` mengaktifkan autentikasi. Tanpa token, server **menolak menyala**. Ini disengaja demi keamanan.

## Langkah 3: Pastikan server hidup

```bash
curl localhost:8080/readyz
# {"status":"ready"}

T=3f9a1c...e07b
curl -X PUT "localhost:8080/v1/namespaces/tes/keys/halo?ttl=1m" \
     -H "Authorization: Bearer $T" -d '{"pesan":"berhasil"}'
curl localhost:8080/v1/namespaces/tes/keys/halo -H "Authorization: Bearer $T"
# {"pesan":"berhasil"}
```

Namespace (`tes` di contoh ini) tidak perlu dibuat terlebih dahulu. Ia otomatis ada saat pertama kali ditulisi.

## Langkah 4: Sambungkan dari aplikasimu

**Node.js, Bun, Deno, atau Next.js:**

```bash
npm install @emberkv/client
```

Sebelum paket npm-nya terbit, salin saja file [`sdk/typescript/src/index.ts`](../sdk/typescript/src/index.ts) ke proyekmu, misalnya ke `lib/emberkv.ts`. File itu berdiri sendiri tanpa dependensi.

```ts
import { createKit } from "@emberkv/client"; // atau "./lib/emberkv"

export const kit = createKit({
  baseUrl: process.env.EMBERKV_URL!,
  token: process.env.EMBERKV_TOKEN,
});
```

**PHP/Laravel, Python, Go, dan bahasa lain:** pakai HTTP biasa dengan header `Authorization: Bearer <token>`. Contohnya ada di [QUICKSTART.md](QUICKSTART.md#3-any-other-language-the-http-api), dan referensi lengkapnya di [API.md](API.md).

## Langkah 5: Pakai fiturnya

```ts
// Login: buat session, simpan token di cookie
const cookie = await kit.sessions.create(user.id, { device: "android" });
const session = await kit.sessions.resolve(cookie); // null kalau kedaluwarsa atau palsu
await kit.sessions.destroyAll(user.id);             // logout dari semua perangkat

// Batasi percobaan login: maksimal 5 kali per 15 menit
const { allowed } = await kit.rateLimit.check(`login:${email}`, { limit: 5, window: "15m" });
if (!allowed) return res.status(429);

// Anti dobel bayar: klik dua kali tetap diproses sekali
const { result } = await kit.idempotency.once("bayar", orderId, () => prosesPembayaran());

// "Andi sedang mengedit dokumen ini"
const lock = await kit.leases.acquire(`artikel:${id}`, { id: user.id, name: user.name });

// Cache laporan yang berat: dihitung sekali, dipakai semua server selama 1 menit
const laporan = await kit.cache.readThrough("penjualan:hari-ini", { ttl: "1m", load: hitungLaporan });

// Data bebas lainnya, misalnya OTP yang hilang sendiri setelah 5 menit
await kit.client.namespace("otp").put(`user:${id}`, { kode: "481516" }, { ttl: "5m" });
```

| Fitur | Kalau server EmberKV mati |
|---|---|
| `sessions` | **berhenti** (melempar error): lebih aman daripada membiarkan sembarang orang masuk |
| `rateLimit`, `idempotency`, `leases` | tetap jalan tanpa perlindungan, dengan peringatan di log |
| `cache` | dilewati: data dihitung langsung dari sumbernya |

## Langkah 6: Sebelum masuk produksi

| Hal | Cara |
|---|---|
| **Satu token per aplikasi**, dengan akses terbatas | `docker run --rm --entrypoint ekv ghcr.io/dimasmuly/emberkv token --name toko-online --access read_write --namespaces sessions,limits,cache`, lalu pasang `auth.toml` lewat `EMBERKV_AUTH_FILE` ([INTEGRATION.md](INTEGRATION.md)) |
| **Jangan buka ke internet** | Tetap di `127.0.0.1` atau jaringan privat. Kalau perlu lintas server, pasang TLS di depannya |
| **Backup** | Tambahkan `-v emberkv-backups:/backups -e EMBERKV_BACKUP_DIR=/backups`, lalu panggil `POST /v1/admin/checkpoints` secara terjadwal |
| **Monitoring** | Pakai `/readyz` untuk health check dan port `9464` untuk metrics Prometheus |
| **Jenis data** | Hanya data sementara (session, OTP, cache, kunci). Data bisnis seperti user, order, dan transaksi tetap di PostgreSQL atau MySQL |

## Kalau ada masalah

| Gejala | Penyebab dan solusi |
|---|---|
| Container langsung mati dengan pesan "refusing to listen ... without authentication" | `EMBERKV_ADMIN_TOKEN` belum diisi |
| Pesan "the admin token must be at least 16 characters" | Token terlalu pendek; buat ulang dengan `openssl rand -hex 24` |
| `401 unauthenticated` | Token salah, atau header `Authorization: Bearer` lupa dikirim |
| `403 forbidden` | Token *scoped* tidak punya akses ke namespace itu; tambahkan namespace-nya di `ekv token --namespaces` |
| `docker pull` ditolak (*denied*) | Paket GHCR belum dijadikan public oleh pemilik repo |
