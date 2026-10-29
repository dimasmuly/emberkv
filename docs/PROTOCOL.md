# Wire protocol (v1)

Client and server exchange **frames** over TCP. Every request gets exactly one response, in order. Clients may pipeline requests.

## Framing

```text
+----------------+--------------------------+
| body_len : u32 | body : body_len bytes    |
+----------------+--------------------------+
body = opcode : u8, fields...
```

* All integers are **big-endian**.
* `blob` = `len : u32` followed by `len` bytes.
* `bool` = one byte, `0` or `1`. Any other value is a protocol error.
* Frames larger than the server's limit (default 32 MiB) are rejected based on `body_len` alone, before the body is read.
* Trailing bytes after the last field are a protocol error.

## Requests

| Opcode | Name | Fields |
|---|---|---|
| `0x01` | PING | — |
| `0x02` | GET | `key: blob` |
| `0x03` | PUT | `key: blob`, `value: blob` |
| `0x04` | DELETE | `key: blob` |
| `0x05` | SCAN | `prefix: blob`, `limit: u32` (server may cap it) |
| `0x06` | STATS | — |
| `0x07` | AUTH | `token: blob` |

## Responses

| Opcode | Name | Fields | Reply to |
|---|---|---|---|
| `0x81` | PONG | — | PING |
| `0x82` | OK | — | PUT, AUTH |
| `0x83` | VALUE | `found: bool`, then `value: blob` if found | GET |
| `0x84` | DELETED | `existed: bool` | DELETE |
| `0x85` | ENTRIES | `count: u32`, then `count` × (`key: blob`, `value: blob`) | SCAN |
| `0x86` | STATS | `keys`, `segments`, `total_bytes`, `live_bytes`: `u64` each | STATS |
| `0xFF` | ERROR | `code: u16`, `message: blob` (UTF-8) | any |

## Error codes

| Code | Name | Retry? |
|---|---|---|
| 1 | BAD_REQUEST | No; fix the request |
| 2 | TOO_LARGE | No; shrink the key, value or scan |
| 3 | INTERNAL | Maybe; the server logged details |
| 4 | UNAVAILABLE | Yes, later or against another node |
| 5 | UNAUTHORIZED | No; authenticate with an admin token |

Clients must treat unknown codes as non-retryable.

If the server cannot parse a frame, it sends an ERROR (BAD_REQUEST) and **closes the connection**, because the byte stream may no longer be aligned on frame boundaries.

## Authentication

When the server runs with an auth file, a connection must send `AUTH` with an **admin** token before any request other than `PING`. Until then, requests get `ERROR` (UNAUTHORIZED). A successful `AUTH` returns `OK`. A failed one returns `ERROR` (UNAUTHORIZED) and the server **closes the connection**, which slows down guessing. The binary protocol addresses the raw keyspace across all namespaces, so non-admin keys are not accepted. Application access goes through the HTTP API ([API.md](API.md)).

With authentication disabled, `AUTH` always succeeds.

## Example

`PUT "k" = "v"`:

```text
00 00 00 0b                 body_len = 11
03                          PUT
00 00 00 01 6b              key   = "k"
00 00 00 01 76              value = "v"
```

Reply:

```text
00 00 00 01  82             OK
```

## Hardening notes

* `ENTRIES.count` is checked against the bytes actually left in the frame (each entry needs ≥ 8) **before** allocating, so a 5-byte frame cannot request a 4-billion-element `Vec`.
* Decoding is zero-copy: `key` and `value` are `Bytes` slices of the receive buffer.
* `encoded_len()` is exact (checked by a property test). The encoder uses it to enforce the frame limit before writing anything and to reserve buffer space in one step.
