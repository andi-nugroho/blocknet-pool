<h1 align="center">Blocknet Pool (Rust)</h1>

<p align="center">
  Blocknet mining pool server in Rust (stratum + API + payouts).
</p>

<p align="center">
  <img src="https://img.shields.io/badge/status-testing%2Fdev-aaff00?style=flat-square&labelColor=000" alt="Status">
  <img src="https://img.shields.io/badge/language-rust-aaff00?style=flat-square&labelColor=000" alt="Rust">
  <img src="https://img.shields.io/badge/license-BSD--3--Clause-aaff00?style=flat-square&labelColor=000" alt="License">
</p>

## Quick Start

```bash
cargo build --release
cargo run -- init
# edit config.json
# edit .env and set BLOCKNET_WALLET_PASSWORD
cargo run --release
```

Custom config:

```bash
cargo run --release -- --config /path/to/config.json
```

## Database Backend

Default backend is SQLite (`database_path`).

To use Postgres, set `database_url` in `config.json`, for example:

```json
{
  "database_url": "postgres://user:password@127.0.0.1:5432/blocknet_pool"
}
```

When `database_url` is set, Postgres is used automatically and `database_path` is ignored.

## Runtime Components

- Stratum server
- Template/job manager
- Validation engine (bounded queues, candidate priority)
- Persistent storage (SQLite)
- Payout processor
- HTTP API

## Core API Endpoints

- `GET /api/stats`
- `GET /api/miners`
- `GET /api/miner/{address}`
- `GET /api/blocks`
- `GET /api/payouts`

## Stratum Notes

- Login supports protocol negotiation (`protocol_version`, `capabilities`)
- Submit supports legacy and v2 payloads
- Candidate shares are prioritized in validation
- Queue pressure behavior preserves legacy inline handling for no-claimed-hash submits

## Daemon Requirements

The pool expects a running Blocknet daemon API with mining + wallet routes enabled.

At minimum:

- `GET /api/status`
- `GET /api/mining/blocktemplate`
- `POST /api/mining/submitblock`
- `GET /api/block/{id}`
- `GET /api/wallet/address`
- `GET /api/wallet/balance`
- `POST /api/wallet/send`
- `POST /api/wallet/load`
- `POST /api/wallet/unlock`

## Notes

- This repository no longer uses Go; runtime is Rust-only.