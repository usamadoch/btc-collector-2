# btc-collector-2

BTC/USDT Real-time & Backfill Data Collector built with Node.js, TimescaleDB, and Pino logger.

## Features
- Real-time trade streaming via WebSocket
- Automated historical backfill
- TimescaleDB hypertable persistence
- Burst collapsing logger with Pino

## Setup & Installation

1. **Install Dependencies:**
   ```bash
   npm install
   ```

2. **Configure Environment:**
   Copy `.env.example` to `.env` and configure your database settings:
   ```bash
   cp .env.example .env
   ```

3. **Run Collector:**
   - **Development (with live reload):**
     ```bash
     npm run dev
     ```
   - **Production:**
     ```bash
     npm start
     ```
