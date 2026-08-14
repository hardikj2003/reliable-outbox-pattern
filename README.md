# Reliable Order Event Processing System

## Phase 1: Project Foundation

### Prerequisites
- Node.js >= 20
- Docker & Docker Compose

### Setup

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Start PostgreSQL:**
   ```bash
   docker compose up -d
   ```

3. **Configure environment:**
   ```bash
   cp .env.example .env
   ```

4. **Generate Prisma client:**
   ```bash
   npm run db:generate
   ```

5. **Create initial migration:**
   ```bash
   npm run db:migrate
   ```
   When prompted, name the migration `init`.

6. **Start development server:**
   ```bash
   npm run dev
   ```

### Health Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness probe — server is running |
| `GET /health/ready` | Readiness probe — database is reachable |

### Project Structure

```
src/
  config/         # Configuration (env, logger, database)
  controllers/    # HTTP request handlers
  routes/         # Express route definitions
  middleware/     # Express middleware
  types/          # TypeScript declarations
  app.ts          # Express application factory
  server.ts       # Application entry point
```

### Architecture Decisions

- **Zod for env validation:** Fail fast on startup if required environment variables are missing or invalid.
- **Pino for structured logging:** JSON logs in production, pretty-printed in development. Every log includes `pid` and `env`.
- **Prisma singleton:** Prevents multiple Prisma Client instances during development hot reload.
- **Request ID middleware:** Injects or propagates `X-Request-ID` for distributed tracing and log correlation.
- **Graceful shutdown:** `SIGTERM`/`SIGINT` handlers close the HTTP server and database connections cleanly, with a 10-second forced timeout.
- **ESLint flat config:** Modern `eslint.config.mjs` with TypeScript strict rules including `no-explicit-any`.