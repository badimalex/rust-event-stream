# Security

## Authentication
`/v1/events` and `/v1/events/{id}` require `X-API-Key`.

## Authorization
Per-tenant authorization is not implemented.

## Input validation
Event fields, payload size, and HTTP body size are validated.

## Resource limits
Timeout, concurrency limit, load shedding, and bounded queue are enabled.

## Secrets
`DATABASE_URL` and `API_KEY` are loaded from environment variables.

## Error exposure
Internal errors are logged and generic errors are returned to clients.

## Known limitations
Authenticated callers may access arbitrary `tenant_id` values.