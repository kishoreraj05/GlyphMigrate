# Security

## Reporting a vulnerability

Please don't open a public issue for security problems. Use GitHub's
**Security → Report a vulnerability** (private vulnerability reporting) on this repository, and include
steps to reproduce. You'll get an acknowledgement, and a fix or mitigation plan once the issue is confirmed.

## Things to know before deploying

GlyphMigrate is early-stage software. Keep these in mind before pointing it at real systems:

* **Credentials:** connection passwords are stored unencrypted in the local SQLite metadata DB
  (`GLYPH_DATA_DIR`). Protect that volume, and use dedicated, least-privilege database users.
* **No authentication:** the API and UI have no login. Run them only on a trusted network, or behind
  an authenticating proxy.
* **Transforms run arbitrary SQL** on the target database as the target connection's user. Give that user
  rights only on the staging schemas and the tables it must load.
* **Source connections:** use a read-only user (or a read replica). GlyphMigrate opens read-only
  transactions, but database permissions are the real boundary.
* **Demo credentials:** `glyph` / `glyph` in `docker-compose.yml` are for the sample databases only.
