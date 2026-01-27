# Database Migrations Guide

This project uses **Alembic** to manage database schema changes without requiring database drops/recreates.

## How It Works

- **Automatic on Startup**: The API container automatically runs pending migrations when it starts
- **Version Control**: Each migration is a timestamped Python file in `migrations/versions/`
- **Reversible**: Each migration can be rolled back if needed

## Creating New Migrations

When you modify `api/models.py`, you need to create a migration to apply the changes to the database.

### Step 1: Modify Your Models

Edit `api/models.py` to add/change/remove columns or tables:

```python
class MyNewModel(Base):
    __tablename__ = 'my_new_table'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
```

### Step 2: Auto-Generate Migration

```bash
# Inside the API container
docker exec pineapple-index-api-1 alembic revision --autogenerate -m "Add my_new_table"
```

This creates a new file in `migrations/versions/` with the required changes.

### Step 3: Review the Migration

Open the generated file (e.g., `migrations/versions/002_add_my_new_table.py`) and verify it looks correct.

### Step 4: Deploy

Simply restart the API container—the migration runs automatically:

```bash
docker-compose up -d api
```

Check logs to confirm:

```bash
docker-compose logs api | grep -i migration
```

Expected output:
```
api-1  | 2026-01-27T20:00:00Z [api] running database migrations...
api-1  | Running database migrations...
api-1  | ✓ Migrations completed successfully
```

## Manual Migrations

If auto-generate doesn't work perfectly, you can write migrations manually:

```bash
# Create a blank migration
docker exec pineapple-index-api-1 alembic revision -m "my migration description"
```

Then edit the generated file to write SQL operations in the `upgrade()` and `downgrade()` functions.

## Viewing Migration Status

```bash
# Check current database version
docker exec pineapple-index-api-1 alembic current

# View migration history
docker exec pineapple-index-api-1 alembic history
```

## Rolling Back Migrations

If something goes wrong, you can rollback to a previous version:

```bash
# Rollback one migration
docker exec pineapple-index-api-1 alembic downgrade -1

# Rollback to specific migration
docker exec pineapple-index-api-1 alembic downgrade 001_initial
```

## Workflow Summary

```
1. Modify api/models.py
2. docker exec api alembic revision --autogenerate -m "description"
3. Review migrations/versions/XXX_description.py
4. docker-compose up -d api  (runs migration automatically)
5. Verify: docker-compose logs api | grep migration
```

## Tips

- Always review generated migrations before deploying
- Keep migrations focused (one logical change per migration)
- Never manually edit the `alembic_version` table
- Migrations must be idempotent (safe to run multiple times)
