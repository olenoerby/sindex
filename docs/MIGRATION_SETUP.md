# Database Migration System - Setup Complete ✓

## What Was Implemented

A **zero-downtime database migration system** using Alembic so you never have to delete the database again when adding new features.

## How It Works

1. **Automatic on Startup**: When the API container starts, it automatically runs any pending migrations
2. **Schema Versioning**: Each database schema change is recorded in `migrations/versions/`
3. **Reversible**: Migrations can be rolled back if needed

## Quick Start Guide

### To Add a New Feature

```bash
# 1. Modify your models
# Edit api/models.py and add/change your schema

# 2. Generate migration
docker exec pineapple-index-api-1 alembic revision --autogenerate -m "add user_preferences table"

# 3. Review the migration file created in migrations/versions/

# 4. Deploy (migration runs automatically)
docker-compose up -d api
```

### Check Migration Status

```bash
# View current database version
docker exec pineapple-index-api-1 alembic current

# View all migrations
docker exec pineapple-index-api-1 alembic history

# View API logs for migration output
docker-compose logs api | grep -i migration
```

## Files Created/Modified

### New Files
- `migrations/` - Alembic directory with all migrations
- `migrations/versions/001_initial.py` - Initial schema migration (the current schema)
- `alembic.ini` - Alembic configuration
- `scripts/run_migrations.py` - Migration runner script
- `docs/MIGRATIONS.md` - Detailed migration documentation

### Modified Files
- `scripts/container-entrypoint.sh` - Now runs migrations before starting API
- `README.md` - Added Database Migrations section
- `api/Dockerfile` - Copied in new scripts

## Key Features

✅ **No database deletes needed** - Schema evolves without data loss
✅ **Automatic on startup** - Migrations run when containers start
✅ **Version control** - All changes tracked in `migrations/versions/`
✅ **Reversible** - Can rollback migrations if needed
✅ **Auto-detect** - Alembic auto-generates migrations from model changes
✅ **Current database** - Existing schema already captured in `001_initial.py`

## Example Workflow

```python
# 1. Add a new model
class UserPreferences(Base):
    __tablename__ = 'user_preferences'
    id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False)
    theme = Column(String, default='light')

# 2. Generate migration
$ docker exec api alembic revision --autogenerate -m "add user_preferences table"
Generating /app/migrations/versions/002_add_user_preferences_table.py ...  done

# 3. Review migrations/versions/002_add_user_preferences_table.py

# 4. Deploy
$ docker-compose up -d api
✓ Migrations completed successfully

# 5. Database updated automatically - no deletes needed!
```

## Next Steps

- See [docs/MIGRATIONS.md](docs/MIGRATIONS.md) for detailed commands
- Always review generated migrations before deploying
- For complex changes, you can write migrations manually

## Troubleshooting

**Migration fails on startup?**
- Check API logs: `docker-compose logs api`
- Verify models are syntactically correct
- Review the generated migration file

**Need to rollback?**
- `docker exec api alembic downgrade -1` (rollback one migration)
- `docker exec api alembic downgrade 001_initial` (rollback to specific version)

**Starting fresh with migrations?**
- Keep `migrations/versions/` in version control
- The `alembic_version` table in the database tracks applied migrations
- Never manually edit `alembic_version`

