"""
Gestión de conexión y sesiones de SQLAlchemy para PostgreSQL.
"""

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from config import db_config
from db.models import Base

logger = logging.getLogger(__name__)


# =============================================================
# ENGINE Y SESSION FACTORY
# =============================================================

_engine = None
_SessionFactory = None


def get_engine(echo: bool = None):
    """
    Crea o reutiliza el engine de SQLAlchemy.
    """
    global _engine
    if _engine is None:
        echo_sql = echo if echo is not None else db_config.echo_sql
        _engine = create_engine(
            db_config.url,
            echo=echo_sql,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,  # Verifica conexión antes de usarla
        )
        logger.info(f"Engine creado para: {db_config.host}:{db_config.port}/{db_config.database}")
    return _engine


def get_session_factory() -> sessionmaker:
    """
    Retorna la fábrica de sesiones (singleton).
    """
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=get_engine(), expire_on_commit=False)
    return _SessionFactory


def get_session() -> Session:
    """
    Crea una nueva sesión de base de datos.

    Uso:
        session = get_session()
        try:
            # ... operaciones ...
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
    """
    factory = get_session_factory()
    return factory()


# =============================================================
# INICIALIZACIÓN DE BASE DE DATOS
# =============================================================

def create_all_tables():
    """
    Crea todas las tablas definidas en los modelos.
    Útil para inicialización o desarrollo.
    """
    engine = get_engine()
    Base.metadata.create_all(engine)
    ensure_constraints(engine)
    logger.info("Todas las tablas creadas exitosamente.")


def ensure_constraints(engine=None):
    """
    Migración idempotente: crea columnas, tablas, índices y constraints
    faltantes sobre una BD que puede tener un esquema anterior.
    Usa IF NOT EXISTS / DO $$ para ser seguro de re-ejecutar.
    """
    if engine is None:
        engine = get_engine()

    ddl_statements = [
        # ─────────────────────────────────────────────────
        # 1. Columnas faltantes
        # ─────────────────────────────────────────────────

        # dedup_hash en external_records
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'external_records' AND column_name = 'dedup_hash'
            ) THEN
                ALTER TABLE external_records ADD COLUMN dedup_hash VARCHAR(64);
            END IF;
        END $$;
        """,

        # updated_at en authors
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'authors' AND column_name = 'updated_at'
            ) THEN
                ALTER TABLE authors ADD COLUMN updated_at TIMESTAMPTZ DEFAULT now();
            END IF;
        END $$;
        """,

        # updated_at en external_records
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'external_records' AND column_name = 'updated_at'
            ) THEN
                ALTER TABLE external_records ADD COLUMN updated_at TIMESTAMPTZ DEFAULT now();
            END IF;
        END $$;
        """,

        # journal_id en canonical_publications
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'canonical_publications' AND column_name = 'journal_id'
            ) THEN
                ALTER TABLE canonical_publications ADD COLUMN journal_id INTEGER
                    REFERENCES journals(id) ON DELETE SET NULL;
            END IF;
        END $$;
        """,

        # ─────────────────────────────────────────────────
        # 2. JSON → JSONB (sin pérdida de datos)
        # ─────────────────────────────────────────────────

        # raw_data en external_records
        """
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'external_records'
                  AND column_name = 'raw_data'
                  AND data_type = 'json'
            ) THEN
                ALTER TABLE external_records
                    ALTER COLUMN raw_data TYPE JSONB USING raw_data::JSONB;
            END IF;
        END $$;
        """,

        # match_details en reconciliation_log
        """
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'reconciliation_log'
                  AND column_name = 'match_details'
                  AND data_type = 'json'
            ) THEN
                ALTER TABLE reconciliation_log
                    ALTER COLUMN match_details TYPE JSONB USING match_details::JSONB;
            END IF;
        END $$;
        """,

        # ─────────────────────────────────────────────────
        # 3. Índices
        # ─────────────────────────────────────────────────

        "CREATE UNIQUE INDEX IF NOT EXISTS ix_ext_dedup_hash ON external_records (dedup_hash);",

        "CREATE UNIQUE INDEX IF NOT EXISTS ix_canon_doi_unique ON canonical_publications (doi) WHERE doi IS NOT NULL;",

        "CREATE INDEX IF NOT EXISTS ix_authors_openalex ON authors (openalex_id) WHERE openalex_id IS NOT NULL;",
        "CREATE INDEX IF NOT EXISTS ix_authors_scopus ON authors (scopus_id) WHERE scopus_id IS NOT NULL;",

        "CREATE INDEX IF NOT EXISTS ix_pub_authors_author ON publication_authors (author_id);",
        "CREATE INDEX IF NOT EXISTS ix_pub_authors_pub ON publication_authors (publication_id);",

        "CREATE INDEX IF NOT EXISTS ix_recon_log_ext ON reconciliation_log (external_record_id);",
        "CREATE INDEX IF NOT EXISTS ix_recon_log_canon ON reconciliation_log (canonical_publication_id);",

        "CREATE INDEX IF NOT EXISTS ix_canon_journal ON canonical_publications (journal_id) WHERE journal_id IS NOT NULL;",

        # ─────────────────────────────────────────────────
        # 4. Índices GIN full-text (búsquedas rápidas)
        # ─────────────────────────────────────────────────

        "CREATE INDEX IF NOT EXISTS ix_canon_title_fts ON canonical_publications USING GIN (to_tsvector('spanish', coalesce(title, '')));",
        "CREATE INDEX IF NOT EXISTS ix_ext_title_fts ON external_records USING GIN (to_tsvector('spanish', coalesce(title, '')));",
        "CREATE INDEX IF NOT EXISTS ix_authors_name_fts ON authors USING GIN (to_tsvector('spanish', coalesce(name, '')));",

        # ─────────────────────────────────────────────────
        # 5. Constraints de unicidad
        # ─────────────────────────────────────────────────

        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'uq_source_record'
            ) THEN
                ALTER TABLE external_records
                ADD CONSTRAINT uq_source_record UNIQUE (source_name, source_id);
            END IF;
        END $$;
        """,

        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'uq_pub_author'
            ) THEN
                ALTER TABLE publication_authors
                ADD CONSTRAINT uq_pub_author UNIQUE (publication_id, author_id);
            END IF;
        END $$;
        """,

        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'uq_author_institution'
            ) THEN
                ALTER TABLE author_institutions
                ADD CONSTRAINT uq_author_institution UNIQUE (author_id, institution_id);
            END IF;
        END $$;
        """,

        # ─────────────────────────────────────────────────
        # 6. Check constraints de estado
        # ─────────────────────────────────────────────────

        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'ck_ext_status'
            ) THEN
                ALTER TABLE external_records
                ADD CONSTRAINT ck_ext_status
                CHECK (status IN ('pending','matched','new_canonical','manual_review','rejected'));
            END IF;
        END $$;
        """,

        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'ck_log_action'
            ) THEN
                ALTER TABLE reconciliation_log
                ADD CONSTRAINT ck_log_action
                CHECK (action IN ('linked_existing','created_new','flagged_review','rejected','manual_resolved'));
            END IF;
        END $$;
        """,
    ]

    with engine.connect() as conn:
        for stmt in ddl_statements:
            try:
                conn.execute(text(stmt))
            except Exception as e:
                logger.debug(f"Migración omitida (ya aplicada o no aplicable): {e}")
        conn.commit()
    logger.info("Migración idempotente completada — constraints, índices y columnas verificados.")


def drop_all_tables():
    """
    PELIGRO: Elimina todas las tablas.
    Solo para desarrollo/testing.
    """
    engine = get_engine()
    Base.metadata.drop_all(engine)
    logger.warning("Todas las tablas eliminadas.")


def check_connection() -> bool:
    """
    Verifica que la conexión a PostgreSQL funciona.
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Conexión a PostgreSQL verificada correctamente.")
        return True
    except Exception as e:
        logger.error(f"Error de conexión a PostgreSQL: {e}")
        return False


def get_table_counts() -> dict:
    """
    Retorna el conteo de registros de cada tabla principal.
    Útil para diagnóstico rápido.
    """
    session = get_session()
    try:
        from db.models import (
            CanonicalPublication,
            Author,
            ExternalRecord,
            ReconciliationLog,
        )
        return {
            "canonical_publications": session.query(CanonicalPublication).count(),
            "authors": session.query(Author).count(),
            "external_records": session.query(ExternalRecord).count(),
            "reconciliation_log": session.query(ReconciliationLog).count(),
        }
    finally:
        session.close()
