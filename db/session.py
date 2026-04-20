"""
Gestión de conexión y sesiones de SQLAlchemy para PostgreSQL.
"""

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from config import db_config
from db.models import Base, SOURCE_MODELS, SOURCE_TABLE_NAMES
from db.source_registry import SOURCE_REGISTRY

logger = logging.getLogger(__name__)


# =============================================================
# ENGINE Y SESSION FACTORY
# =============================================================

_engine = None
_SessionFactory = None


def get_engine(echo: bool = None):
    global _engine
    if _engine is None:
        echo_sql = echo if echo is not None else db_config.echo_sql
        _engine = create_engine(
            db_config.url,
            echo=echo_sql,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
        )
        logger.info(f"Engine creado para: {db_config.host}:{db_config.port}/{db_config.database}")
    return _engine


def get_session_factory() -> sessionmaker:
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=get_engine(), expire_on_commit=False)
    return _SessionFactory


def get_session() -> Session:
    factory = get_session_factory()
    return factory()


# =============================================================
# INICIALIZACIÓN DE BASE DE DATOS
# =============================================================

def create_all_tables():
    """Crea todas las tablas definidas en los modelos."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    ensure_constraints(engine)
    create_all_source_records_view(engine)
    logger.info("Todas las tablas y vista creadas exitosamente.")


def create_all_source_records_view(engine=None):
    """
    Crea (o reemplaza) la vista SQL all_source_records que unifica
    las 5 tablas de fuente con columnas comunes. Útil para consultas
    cruzadas (stats, listados, etc.).
    """
    if engine is None:
        engine = get_engine()

    # Columnas comunes que se incluyen en la vista
    common_cols = [
        "id", "dedup_hash", "doi", "title", "normalized_title",
        "publication_year", "publication_date", "publication_type",
        "source_journal", "issn", "language", "is_open_access", "oa_status",
        "citation_count", "authors_text", "normalized_authors", "url",
        "raw_data", "status", "canonical_publication_id",
        "match_type", "match_score", "reconciled_at",
        "created_at", "updated_at",
    ]

    # Derivado del registry — se actualiza automáticamente al registrar nuevas fuentes
    selects = []
    for src_def in SOURCE_REGISTRY.all():
        table_name = src_def.model_class.__tablename__
        cols_sql = ", ".join(common_cols)
        selects.append(
            f"SELECT '{src_def.name}' AS source_name, {src_def.id_attr} AS source_id, {cols_sql} "
            f"FROM {table_name}"
        )

    view_sql = "CREATE OR REPLACE VIEW all_source_records AS\n" + "\nUNION ALL\n".join(selects)

    with engine.connect() as conn:
        try:
            conn.execute(text(view_sql))
            conn.commit()
            logger.info("Vista all_source_records creada/actualizada.")
        except Exception as e:
            logger.warning(f"No se pudo crear vista all_source_records: {e}")


def ensure_constraints(engine=None):
    """
    Migración idempotente: crea índices y constraints faltantes.
    """
    if engine is None:
        engine = get_engine()

    ddl_statements = [
        # --- v11: extensión pg_trgm (fuzzy matching) ---
        "CREATE EXTENSION IF NOT EXISTS pg_trgm;",

        # --- v12: cedula en authors ---
        "ALTER TABLE authors ADD COLUMN IF NOT EXISTS cedula VARCHAR(30);",
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE tablename='authors' AND indexname='ix_authors_cedula'
            ) THEN
                CREATE UNIQUE INDEX ix_authors_cedula ON authors (cedula)
                WHERE cedula IS NOT NULL;
            END IF;
        END $$;
        """,

        # --- v11: columnas nuevas en authors ---
        """
        ALTER TABLE authors
            ADD COLUMN IF NOT EXISTS verification_status VARCHAR(30)
                NOT NULL DEFAULT 'auto_detected';
        """,
        """
        ALTER TABLE authors
            ADD COLUMN IF NOT EXISTS possible_duplicate_of INTEGER
                REFERENCES authors(id) ON DELETE SET NULL;
        """,

        # --- v11: índices en authors ---
        "CREATE INDEX IF NOT EXISTS ix_authors_external_ids_gin ON authors USING GIN (external_ids);",
        "CREATE INDEX IF NOT EXISTS ix_authors_normalized_name_trgm ON authors USING GIN (normalized_name gin_trgm_ops);",
        "CREATE INDEX IF NOT EXISTS ix_authors_verification_status ON authors (verification_status);",
        "CREATE INDEX IF NOT EXISTS ix_authors_possible_dup ON authors (possible_duplicate_of) WHERE possible_duplicate_of IS NOT NULL;",

        # --- v11: tabla author_audit_log ---
        """
        CREATE TABLE IF NOT EXISTS author_audit_log (
            id          SERIAL PRIMARY KEY,
            author_id   INTEGER REFERENCES authors(id) ON DELETE CASCADE,
            change_type VARCHAR(30) NOT NULL,
            before_data JSONB,
            after_data  JSONB,
            field_changes JSONB,
            source      VARCHAR(100),
            changed_by  VARCHAR(200),
            created_at  TIMESTAMPTZ DEFAULT now()
        );
        """,
        "CREATE INDEX IF NOT EXISTS ix_author_audit_author_id ON author_audit_log (author_id);",
        "CREATE INDEX IF NOT EXISTS ix_author_audit_created ON author_audit_log (created_at DESC);",

        # --- v11: tabla author_conflicts ---
        """
        CREATE TABLE IF NOT EXISTS author_conflicts (
            id              SERIAL PRIMARY KEY,
            author_id       INTEGER REFERENCES authors(id) ON DELETE CASCADE,
            field_name      VARCHAR(100) NOT NULL,
            existing_value  TEXT,
            new_value       TEXT,
            existing_source VARCHAR(100),
            new_source      VARCHAR(100),
            resolved        BOOLEAN DEFAULT FALSE,
            resolution      VARCHAR(50),
            resolved_at     TIMESTAMPTZ,
            resolved_by     VARCHAR(200),
            created_at      TIMESTAMPTZ DEFAULT now()
        );
        """,
        "CREATE INDEX IF NOT EXISTS ix_author_conflicts_author ON author_conflicts (author_id);",
        "CREATE INDEX IF NOT EXISTS ix_author_conflicts_unresolved ON author_conflicts (resolved, created_at DESC);",

        # --- v11: columnas nuevas en author_institutions ---
        "ALTER TABLE author_institutions ADD COLUMN IF NOT EXISTS start_year INTEGER;",
        "ALTER TABLE author_institutions ADD COLUMN IF NOT EXISTS end_year INTEGER;",
        "ALTER TABLE author_institutions ADD COLUMN IF NOT EXISTS is_current BOOLEAN DEFAULT TRUE;",

        # --- Índices para autores (legacy — pueden fallar silenciosamente) ---
        "CREATE INDEX IF NOT EXISTS ix_authors_openalex ON authors (openalex_id) WHERE openalex_id IS NOT NULL;",
        "CREATE INDEX IF NOT EXISTS ix_authors_scopus ON authors (scopus_id) WHERE scopus_id IS NOT NULL;",

        # --- Índices para publication_authors ---
        "CREATE INDEX IF NOT EXISTS ix_pub_authors_author ON publication_authors (author_id);",
        "CREATE INDEX IF NOT EXISTS ix_pub_authors_pub ON publication_authors (publication_id);",

        # --- Índices para reconciliation_log ---
        "CREATE INDEX IF NOT EXISTS ix_recon_log_canon ON reconciliation_log (canonical_publication_id);",

        # --- Índices para canonical_publications ---
        "CREATE INDEX IF NOT EXISTS ix_canon_journal ON canonical_publications (journal_id) WHERE journal_id IS NOT NULL;",

        # --- v16: abstract, page_range, publisher en tablas de fuente ---
        "ALTER TABLE openalex_records       ADD COLUMN IF NOT EXISTS abstract   TEXT;",
        "ALTER TABLE openalex_records       ADD COLUMN IF NOT EXISTS page_range VARCHAR(100);",
        "ALTER TABLE openalex_records       ADD COLUMN IF NOT EXISTS publisher  VARCHAR(500);",
        "ALTER TABLE scopus_records         ADD COLUMN IF NOT EXISTS abstract   TEXT;",
        "ALTER TABLE scopus_records         ADD COLUMN IF NOT EXISTS page_range VARCHAR(100);",
        "ALTER TABLE scopus_records         ADD COLUMN IF NOT EXISTS publisher  VARCHAR(500);",
        "ALTER TABLE wos_records            ADD COLUMN IF NOT EXISTS abstract   TEXT;",
        "ALTER TABLE wos_records            ADD COLUMN IF NOT EXISTS page_range VARCHAR(100);",
        "ALTER TABLE wos_records            ADD COLUMN IF NOT EXISTS publisher  VARCHAR(500);",
        "ALTER TABLE cvlac_records          ADD COLUMN IF NOT EXISTS abstract   TEXT;",
        "ALTER TABLE cvlac_records          ADD COLUMN IF NOT EXISTS page_range VARCHAR(100);",
        "ALTER TABLE cvlac_records          ADD COLUMN IF NOT EXISTS publisher  VARCHAR(500);",
        "ALTER TABLE datos_abiertos_records ADD COLUMN IF NOT EXISTS abstract   TEXT;",
        "ALTER TABLE datos_abiertos_records ADD COLUMN IF NOT EXISTS page_range VARCHAR(100);",
        "ALTER TABLE datos_abiertos_records ADD COLUMN IF NOT EXISTS publisher  VARCHAR(500);",
        "ALTER TABLE google_scholar_records ADD COLUMN IF NOT EXISTS abstract   TEXT;",
        "ALTER TABLE google_scholar_records ADD COLUMN IF NOT EXISTS page_range VARCHAR(100);",
        "ALTER TABLE google_scholar_records ADD COLUMN IF NOT EXISTS publisher  VARCHAR(500);",

        # --- v17: tabla de pares posiblemente duplicados ---
        """
        CREATE TABLE IF NOT EXISTS possible_duplicate_pairs (
            id              SERIAL PRIMARY KEY,
            canonical_id_1  INTEGER NOT NULL REFERENCES canonical_publications(id) ON DELETE CASCADE,
            canonical_id_2  INTEGER NOT NULL REFERENCES canonical_publications(id) ON DELETE CASCADE,
            similarity_score FLOAT  NOT NULL,
            match_method    VARCHAR(50) NOT NULL DEFAULT 'title',
            detected_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            status          VARCHAR(20) NOT NULL DEFAULT 'pending',
            CONSTRAINT uq_dup_pair UNIQUE (canonical_id_1, canonical_id_2),
            CONSTRAINT chk_dup_order CHECK (canonical_id_1 < canonical_id_2)
        );
        """,
        "CREATE INDEX IF NOT EXISTS ix_dup_pairs_id1 ON possible_duplicate_pairs (canonical_id_1);",
        "CREATE INDEX IF NOT EXISTS ix_dup_pairs_id2 ON possible_duplicate_pairs (canonical_id_2);",
        "CREATE INDEX IF NOT EXISTS ix_dup_pairs_status ON possible_duplicate_pairs (status) WHERE status = 'pending';",

        # --- Índices GIN full-text ---
        "CREATE INDEX IF NOT EXISTS ix_canon_title_fts ON canonical_publications USING GIN (to_tsvector('spanish', coalesce(title, '')));",
        "CREATE INDEX IF NOT EXISTS ix_authors_name_fts ON authors USING GIN (to_tsvector('spanish', coalesce(name, '')));",

        # --- Constraints de unicidad ---
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_pub_author') THEN
                ALTER TABLE publication_authors ADD CONSTRAINT uq_pub_author UNIQUE (publication_id, author_id);
            END IF;
        END $$;
        """,
        """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_author_institution') THEN
                ALTER TABLE author_institutions ADD CONSTRAINT uq_author_institution UNIQUE (author_id, institution_id);
            END IF;
        END $$;
        """,
    ]

    # Índices GIN full-text para cada tabla de fuente
    for table_name in SOURCE_TABLE_NAMES:
        ddl_statements.append(
            f"CREATE INDEX IF NOT EXISTS ix_{table_name}_title_fts ON {table_name} "
            f"USING GIN (to_tsvector('spanish', coalesce(title, '')));"
        )

    # Cada sentencia DDL se ejecuta en su propia conexión con autocommit=True.
    # Esto evita que un fallo en una sentencia aborte el resto de la transacción
    # (comportamiento por defecto de PostgreSQL dentro de un bloque de transacción).
    applied = 0
    skipped = 0
    for stmt in ddl_statements:
        try:
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text(stmt.strip()))
            applied += 1
        except Exception as e:
            skipped += 1
            logger.debug("DDL omitido (%s): %.120s", type(e).__name__, stmt.strip()[:120])

    logger.info(
        "Migración idempotente completada — %d sentencias aplicadas, %d omitidas.",
        applied, skipped,
    )


def drop_all_tables():
    """PELIGRO: Elimina todas las tablas. Solo para desarrollo/testing."""
    engine = get_engine()
    # Primero eliminar la vista
    with engine.connect() as conn:
        conn.execute(text("DROP VIEW IF EXISTS all_source_records CASCADE"))
        conn.commit()
    Base.metadata.drop_all(engine)
    logger.warning("Todas las tablas eliminadas.")


def check_connection() -> bool:
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
    """Retorna el conteo de registros de cada tabla principal."""
    session = get_session()
    try:
        from db.models import (
            CanonicalPublication, Author, ReconciliationLog,
            count_all_source_records,
        )
        source_total = count_all_source_records(session)
        return {
            "canonical_publications": session.query(CanonicalPublication).count(),
            "authors": session.query(Author).count(),
            "source_records_total": source_total,
            "reconciliation_log": session.query(ReconciliationLog).count(),
        }
    finally:
        session.close()
