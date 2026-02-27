"""
Gestión de conexión y sesiones de SQLAlchemy para PostgreSQL.
"""

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from config import db_config
from db.models import Base, SOURCE_MODELS, SOURCE_TABLE_NAMES

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

    # Mapeo de columna source_id por tabla
    source_id_col = {
        "openalex_records": "openalex_work_id",
        "scopus_records": "scopus_doc_id",
        "wos_records": "wos_uid",
        "cvlac_records": "cvlac_product_id",
        "datos_abiertos_records": "datos_source_id",
    }
    source_name_val = {
        "openalex_records": "openalex",
        "scopus_records": "scopus",
        "wos_records": "wos",
        "cvlac_records": "cvlac",
        "datos_abiertos_records": "datos_abiertos",
    }

    selects = []
    for table_name in SOURCE_TABLE_NAMES:
        cols_sql = ", ".join(common_cols)
        sid_col = source_id_col[table_name]
        sname = source_name_val[table_name]
        selects.append(
            f"SELECT '{sname}' AS source_name, {sid_col} AS source_id, {cols_sql} "
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
        # --- Índices para autores ---
        "CREATE INDEX IF NOT EXISTS ix_authors_openalex ON authors (openalex_id) WHERE openalex_id IS NOT NULL;",
        "CREATE INDEX IF NOT EXISTS ix_authors_scopus ON authors (scopus_id) WHERE scopus_id IS NOT NULL;",

        # --- Índices para publication_authors ---
        "CREATE INDEX IF NOT EXISTS ix_pub_authors_author ON publication_authors (author_id);",
        "CREATE INDEX IF NOT EXISTS ix_pub_authors_pub ON publication_authors (publication_id);",

        # --- Índices para reconciliation_log ---
        "CREATE INDEX IF NOT EXISTS ix_recon_log_canon ON reconciliation_log (canonical_publication_id);",

        # --- Índices para canonical_publications ---
        "CREATE INDEX IF NOT EXISTS ix_canon_journal ON canonical_publications (journal_id) WHERE journal_id IS NOT NULL;",

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

    with engine.connect() as conn:
        for stmt in ddl_statements:
            try:
                conn.execute(text(stmt))
            except Exception as e:
                logger.debug(f"Migración omitida: {e}")
        conn.commit()
    logger.info("Migración idempotente completada — constraints e índices verificados.")


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
