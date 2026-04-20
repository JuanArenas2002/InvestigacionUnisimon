# Backward-compatibility shim. New code: use project.infrastructure.persistence.session
from project.infrastructure.persistence.session import *  # noqa: F401, F403
from project.infrastructure.persistence.session import (  # noqa: F401
    get_engine, get_session_factory, get_session,
    create_all_tables, create_all_source_records_view,
    ensure_constraints, drop_all_tables,
    check_connection, get_table_counts,
)
