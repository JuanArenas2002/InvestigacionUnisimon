# Backward-compatibility shim. New code: use project.infrastructure.persistence.models
from project.infrastructure.persistence.models import *  # noqa: F401, F403
from project.infrastructure.persistence.models import (  # noqa: F401
    Base, SourceRecordMixin,
    Journal, Institution, CanonicalPublication, PossibleDuplicatePair,
    Author, AuthorInstitution, PublicationAuthor, ReconciliationLog,
    DisciplinaryField, FieldParameter, ResearchThreshold,
    AuthorAuditLog, AuthorConflict, ResearcherCredential, User,
    SOURCE_MODELS, SOURCE_TABLE_NAMES,
    get_source_model, count_source_records_for_canonical,
    find_record_by_doi_across_sources, get_all_source_records_for_canonical,
    count_all_source_records, count_source_records_by_status,
    count_source_records_by_source, get_thresholds_by_field,
)
