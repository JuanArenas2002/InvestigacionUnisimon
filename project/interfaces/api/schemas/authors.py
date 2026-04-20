# Canonical location for author HTTP schemas.
# api/schemas/authors.py stays as the implementation until full migration.
from api.schemas.authors import *  # noqa: F401, F403
from api.schemas.authors import (  # noqa: F401
    AuthorBase, AuthorRead, AuthorDetail, AuthorPublicationRead,
    NameOptionsResponse, UpdateNameRequest,
    SourceLinksResponse, UpdateSourceLinkRequest, UpdateOrcidRequest,
)
