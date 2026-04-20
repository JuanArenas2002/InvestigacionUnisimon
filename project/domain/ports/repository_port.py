from project.domain.ports.author_repository import AuthorRepositoryPort
from project.domain.ports.publication_repository import PublicationRepositoryPort


class RepositoryPort(PublicationRepositoryPort, AuthorRepositoryPort):
    """
    Puerto combinado de persistencia.

    Hereda PublicationRepositoryPort + AuthorRepositoryPort.
    Mantiene backward-compatibility: el codigo existente que depende de
    RepositoryPort sigue funcionando sin cambios.

    Nuevo codigo debe preferir el port especifico mas estrecho:
      - IngestPipeline       → PublicationRepositoryPort
      - AuthorProfileUseCase → AuthorRepositoryPort
    """
