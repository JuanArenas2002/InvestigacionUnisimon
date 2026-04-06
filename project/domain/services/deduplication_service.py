from typing import Dict, List

from project.domain.models.publication import Publication


class DeduplicationService:
    """Elimina duplicados internos antes del matching y enriquecimiento."""

    def deduplicate(self, publications: List[Publication]) -> List[Publication]:
        by_key: Dict[str, Publication] = {}
        for publication in publications:
            key = publication.identity_key()
            existing = by_key.get(key)
            if existing is None:
                by_key[key] = publication
                continue

            # Resolver conflictos conservando el registro mas informativo.
            by_key[key] = self._select_best(existing, publication)
        return list(by_key.values())

    @staticmethod
    def _select_best(left: Publication, right: Publication) -> Publication:
        left_score = int(bool(left.doi)) + int(bool(left.source_id)) + len(left.authors)
        right_score = int(bool(right.doi)) + int(bool(right.source_id)) + len(right.authors)
        return left if left_score >= right_score else right
