"""
Configuración de la aplicación.
Reutiliza settings de config.py existente.
"""
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

# Importar config existente
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import AppConfig, DatabaseConfig, ReconciliationConfig

# Re-exportar para usar en new_arch
__all__ = ["get_settings", "AppConfig", "DatabaseConfig", "ReconciliationConfig"]


@dataclass
class Settings:
    """Configuración unificada"""
    app: AppConfig
    database: DatabaseConfig
    reconciliation: ReconciliationConfig
    
    @classmethod
    def from_existing_config(cls):
        """Crea Settings desde config.py existente"""
        return cls(
            app=AppConfig(),
            database=DatabaseConfig(),
            reconciliation=ReconciliationConfig(),
        )


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Obtiene la configuración singleton"""
    global _settings
    if _settings is None:
        _settings = Settings.from_existing_config()
    return _settings
