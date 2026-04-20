# Backward-compatibility shim. New code: use project.interfaces.api.main
from project.interfaces.api.main import app, lifespan  # noqa: F401
