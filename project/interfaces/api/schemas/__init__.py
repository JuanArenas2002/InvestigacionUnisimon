# Shim — delegates to api.schemas for backward compat.
# New code: import from project.interfaces.api.schemas.*
from api.schemas import *  # noqa: F401, F403
