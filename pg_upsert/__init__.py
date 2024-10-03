import logging

from .__version__ import (
    __author__,
    __author_email__,
    __description__,
    __license__,
    __title__,
    __url__,
    __version__,
)
from .pg_upsert import PgUpsert

__all__ = ["PgUpsert"]


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
