#!venv/bin/python
import logging
import sqlite3
import time
import pathlib

from upload_utils import _build_route_data, set_routes_bulk

PWD = pathlib.Path(__file__).resolve().parent
DB_PATH = PWD / "flight_routes.sqb"

logger = logging.getLogger(__name__)


def _load_recent_routes(since: int) -> list[tuple[str, str]]:
    """Return callsigns with exactly one distinct route updated since
    the given Unix timestamp."""
    with sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True) as _conn:
        _cursor = _conn.cursor()
        _cursor.execute(
            """
            SELECT Callsign, MIN(Route)
            FROM flight_routes
            WHERE UpdateTime >= ?
            GROUP BY Callsign
            HAVING COUNT(DISTINCT Route) = 1
            """,
            (since,),
        )
        return _cursor.fetchall()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)

    _three_weeks_ago = int(time.time()) - 3 * 7 * 24 * 3600
    _rows = _load_recent_routes(_three_weeks_ago)
    logger.warning(f"Loaded {len(_rows)} candidates from SQLite.")

    _routes = []
    for _callsign, _route in _rows:
        _data = _build_route_data(_callsign, _route, plausible=True)
        if _data is not None:
            _routes.append(_data)

    _new, _updated, _unchanged, _rejected = set_routes_bulk(_routes)
    logger.warning(
        f"Done: {len(_rows)} candidates — "
        f"{len(_routes)} valid, "
        f"{_new} new, {_updated} updated, "
        f"{_unchanged} unchanged, {_rejected} rejected."
    )
