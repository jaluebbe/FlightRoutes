# encoding=utf8
import json
import time
import arrow
import redis
import requests
import logging

from opensky_api import OpenSkyApi
from opensky_utils import validated_position, update_icao24s_from_redis

from config import *


logging.basicConfig(level=logging.INFO)


class OpenSkyApiConsumer:

    last_update = None
    logger = logging.getLogger(__name__)
    redis_connection = redis.Redis(host=REDIS_HOST, decode_responses=True)

    def __init__(self):
        self._initialize_connection()

    def _initialize_connection(self):
        self.api = OpenSkyApi(OPENSKY_USER, OPENSKY_PASSWORD)

    def worker(self, interval=45):
        while True:
            update_icao24s_from_redis()
            positions = {}
            t_start = arrow.utcnow()
            self.logger.info(
                "UTC {}".format(t_start.format("YYYY-MM-DD HH:mm:ss"))
            )
            try:
                _response = self.api.get_states()
                if _response is None:
                    self.logger.warning("API response is None.")
                    time.sleep(
                        max((arrow.utcnow() - t_start).total_seconds(), 5)
                    )
                    continue
                else:
                    _all_states = _response.states
                    states_time = _response.time
            except requests.exceptions.ReadTimeout:
                self.logger.error("connection timeout.")
                continue
            except IOError as e:
                self.logger.exception(
                    "problem with OpenSky Network connection."
                )
                self.logger.info("Sleep for 60 seconds.")
                time.sleep(60)
                self.logger.info("...reconnecting to OpenSky Network.")
                self._initialize_connection()
                continue
            t_data_received = arrow.utcnow()

            for _state in _all_states:
                position = validated_position(
                    _state,
                    accepted_operators=None,
                    allow_numerical_callsign=True,
                    allow_alphanumerical_callsign=True,
                    allow_on_ground=False,
                )
                if position is None:
                    continue
                age = states_time - position["time_position"]
                if age > 60:
                    continue
                callsign = position["callsign"]
                positions[callsign] = position
            try:
                self.redis_connection.set(
                    "opensky_positions",
                    json.dumps(
                        {"positions": positions, "states_time": states_time}
                    ),
                )
            except (
                redis.exceptions.TimeoutError,
                redis.exceptions.ConnectionError,
            ):
                self.logger.exception("Cannot store positions in redis db.")
            t_data_processed = arrow.utcnow()
            duration_receiving = (t_data_received - t_start).total_seconds()
            duration_processing = (
                t_data_processed - t_data_received
            ).total_seconds()
            duration_total = (t_data_processed - t_start).total_seconds()
            self.logger.info(
                "{} states received ({:.1f}s) and processed ({:.1f}s)."
                "".format(
                    len(_all_states), duration_receiving, duration_processing
                )
            )
            time.sleep(max(0, interval - duration_total))


if __name__ == "__main__":

    osac = OpenSkyApiConsumer()
    osac.worker()
