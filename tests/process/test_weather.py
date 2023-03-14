"""Weather tests"""

from unittest.mock import patch

import pytest
from requests.exceptions import RequestException

from bemserver_core.process.weather import OikolabWeatherDataClient

from bemserver_core.exceptions import (
    BEMServerCoreWeatherAPIConnectionError,
    BEMServerCoreWeatherAPIQueryError,
)


@pytest.fixture
def client():
    return OikolabWeatherDataClient("api_key")


class TestWeather:
    @patch("bemserver_core.process.weather.requests.get")
    def test_get_weather_data(self, mock_get, client):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "data": {
                "AIR_TEMPERATURE": [
                    {"value": 20.0, "time": "2019-01-01T00:00:00Z"},
                    {"value": 21.0, "time": "2019-01-01T01:00:00Z"},
                ]
            }
        }
        response = client.get_weather_data(
            params=["AIR_TEMPERATURE"],
            latitude=46.0,
            longitude=6.0,
            dt_start="2019-01-01T00:00:00Z",
            dt_end="2019-01-01T01:00:00Z",
        )
        assert response == {
            "data": {
                "AIR_TEMPERATURE": [
                    {"value": 20.0, "time": "2019-01-01T00:00:00Z"},
                    {"value": 21.0, "time": "2019-01-01T01:00:00Z"},
                ]
            }
        }

    @patch("bemserver_core.process.weather.requests.get")
    def test_get_weather_data_query_error(self, mock_get, client):
        mock_get.return_value.status_code = 400
        mock_get.return_value.text = "Query error"
        with pytest.raises(
            BEMServerCoreWeatherAPIQueryError,
            match="Error while querying the weather API: Query error",
        ):
            client.get_weather_data(
                params=["AIR_TEMPERATURE"],
                latitude="dummy",
                longitude=6.0,
                dt_start="2019-01-01T00:00:00Z",
                dt_end="2019-01-01T01:00:00Z",
            )

    @patch("bemserver_core.process.weather.requests.get")
    def test_get_weather_data_connexion_error(self, mock_get, client):
        mock_get.side_effect = RequestException("Connection error")
        with pytest.raises(
            BEMServerCoreWeatherAPIConnectionError,
            match="Error while connecting to the weather API: Connection error",
        ):
            client.get_weather_data(
                params=["AIR_TEMPERATURE"],
                latitude=46.0,
                longitude=6.0,
                dt_start="2019-01-01T00:00:00Z",
                dt_end="2019-01-01T01:00:00Z",
            )
