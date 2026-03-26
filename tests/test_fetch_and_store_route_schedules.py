#!/usr/bin/env python3
"""
Test suite for fetch_and_store_route_schedules.py
Tests the retry functionality and API interaction functions.
"""

from unittest.mock import MagicMock, patch

import pytest

from fetch_and_store_route_schedules import (
    COMPIEGNE_STATION_ID,
    PARIS_GARE_DU_NORD_STATION_ID,
    extract_relevant_schedules,
    fetch_lines_between_stations,
    fetch_route_schedules_for_line,
)


class TestFetchLinesBetweenStations:
    """Test the fetch_lines_between_stations function with retry logic."""

    def test_successful_api_calls(self):
        """Test successful API calls return expected data."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "lines": [{"id": "line:C14", "name": "C14"}, {"id": "line:C15", "name": "C15"}]
        }

        with patch("requests.get", return_value=mock_response):
            result = fetch_lines_between_stations(COMPIEGNE_STATION_ID, PARIS_GARE_DU_NORD_STATION_ID)

            # Should return lines that are common to both stations
            assert isinstance(result, list)
            # Mock returns same lines for both stations, so all should be returned
            assert len(result) == 2

    def test_retry_on_failure_then_success(self):
        """Test that the function retries on failure and eventually succeeds."""
        call_count = 0

        def mock_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1

            mock_response = MagicMock()
            # Fail first two attempts, succeed on third
            if call_count <= 2:
                mock_response.status_code = 500
                mock_response.text = "Internal Server Error"
            else:
                mock_response.status_code = 200
                mock_response.json.return_value = {"lines": [{"id": "line:C14", "name": "C14"}]}

            return mock_response

        with patch("requests.get", side_effect=mock_get):
            result = fetch_lines_between_stations(COMPIEGNE_STATION_ID, PARIS_GARE_DU_NORD_STATION_ID)

            # First API call: 3 attempts (2 failures + 1 success)
            # Second API call: 1 attempt (success)
            # Total: 4 calls
            assert call_count == 4
            assert len(result) == 1

    def test_retry_exhausted_raises_exception(self):
        """Test that the function raises exception when all retries are exhausted."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Service Unavailable"

        with patch("requests.get", return_value=mock_response):
            with pytest.raises(Exception) as exc_info:
                fetch_lines_between_stations(COMPIEGNE_STATION_ID, PARIS_GARE_DU_NORD_STATION_ID)

            # Should raise RetryError when all retries are exhausted
            assert "RetryError" in str(exc_info.value)


class TestFetchRouteSchedulesForLine:
    """Test the fetch_route_schedules_for_line function with retry logic."""

    def test_successful_api_call(self):
        """Test successful API call returns expected data."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "route_schedules": [{"display_informations": {"label": "C14"}, "table": {"headers": [], "rows": []}}]
        }

        with patch("requests.get", return_value=mock_response):
            result = fetch_route_schedules_for_line("line:C14", "20240101")

            assert "route_schedules" in result
            assert len(result["route_schedules"]) == 1

    def test_retry_on_failure_then_success(self):
        """Test that the function retries on failure and eventually succeeds."""
        call_count = 0

        def mock_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1

            mock_response = MagicMock()
            # Fail first two attempts, succeed on third
            if call_count <= 2:
                mock_response.status_code = 500
                mock_response.text = "kraken routeschedule error while solving request: zmq backend timed out"
            else:
                mock_response.status_code = 200
                mock_response.json.return_value = {"route_schedules": []}

            return mock_response

        with patch("requests.get", side_effect=mock_get):
            result = fetch_route_schedules_for_line("line:C14", "20240101")

            # Should have made 3 calls total (3 retries)
            assert call_count == 3
            assert "route_schedules" in result

    def test_retry_exhausted_raises_exception(self):
        """Test that the function raises exception when all retries are exhausted."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "kraken interface callZmq error while solving request: zmq backend timed out"

        with patch("requests.get", return_value=mock_response):
            with pytest.raises(Exception) as exc_info:
                fetch_route_schedules_for_line("line:C14", "20240101")

            # Should raise RetryError when all retries are exhausted
            assert "RetryError" in str(exc_info.value)


class TestExtractRelevantSchedules:
    """Test the extract_relevant_schedules function."""

    def test_extract_schedules_with_both_stations(self):
        """Test extracting schedules that include both Compiègne and Paris Nord."""
        route_schedules_data = {
            "route_schedules": [
                {
                    "display_informations": {"label": "C14", "trip_short_name": "12345"},
                    "table": {
                        "headers": [
                            {
                                "display_informations": {"trip_short_name": "12345"},
                                "links": [{"type": "vehicle_journey", "id": "vehicle_journey:12345"}],
                            }
                        ],
                        "rows": [
                            {
                                "stop_point": {"stop_area": {"id": COMPIEGNE_STATION_ID}, "name": "Compiègne"},
                                "date_times": [{"base_date_time": "20240101T080000", "date_time": "20240101T080500"}],
                            },
                            {
                                "stop_point": {
                                    "stop_area": {"id": PARIS_GARE_DU_NORD_STATION_ID},
                                    "name": "Paris Nord",
                                },
                                "date_times": [{"base_date_time": "20240101T090000", "date_time": "20240101T091000"}],
                            },
                        ],
                    },
                }
            ]
        }

        result = extract_relevant_schedules(route_schedules_data, COMPIEGNE_STATION_ID, PARIS_GARE_DU_NORD_STATION_ID)

        assert len(result) == 1
        assert result[0]["train_number"] == "12345"
        assert result[0]["vehicle_journey_id"] == "vehicle_journey:12345"

        # Should have identified Compiègne as departure and Paris Nord as arrival
        assert result[0]["from_row"]["stop_point"]["stop_area"]["id"] == COMPIEGNE_STATION_ID
        assert result[0]["to_row"]["stop_point"]["stop_area"]["id"] == PARIS_GARE_DU_NORD_STATION_ID

    def test_no_schedules_with_both_stations(self):
        """Test that no schedules are returned when both stations aren't present."""
        route_schedules_data = {
            "route_schedules": [
                {
                    "display_informations": {"label": "C14", "trip_short_name": "12345"},
                    "table": {
                        "headers": [
                            {
                                "display_informations": {"trip_short_name": "12345"},
                                "links": [{"type": "vehicle_journey", "id": "vehicle_journey:12345"}],
                            }
                        ],
                        "rows": [
                            {
                                "stop_point": {"stop_area": {"id": COMPIEGNE_STATION_ID}, "name": "Compiègne"},
                                "date_times": [{"base_date_time": "20240101T080000", "date_time": "20240101T080500"}],
                            }
                            # Missing Paris Nord station
                        ],
                    },
                }
            ]
        }

        result = extract_relevant_schedules(route_schedules_data, COMPIEGNE_STATION_ID, PARIS_GARE_DU_NORD_STATION_ID)

        # Should return empty list since both stations aren't present
        assert len(result) == 0

    def test_empty_route_schedules(self):
        """Test with empty route schedules data."""
        route_schedules_data = {"route_schedules": []}

        result = extract_relevant_schedules(route_schedules_data, COMPIEGNE_STATION_ID, PARIS_GARE_DU_NORD_STATION_ID)

        assert len(result) == 0

