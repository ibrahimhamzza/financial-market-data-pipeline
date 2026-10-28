import json

import pandas as pd
import pytest
import requests

from ingestion.api_client import (
    APIClientError,
    PayloadValidationError,
    parse_alpha_vantage_payload,
    validate_http_response,
)


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, content=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.content = content if content is not None else (b"{}" if payload is not None else b"")
        self.text = text or (json.dumps(payload) if payload is not None else "")

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


def test_validate_http_rejects_non_200():
    with pytest.raises(APIClientError):
        validate_http_response(_FakeResponse(status_code=500, text="boom", content=b"boom"))


def test_validate_http_rejects_empty_body():
    with pytest.raises(APIClientError):
        validate_http_response(_FakeResponse(status_code=200, payload=None, content=b""))


def test_parse_alpha_vantage_happy_path():
    payload = {
        "Meta Data": {"2. Symbol": "AAPL"},
        "Time Series (Daily)": {
            "2024-01-03": {
                "1. open": "100",
                "2. high": "110",
                "3. low": "90",
                "4. close": "105",
                "5. volume": "1000",
            },
            "2024-01-02": {
                "1. open": "99",
                "2. high": "101",
                "3. low": "98",
                "4. close": "100",
                "5. volume": "900",
            },
        },
    }
    frame = parse_alpha_vantage_payload(payload, "aapl")
    assert list(frame["symbol"].unique()) == ["AAPL"]
    assert len(frame) == 2
    assert frame.iloc[0]["trade_date"] == pd.Timestamp("2024-01-02")
    assert frame.iloc[1]["close_price"] == 105.0


def test_parse_alpha_vantage_rate_limit_note():
    with pytest.raises(PayloadValidationError):
        parse_alpha_vantage_payload({"Note": "call frequency"}, "AAPL")


def test_validate_http_accepts_json(monkeypatch):
    payload = {"ok": True}

    class Response(requests.Response):
        pass

    response = _FakeResponse(status_code=200, payload=payload, content=b'{"ok": true}')
    assert validate_http_response(response) == payload
