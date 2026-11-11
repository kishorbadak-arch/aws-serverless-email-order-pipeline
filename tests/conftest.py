"""Shared fixtures.

The Lambda files live in folders called ``lambda/``, and ``lambda`` is a Python
keyword, so they cannot be imported normally. They are loaded by file path
instead, with boto3 replaced by mocks so no AWS account or credentials are
needed to run the tests.
"""
import importlib.util
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load_module(name, relative_path, boto_clients, boto_resources):
    def fake_client(service, *args, **kwargs):
        return boto_clients.setdefault(service, mock.MagicMock(name=f"client-{service}"))

    def fake_resource(service, *args, **kwargs):
        return boto_resources.setdefault(service, mock.MagicMock(name=f"resource-{service}"))

    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    with mock.patch("boto3.client", side_effect=fake_client), \
            mock.patch("boto3.resource", side_effect=fake_resource):
        spec.loader.exec_module(module)
    return module


@pytest.fixture
def poller(monkeypatch):
    monkeypatch.setenv("RAW_BUCKET", "test-raw-bucket")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    return _load_module("poller_lambda", "lambda/poller/lambda_function.py", {}, {})


@pytest.fixture
def parser(monkeypatch):
    monkeypatch.setenv("SILVER_BUCKET", "test-silver-bucket")
    monkeypatch.setenv("ALERT_TOPIC_ARN", "arn:aws:sns:ap-south-1:123456789012:alerts")
    monkeypatch.setenv("ALERT_THRESHOLD", "1000")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    return _load_module("parser_lambda", "lambda/parser/lambda_function.py", {}, {})
