import io
from decimal import Decimal
from email.message import EmailMessage
from unittest import mock

import pandas as pd
import pytest
from botocore.exceptions import ClientError


def client_error():
    return ClientError({"Error": {"Code": "Boom", "Message": "test"}}, "TestOperation")


def make_email(attachments=()):
    msg = EmailMessage()
    msg["Subject"] = "New orders"
    msg.set_content("Please find the orders attached.")
    for filename, content in attachments:
        msg.add_attachment(
            content, maintype="application", subtype="octet-stream", filename=filename
        )
    return msg.as_bytes()


def s3_event(key="raw/2026/08/13/101.eml", bucket="raw-bucket"):
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]}


def serve_email(parser, raw_bytes):
    parser.s3.get_object.return_value = {"Body": io.BytesIO(raw_bytes)}


CSV_ORDERS = (
    b"order_id,customer_id,product,category,quantity,unit_price,order_date,status\n"
    b"o-1,c-1,Mouse,Electronics,2,19.99,2026-08-10,pending\n"
    b"o-2,c-2,Table,Home,1,149.50,2026-08-11,shipped\n"
)


# ---------- read_any ----------

def test_read_any_csv(parser):
    df = parser.read_any(CSV_ORDERS, "csv")
    assert list(df["order_id"]) == ["o-1", "o-2"]


def test_read_any_json(parser):
    data = pd.DataFrame({"order_id": ["o-1"], "quantity": [3]}).to_json().encode()
    df = parser.read_any(data, "json")
    assert df.loc[0, "order_id"] == "o-1"
    assert df.loc[0, "quantity"] == 3


def test_read_any_parquet(parser):
    buf = io.BytesIO()
    pd.DataFrame({"order_id": ["o-1"], "quantity": [3]}).to_parquet(buf)
    df = parser.read_any(buf.getvalue(), "parquet")
    assert df.loc[0, "order_id"] == "o-1"


# ---------- process_for_live_view ----------

def orders_df(**overrides):
    row = {"order_id": "o-1", "quantity": 2, "unit_price": 19.99, "status": "pending"}
    row.update(overrides)
    return pd.DataFrame([row])


def test_live_view_stores_order_with_computed_total(parser):
    parser.table = mock.MagicMock()

    parser.process_for_live_view(orders_df())

    parser.table.put_item.assert_called_once_with(
        Item={"order_id": "o-1", "status": "pending", "total": Decimal("39.98")}
    )


def test_live_view_skips_rows_without_order_id(parser):
    parser.table = mock.MagicMock()

    parser.process_for_live_view(orders_df(order_id=None))

    parser.table.put_item.assert_not_called()


def test_high_value_order_triggers_sns_alert(parser):
    parser.table = mock.MagicMock()

    parser.process_for_live_view(orders_df(quantity=2, unit_price=600))

    parser.sns.publish.assert_called_once()
    kwargs = parser.sns.publish.call_args.kwargs
    assert kwargs["TopicArn"] == "arn:aws:sns:ap-south-1:123456789012:alerts"
    assert "o-1" in kwargs["Message"]
    assert "1200.00" in kwargs["Message"]


def test_order_at_threshold_does_not_alert(parser):
    parser.table = mock.MagicMock()

    parser.process_for_live_view(orders_df(quantity=1, unit_price=1000))

    parser.sns.publish.assert_not_called()


def test_malformed_row_is_skipped_and_others_still_processed(parser):
    parser.table = mock.MagicMock()
    df = pd.DataFrame(
        [
            {"order_id": "bad", "quantity": "abc", "unit_price": 5, "status": "x"},
            {"order_id": "good", "quantity": 1, "unit_price": 5, "status": "x"},
        ]
    )

    parser.process_for_live_view(df)

    parser.table.put_item.assert_called_once()
    assert parser.table.put_item.call_args.kwargs["Item"]["order_id"] == "good"


def test_aws_error_on_one_row_does_not_stop_the_rest(parser):
    parser.table = mock.MagicMock()
    parser.table.put_item.side_effect = [client_error(), None]
    df = pd.DataFrame(
        [
            {"order_id": "o-1", "quantity": 1, "unit_price": 5, "status": "x"},
            {"order_id": "o-2", "quantity": 1, "unit_price": 5, "status": "x"},
        ]
    )

    parser.process_for_live_view(df)

    assert parser.table.put_item.call_count == 2


# ---------- lambda_handler ----------

def test_csv_attachment_is_saved_to_silver_and_loaded_to_live_view(parser):
    parser.table = mock.MagicMock()
    serve_email(parser, make_email([("orders.csv", CSV_ORDERS)]))

    result = parser.lambda_handler(s3_event(), None)

    assert result == {"parsed": 1, "skipped": 0, "failed": 0}
    parser.s3.put_object.assert_called_once()
    kwargs = parser.s3.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "test-silver-bucket"
    assert kwargs["Key"] == "silver/csv/2026/08/13/orders.csv"
    assert kwargs["Body"] == CSV_ORDERS
    assert parser.table.put_item.call_count == 2


def test_unsupported_attachment_type_is_skipped(parser):
    serve_email(parser, make_email([("notes.txt", b"hello")]))

    result = parser.lambda_handler(s3_event(), None)

    assert result == {"parsed": 0, "skipped": 1, "failed": 0}
    parser.s3.put_object.assert_not_called()


def test_attachment_without_extension_is_skipped(parser):
    serve_email(parser, make_email([("README", b"hello")]))

    result = parser.lambda_handler(s3_event(), None)

    assert result == {"parsed": 0, "skipped": 1, "failed": 0}


def test_email_without_attachments_changes_nothing(parser):
    serve_email(parser, make_email())

    result = parser.lambda_handler(s3_event(), None)

    assert result == {"parsed": 0, "skipped": 0, "failed": 0}
    parser.s3.put_object.assert_not_called()


def test_unreadable_s3_object_is_counted_as_failed(parser):
    parser.s3.get_object.side_effect = client_error()

    result = parser.lambda_handler(s3_event(), None)

    assert result == {"parsed": 0, "skipped": 0, "failed": 1}


def test_corrupt_attachment_is_counted_as_failed(parser):
    parser.table = mock.MagicMock()
    serve_email(parser, make_email([("orders.csv", b"")]))

    result = parser.lambda_handler(s3_event(), None)

    assert result == {"parsed": 0, "skipped": 0, "failed": 1}


def test_url_encoded_s3_key_is_decoded(parser):
    serve_email(parser, make_email())

    parser.lambda_handler(s3_event(key="raw/2026/08/13/a+b%21.eml"), None)

    assert parser.s3.get_object.call_args.kwargs["Key"] == "raw/2026/08/13/a b!.eml"


def test_event_without_records_returns_zero_counts(parser):
    assert parser.lambda_handler({}, None) == {"parsed": 0, "skipped": 0, "failed": 0}
