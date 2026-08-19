import email
import boto3
import os
import io
import logging
from decimal import Decimal
from urllib.parse import unquote_plus
from botocore.exceptions import ClientError
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

SILVER_BUCKET = os.environ["SILVER_BUCKET"]
ALERT_TOPIC = os.environ["ALERT_TOPIC_ARN"]
ALERT_THRESHOLD = float(os.environ.get("ALERT_THRESHOLD", "1000"))
table = dynamodb.Table("orders-live")

SUPPORTED_EXTENSIONS = {"csv", "json", "parquet"}


def read_any(data: bytes, ext: str) -> pd.DataFrame:
    buf = io.BytesIO(data)
    if ext == "csv":
        return pd.read_csv(buf)
    if ext == "json":
        return pd.read_json(buf)
    return pd.read_parquet(buf)


def process_for_live_view(df: pd.DataFrame):
    for _, row in df.iterrows():
        try:
            if pd.isna(row.get("order_id")):
                continue

            quantity = float(row.get("quantity", 0) or 0)
            unit_price = float(row.get("unit_price", 0) or 0)
            total = quantity * unit_price

            table.put_item(Item={
                "order_id": str(row["order_id"]),
                "status": str(row.get("status", "unknown")),
                "total": Decimal(str(round(total, 2))),
            })

            if total > ALERT_THRESHOLD:
                sns.publish(
                    TopicArn=ALERT_TOPIC,
                    Message=f"High-value order {row['order_id']}: ${total:.2f}",
                )
        except (ValueError, TypeError) as e:
            logger.warning(f"Skipping malformed row order_id={row.get('order_id')}: {e}")
            continue
        except ClientError as e:
            logger.error(f"AWS call failed for order_id={row.get('order_id')}: {e}")
            continue


def lambda_handler(event, context):
    results = {"parsed": 0, "skipped": 0, "failed": 0}

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        try:
            raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            msg = email.message_from_bytes(raw)
        except ClientError as e:
            logger.error(f"Could not read {key} from S3: {e}")
            results["failed"] += 1
            continue

        parts = key.split("/")
        date_path = "/".join(parts[1:4])
        found_attachment = False

        for part in msg.walk():
            if part.get_content_disposition() != "attachment":
                continue
            found_attachment = True

            filename = part.get_filename()
            if not filename or "." not in filename:
                logger.warning(f"{key}: attachment with no usable filename")
                results["skipped"] += 1
                continue

            ext = filename.rsplit(".", 1)[-1].lower()
            if ext not in SUPPORTED_EXTENSIONS:
                logger.warning(f"{key}: unsupported type '{ext}', skipping")
                results["skipped"] += 1
                continue

            try:
                data = part.get_payload(decode=True)
                silver_key = f"silver/{ext}/{date_path}/{filename}"
                s3.put_object(Bucket=SILVER_BUCKET, Key=silver_key, Body=data)

                df = read_any(data, ext)
                process_for_live_view(df)
                results["parsed"] += 1

            except Exception as e:
                logger.error(f"Failed on attachment {filename} in {key}: {e}")
                results["failed"] += 1
                continue

        if not found_attachment:
            logger.info(f"{key}: no attachments")

    logger.info(f"Done: {results}")
    return results
