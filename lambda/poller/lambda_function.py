import imaplib
import boto3
import os
import logging
from datetime import date
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
ssm = boto3.client("ssm")
BUCKET = os.environ["RAW_BUCKET"]


def get_param(name, decrypt=False):
    try:
        return ssm.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]
    except ClientError as e:
        logger.error(f"Failed to read SSM parameter {name}: {e}")
        raise


def get_last_uid():
    try:
        return int(get_param("/email-pipeline/last-uid"))
    except ssm.exceptions.ParameterNotFound:
        logger.info("No checkpoint yet — starting from UID 0")
        return 0


def save_last_uid(uid):
    try:
        ssm.put_parameter(
            Name="/email-pipeline/last-uid",
            Value=str(uid),
            Type="String",
            Overwrite=True,
        )
    except ClientError as e:
        logger.error(f"Failed to save checkpoint uid={uid}: {e}")


def lambda_handler(event, context):
    processed, failed = 0, 0
    imap = None

    try:
        user = get_param("/email-pipeline/gmail-user")
        pwd = get_param("/email-pipeline/gmail-app-password", decrypt=True)

        imap = imaplib.IMAP4_SSL("imap.gmail.com")
        imap.login(user, pwd)
        imap.select("inbox")

        last_uid = get_last_uid()
        status, data = imap.uid("search", None, f"UID {last_uid + 1}:*")
        if status != "OK":
            logger.error(f"IMAP search failed: {status}")
            return {"processed": 0, "failed": 0}

        uids = data[0].split()
        if not uids:
            logger.info("No new mail")
            return {"processed": 0, "failed": 0}

        max_uid_seen = last_uid

        for uid in uids:
            try:
                status, msg_data = imap.uid("fetch", uid, "(RFC822)")
                if status != "OK" or not msg_data or msg_data[0] is None:
                    logger.warning(f"Could not fetch UID {uid.decode()}, skipping")
                    failed += 1
                    continue

                raw_email = msg_data[0][1]
                key = f"raw/{date.today():%Y/%m/%d}/{uid.decode()}.eml"
                s3.put_object(Bucket=BUCKET, Key=key, Body=raw_email)
                processed += 1
                max_uid_seen = max(max_uid_seen, int(uid))

            except Exception as e:
                logger.error(f"Failed to process UID {uid.decode()}: {e}")
                failed += 1
                continue

        if max_uid_seen > last_uid:
            save_last_uid(max_uid_seen)

    except imaplib.IMAP4.error as e:
        logger.error(f"IMAP auth/connection error: {e}")
        raise
    finally:
        if imap is not None:
            try:
                imap.logout()
            except Exception:
                pass

    logger.info(f"Done: processed={processed}, failed={failed}")
    return {"processed": processed, "failed": failed}
