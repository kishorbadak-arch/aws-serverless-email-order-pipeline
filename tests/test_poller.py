import imaplib
from datetime import date

import pytest
from botocore.exceptions import ClientError


def client_error(code="Boom"):
    return ClientError({"Error": {"Code": code, "Message": "test"}}, "TestOperation")


class FakeIMAP:
    """Minimal stand-in for imaplib.IMAP4_SSL."""

    def __init__(self, uids=b"", messages=None, search_status="OK"):
        self.uids = uids
        self.messages = messages or {}
        self.search_status = search_status
        self.logged_out = False

    def login(self, user, password):
        self.credentials = (user, password)

    def select(self, mailbox):
        return ("OK", [b"1"])

    def uid(self, command, *args):
        if command == "search":
            return (self.search_status, [self.uids])
        uid = args[0].decode()
        if uid in self.messages:
            return ("OK", [(b"header", self.messages[uid])])
        return ("NO", [None])

    def logout(self):
        self.logged_out = True


@pytest.fixture
def aws(poller):
    """Configure the mocked SSM parameters used by the handler."""
    params = {
        "/email-pipeline/gmail-user": "orders@example.com",
        "/email-pipeline/gmail-app-password": "app-password",
        "/email-pipeline/last-uid": "10",
    }

    def get_parameter(Name, WithDecryption=False):
        return {"Parameter": {"Value": params[Name]}}

    poller.ssm.get_parameter.side_effect = get_parameter
    return poller


def run_handler(poller, monkeypatch, fake_imap):
    monkeypatch.setattr(poller.imaplib, "IMAP4_SSL", lambda host: fake_imap)
    return poller.lambda_handler({}, None)


# ---------- checkpoint helpers ----------

def test_get_last_uid_reads_checkpoint_as_int(aws):
    assert aws.get_last_uid() == 10


def test_get_last_uid_starts_at_zero_when_no_checkpoint(poller):
    class ParameterNotFound(ClientError):
        pass

    poller.ssm.exceptions.ParameterNotFound = ParameterNotFound
    poller.ssm.get_parameter.side_effect = ParameterNotFound(
        {"Error": {"Code": "ParameterNotFound", "Message": "missing"}}, "GetParameter"
    )
    assert poller.get_last_uid() == 0


def test_get_param_reraises_aws_errors(poller):
    poller.ssm.get_parameter.side_effect = client_error()
    with pytest.raises(ClientError):
        poller.get_param("/email-pipeline/gmail-user")


def test_save_last_uid_overwrites_parameter_as_string(poller):
    poller.save_last_uid(42)
    poller.ssm.put_parameter.assert_called_once_with(
        Name="/email-pipeline/last-uid", Value="42", Type="String", Overwrite=True
    )


def test_save_last_uid_does_not_raise_on_aws_error(poller):
    poller.ssm.put_parameter.side_effect = client_error()
    poller.save_last_uid(42)  # error is logged, not raised


# ---------- lambda_handler ----------

def test_new_emails_are_archived_and_checkpoint_advances(aws, monkeypatch):
    imap = FakeIMAP(uids=b"11 12", messages={"11": b"raw-one", "12": b"raw-two"})

    result = run_handler(aws, monkeypatch, imap)

    assert result == {"processed": 2, "failed": 0}
    today = f"{date.today():%Y/%m/%d}"
    keys = [c.kwargs["Key"] for c in aws.s3.put_object.call_args_list]
    assert keys == [f"raw/{today}/11.eml", f"raw/{today}/12.eml"]
    bodies = [c.kwargs["Body"] for c in aws.s3.put_object.call_args_list]
    assert bodies == [b"raw-one", b"raw-two"]
    assert aws.ssm.put_parameter.call_args.kwargs["Value"] == "12"
    assert imap.credentials == ("orders@example.com", "app-password")
    assert imap.logged_out


def test_no_new_mail_does_nothing(aws, monkeypatch):
    imap = FakeIMAP(uids=b"")

    result = run_handler(aws, monkeypatch, imap)

    assert result == {"processed": 0, "failed": 0}
    aws.s3.put_object.assert_not_called()
    aws.ssm.put_parameter.assert_not_called()
    assert imap.logged_out


def test_failed_imap_search_returns_zero_counts(aws, monkeypatch):
    imap = FakeIMAP(uids=b"11", search_status="NO")

    result = run_handler(aws, monkeypatch, imap)

    assert result == {"processed": 0, "failed": 0}
    aws.s3.put_object.assert_not_called()


def test_unfetchable_email_is_counted_as_failed(aws, monkeypatch):
    imap = FakeIMAP(uids=b"11 12", messages={"11": b"raw-one"})

    result = run_handler(aws, monkeypatch, imap)

    assert result == {"processed": 1, "failed": 1}


def test_s3_failure_is_counted_and_does_not_stop_the_run(aws, monkeypatch):
    aws.s3.put_object.side_effect = [None, Exception("S3 down")]
    imap = FakeIMAP(uids=b"11 12", messages={"11": b"a", "12": b"b"})

    result = run_handler(aws, monkeypatch, imap)

    assert result == {"processed": 1, "failed": 1}


def test_checkpoint_not_saved_when_nothing_was_processed(aws, monkeypatch):
    imap = FakeIMAP(uids=b"11", messages={})

    result = run_handler(aws, monkeypatch, imap)

    assert result == {"processed": 0, "failed": 1}
    aws.ssm.put_parameter.assert_not_called()


def test_imap_login_error_is_raised_and_connection_closed(aws, monkeypatch):
    class BrokenIMAP(FakeIMAP):
        def login(self, user, password):
            raise imaplib.IMAP4.error("auth failed")

    imap = BrokenIMAP()
    monkeypatch.setattr(aws.imaplib, "IMAP4_SSL", lambda host: imap)

    with pytest.raises(imaplib.IMAP4.error):
        aws.lambda_handler({}, None)

    assert imap.logged_out
