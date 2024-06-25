"""
"""

from botocore.exceptions import ClientError
from wikimedia.utilities.helpers import Text


class Summary:
    """
    Summarizes events"""

    partner = None
    log_url = None
    tracker = None
    event_type = None

    def __init__(self, partner, log_url, tracker, event_type):
        self.partner = partner
        self.log_url = log_url
        self.tracker = tracker
        self.event_type = event_type

    def subject(self):
        """
        Returns the subject of the email."""
        return f"{self.partner.upper()} - Wikimedia {self.event_type} finished"

    def body_text(self):
        """
        Returns the body of the email in plain text format."""
        return f"""
            Wikimedia {self.event_type} summary for {self.partner.upper()}.

            DPLA records
              - Attempted.....{Text.number_fmt(self.tracker.item_cnt)}
              - Successful....{0}
              - Failed........{0}

            Images
              - Attempted.....{Text.number_fmt(self.tracker.image_attempted_cnt)}
              - Successful....{Text.number_fmt(self.tracker.image_success_cnt)}
              - Skipped.......{Text.number_fmt(self.tracker.image_skip_cnt)}
              - Failed........{Text.number_fmt(self.tracker.image_fail_cnt)}

            Storage
              - Added.........{Text.sizeof_fmt(self.tracker.image_size_session)}
              - Total.........{Text.sizeof_fmt(self.tracker.image_size_total)}

            Log file available at {self.log_url}
        """

    def body_html(self):
        """
        Returns the body of the email in HTML format."""
        return f"""<pre>{self.body_text()}</pre>"""


# Taken from Amzaon example code:
# https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/python/example_code/ses/ses_email.py
class SesMailSender:
    """Encapsulates functions to send emails with Amazon SES."""

    def __init__(self, ses_client):
        """
        :param ses_client: A Boto3 Amazon SES client.
        """
        self.ses_client = ses_client

    def send_email(self, source, destination, subject, text, html, reply_tos=None):
        """
        Sends an email.

        :param source: The source email account.
        :param destination: The destination email account.
        :param subject: The subject of the email.
        :param text: The plain text version of the body of the email.
        :param html: The HTML version of the body of the email.
        :param reply_tos: Email accounts that will receive a reply if the recipient
                          replies to the message.
        :return: The ID of the message, assigned by Amazon SES.
        """
        send_args = {
            "Source": source,
            "Destination": destination.to_service_format(),
            "Message": {
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": text}, "Html": {"Data": html}},
            },
        }
        if reply_tos is not None:
            send_args["ReplyToAddresses"] = reply_tos
        try:
            response = self.ses_client.send_email(**send_args)
            message_id = response["MessageId"]
        except ClientError:
            print(f"Couldn't send mail from {source} to {destination}.")
            raise
        else:
            return message_id


class SesDestination:
    """Contains data about an email destination."""

    def __init__(self, tos, ccs=None, bccs=None):
        """
        :param tos: The list of recipients on the 'To:' line.
        :param ccs: The list of recipients on the 'CC:' line.
        :param bccs: The list of recipients on the 'BCC:' line.
        """
        self.tos = tos
        self.ccs = ccs
        self.bccs = bccs

    def to_service_format(self):
        """
        :return: The destination data in the format expected by Amazon SES.
        """
        svc_format = {"ToAddresses": self.tos}
        if self.ccs is not None:
            svc_format["CcAddresses"] = self.ccs
        if self.bccs is not None:
            svc_format["BccAddresses"] = self.bccs
        return svc_format
