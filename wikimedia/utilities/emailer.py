"""
"""

from botocore.exceptions import ClientError
from utilities.format import sizeof_fmt, number_fmt
from mjml import mjml_to_html

class UploadSummary:
    """
    Summarizes upload events"""

    partner = None
    log_url = None
    tracker = None

    def __init__(self, partner, log_url, tracker):
        self.partner = partner
        self.log_url = log_url
        self.tracker = tracker

    def subject(self):
        """
        Returns the subject of the email."""
        return f"{self.partner.upper()} - Wikimedia upload finished"

    def body_text(self):
        """
        Returns the body of the email in plain text format."""
        return f"""
            Finished uploading all Wikimedia assets for {self.partner.upper()}.

            DPLA records: {self.tracker.item_cnt}
            ----------------------------------------
            Images
            - Attempted: {self.tracker.image_attempted_cnt}
            - Uploaded: {self.tracker.image_fail_cnt}
            - Skipped: {self.tracker.image_skip_cnt}
            - Failed: {self.tracker.image_success_cnt}
            ----------------------------------------
            File information
            - Added: {sizeof_fmt(self.tracker.image_size_session)}
            - Total: {sizeof_fmt(self.tracker.image_size_total)}
            ----------------------------------------
            Log file available at {self.log_url}
        """

    def body_html(self):
        return f"""
<mjml>
  <mj-head>
      <mj-attributes>
        <mj-class name="mjclass" color="black" font-size="20px" padding="0px 0px 0px 0px" />
      </mj-attributes>
    </mj-head>

    <mj-body>
        <mj-section>
            <mj-column>
                <mj-text mj-class="mjclass">Wikimedia upload summary for {self.partner.upper()}</mj-text>
            </mj-column>
        </mj-section>

        <mj-section>
            <mj-column>
                <mj-table align="left" vertical-align="middle" padding="0px">
                  <!-- COLUMN 1 -->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                    <th style="padding: 0 15px 0 0;width:75px; ">DPLA Items</th>
                    <td style="padding: 0 15px;">{number_fmt(self.tracker.item_cnt)}</td>
                    <td style="padding: 0 15px;"></td>
                  </tr>

                  <!-- TODO FAILED DPLA RECORDS COUNT  -->
                  <!-- TODO SUCCESSFUL DPLA RECORDS COUNT  -->

                  <!-- IMAGES ROW  JUST A HEADER/LABEL-->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                    <th style="padding: 0 15px 0 0; width:75px">Images</th>
                  <td style="padding: 0 15px;"></td>
                  <td style="padding: 0 15px;"></td>
                  </tr>
                  <!-- ATTEMPTED ROW  -->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                    <td style="padding: 0 15px;width:75px; text-align:left">Attempted</td>
                    <td style="padding: 0 15px;width:100px">{number_fmt(self.tracker.image_attempted_cnt)}</td>
                  </tr>
                  <!-- DOWNLOAD ROW  -->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                    <td style="padding: 0 15px;">Downloaded</td>
                    <td style="padding: 0 15px;">{number_fmt(self.tracker.image_success_cnt)}</td>
                  </tr>
                  <!-- SKIP ROW  -->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                    <td style="padding: 0 15px;">Already in S3</td>
                    <td style="padding: 0 15px;">{number_fmt(self.tracker.image_skip_cnt)}</td>
                  </tr>
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                    <td style="padding: 0 15px;">Failed</td>
                    <td style="padding: 0 15px;">{number_fmt(self.tracker.image_fail_cnt)}</td>
                  </tr>
                  <!-- FILE INFO ROW  HEADER ONLY-->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                      <th style="padding: 0 15px 0 0; width:150px">File information</th>
                        <td style="padding: 0 15px;"></td>
                        <td style="padding: 0 15px;"></td>
                  </tr>
                  <!-- NEW DOWNLOAD ROW  -->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                      <td style="padding: 0 15px;">New downloads</td>
                      <td style="padding: 0 15px;">{sizeof_fmt(self.tracker.image_size_session)}</td>
                  </tr>
                  <!-- TOTAL SIZE ROW  -->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                      <td style="padding: 0 15px;">All {self.partner.upper()} images</td>
                      <td style="padding: 0 15px;">{sizeof_fmt(self.tracker.image_size_total)}</td>
                  </tr>
                </mj-table>
            </mj-column>
        </mj-section>

        <mj-section>
            <mj-column>
                <mj-text mj-class="mjclass"><a href="{self.log_url}">Click here</a> for complete log file</mj-text>
            </mj-column>
        </mj-section>

    </mj-body>
</mjml>"""

class DownloadSummary:
    """
    Summarizes download events"""
    partner = ""
    log_url = ""
    tracker = None

    def __init__(self, partner, log_url, tracker):
        self.partner = partner
        self.log_url = log_url
        self.tracker = tracker

    def subject(self):
        """
        Returns the subject of the email."""
        return f"{self.partner.upper()} - Wikimedia download finished"

    def body_text(self):
        """
        Returns the body of the email in plain text format."""
        return f"""
            Finished downloading all Wikimedia assets for {self.partner.upper()}.

            DPLA records: TBD
            ----------------------------------------
            Images
            - Attempted: {self.tracker.image_attempted_cnt}
            - Downloaded: {self.tracker.image_success_cnt}
            - Skipped: {self.tracker.image_skip_cnt}
            - Failed: {self.tracker.image_fail_cnt}
            ----------------------------------------
            File information
            - Downloaded: TBD
            - All records: {sizeof_fmt(self.tracker.image_size_total)}
            ----------------------------------------
            Log file available at {self.log_url}
        """

    def body_html(self):
        """
        Returns the body of the email in HTML format."""
        return f"""
<mjml>
  <mj-head>
      <mj-attributes>
        <mj-class name="mjclass" color="black" font-size="20px" padding="0px 0px 0px 0px" />
      </mj-attributes>
    </mj-head>

    <mj-body>
        <mj-section>
            <mj-column>
                <mj-text mj-class="mjclass">Wikimedia download summary for {self.partner.upper()}</mj-text>
            </mj-column>
        </mj-section>

        <mj-section>
            <mj-column>
                <mj-table align="left" vertical-align="middle" padding="0px">
                  <!-- COLUMN 1 -->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                    <th style="padding: 0 15px 0 0;width:75px; ">DPLA Items</th>
                    <td style="padding: 0 15px;">{number_fmt(self.tracker.item_cnt)}</td>
                    <td style="padding: 0 15px;"></td>
                  </tr>

                  <!-- TODO FAILED DPLA RECORDS COUNT  -->
                  <!-- TODO SUCCESSFUL DPLA RECORDS COUNT  -->

                  <!-- IMAGES ROW  JUST A HEADER/LABEL-->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                    <th style="padding: 0 15px 0 0; width:75px">Images</th>
                  <td style="padding: 0 15px;"></td>
                  <td style="padding: 0 15px;"></td>
                  </tr>
                  <!-- ATTEMPTED ROW  -->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                    <td style="padding: 0 15px;width:75px; text-align:left">Attempted</td>
                    <td style="padding: 0 15px;width:100px">{number_fmt(self.tracker.image_attempted_cnt)}</td>
                  </tr>
                  <!-- DOWNLOAD ROW  -->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                    <td style="padding: 0 15px;">Downloaded</td>
                    <td style="padding: 0 15px;">{number_fmt(self.tracker.image_success_cnt)}</td>
                  </tr>
                  <!-- SKIP ROW  -->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                    <td style="padding: 0 15px;">Already in S3</td>
                    <td style="padding: 0 15px;">{number_fmt(self.tracker.image_skip_cnt)}</td>
                  </tr>
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                    <td style="padding: 0 15px;">Failed</td>
                    <td style="padding: 0 15px;">{number_fmt(self.tracker.image_fail_cnt)}</td>
                  </tr>
                  <!-- FILE INFO ROW  HEADER ONLY-->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                      <th style="padding: 0 15px 0 0; width:150px">File information</th>
                        <td style="padding: 0 15px;"></td>
                        <td style="padding: 0 15px;"></td>
                  </tr>
                  <!-- NEW DOWNLOAD ROW  -->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                      <td style="padding: 0 15px;">New downloads</td>
                      <td style="padding: 0 15px;">{sizeof_fmt(self.tracker.image_size_session)}</td>
                  </tr>
                  <!-- TOTAL SIZE ROW  -->
                  <tr style="border-bottom:1px solid #ecedee;text-align:left;padding:15px 0;">
                      <td style="padding: 0 15px;">All {self.partner.upper()} images</td>
                      <td style="padding: 0 15px;">{sizeof_fmt(self.tracker.image_size_total)}</td>
                  </tr>
                </mj-table>
            </mj-column>
        </mj-section>

        <mj-section>
            <mj-column>
                <mj-text mj-class="mjclass"><a href="{self.log_url}">Click here</a> for complete log file</mj-text>
            </mj-column>
        </mj-section>

    </mj-body>
</mjml>
        """


# Taken from Amzaon example code:
# >  https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/python/example_code/ses/ses_email.py
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
        result = mjml_to_html(html)
        html: str = result.html

        send_args = {
            'Source': source,
            'Destination': destination.to_service_format(),
            'Message': {
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': text}, 'Html': {'Data': html}}}}
        if reply_tos is not None:
            send_args['ReplyToAddresses'] = reply_tos
        try:
            response = self.ses_client.send_email(**send_args)
            message_id = response['MessageId']
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
        svc_format = {'ToAddresses': self.tos}
        if self.ccs is not None:
            svc_format['CcAddresses'] = self.ccs
        if self.bccs is not None:
            svc_format['BccAddresses'] = self.bccs
        return svc_format
