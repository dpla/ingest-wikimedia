# Description: This file contains the code to send emails using Amazon SES.
# Taken from Amzaon example code:
#   https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/python/example_code/ses/ses_email.py

from botocore.exceptions import ClientError

class UploadSummary():
    """
    Summarizes upload events"""

    partner_name = ""
    total_upload = 0
    log_url = ""
    status = None

    def ___init___(self, partner_name, log_url, total_upload, status):
        self.partner_name = partner_name
        self.total_upload = total_upload
        self.log_url = log_url
        self.status = status

    def subject(self):
        """
        Returns the subject of the email."""
        return f"{self.partner_name.upper()} - Wikimedia upload finished"
    
    def body_text(self):
        """
        Returns the body of the email in plain text format."""

        return f"""
            Finished uploading all Wikimedia assets for {self.partner_name.upper()}.     
        
            DPLA records: TBD
            ----------------------------------------
            Images
            - Attempted: {self.status.download_count + self.status.skip_count + self.status.fail_count}
            - Uploaded: {self.status.download_count}
            - Skipped: {self.status.skip_count}
            - Failed: {self.status.fail_count}
            ----------------------------------------
            File information
            - Added: {self.total_upload}
            ----------------------------------------
            Log file available at {self.log_url}
        """
    def body_html(self):
        pass

class DownloadSummary:
    """
    Summarizes download events"""
    partner_name = ""
    total_download = 0
    log_url = ""
    status = None

    def __init__(self, partner_name, log_url, total_download, status):
        self.partner_name = partner_name
        self.total_download = total_download
        self.log_url = log_url
        self.status = status

    def subject(self):
        """
        Returns the subject of the email."""
        return f"{self.partner_name.upper()} - Wikimedia download finished"
    
    def body_text(self):
        """
        Returns the body of the email in plain text format."""
        return f"""
            Finished downloading all Wikimedia assets for {self.partner_name.upper()}.     
        
            DPLA records: TBD
            ----------------------------------------
            Images
            - Attempted: {self.status.download_count + self.status.skip_count + self.status.fail_count}
            - Downloaded: {self.status.download_count}
            - Skipped: {self.status.skip_count}
            - Failed: {self.status.fail_count}
            ----------------------------------------
            File information
            - Downloaded: TBD
            - All records: {self.total_download}
            ----------------------------------------
            Log file available at {self.log_url}
        """

    def body_html(self):
        """
        Returns the body of the email in HTML format."""
        return f"""
            <html>
            <head>
                <meta content="text/html; charset=UTF-8" http-equiv="content-type">
                <style type="text/css">.lst-kix_ugfaekz7c25d-4>li:before{{content:"-  "}}.lst-kix_75qdqn1hz53m-5>li:before{{content:"-  "}}.lst-kix_75qdqn1hz53m-7>li:before{{content:"-  "}}.lst-kix_ugfaekz7c25d-1>li:before{{content:"-  "}}.lst-kix_ugfaekz7c25d-5>li:before{{content:"-  "}}.lst-kix_75qdqn1hz53m-4>li:before{{content:"-  "}}.lst-kix_75qdqn1hz53m-8>li:before{{content:"-  "}}.lst-kix_ugfaekz7c25d-0>li:before{{content:"-  "}}.lst-kix_ugfaekz7c25d-8>li:before{{content:"-  "}}.lst-kix_75qdqn1hz53m-1>li:before{{content:"-  "}}.lst-kix_75qdqn1hz53m-3>li:before{{content:"-  "}}.lst-kix_ugfaekz7c25d-6>li:before{{content:"-  "}}.lst-kix_ugfaekz7c25d-7>li:before{{content:"-  "}}ul.lst-kix_75qdqn1hz53m-7{{list-style-type:none}}.lst-kix_75qdqn1hz53m-2>li:before{{content:"-  "}}ul.lst-kix_75qdqn1hz53m-8{{list-style-type:none}}.lst-kix_75qdqn1hz53m-6>li:before{{content:"-  "}}ul.lst-kix_ugfaekz7c25d-0{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-1{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-2{{list-style-type:none}}ul.lst-kix_75qdqn1hz53m-5{{list-style-type:none}}ul.lst-kix_75qdqn1hz53m-6{{list-style-type:none}}ul.lst-kix_75qdqn1hz53m-3{{list-style-type:none}}ul.lst-kix_75qdqn1hz53m-4{{list-style-type:none}}ul.lst-kix_75qdqn1hz53m-1{{list-style-type:none}}ul.lst-kix_75qdqn1hz53m-2{{list-style-type:none}}.lst-kix_75qdqn1hz53m-0>li:before{{content:"-  "}}ul.lst-kix_75qdqn1hz53m-0{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-3{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-4{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-5{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-6{{list-style-type:none}}.lst-kix_ugfaekz7c25d-2>li:before{{content:"-  "}}ul.lst-kix_ugfaekz7c25d-7{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-8{{list-style-type:none}}.lst-kix_ugfaekz7c25d-3>li:before{{content:"-  "}}ol{{margin:0;padding:0}}table td,table th{{padding:0}}.c5{{border-right-style:solid;padding:-9.4pt -9.4pt -9.4pt -9.4pt;border-bottom-color:#ffffff;border-top-width:1pt;border-right-width:1pt;border-left-color:#ffffff;vertical-align:middle;border-right-color:#ffffff;border-left-width:1pt;border-top-style:solid;border-left-style:solid;border-bottom-width:1pt;width:99pt;border-top-color:#ffffff;border-bottom-style:solid}}.c7{{border-right-style:solid;padding:-9.4pt -9.4pt -9.4pt -9.4pt;border-bottom-color:#ffffff;border-top-width:1pt;border-right-width:1pt;border-left-color:#ffffff;vertical-align:middle;border-right-color:#ffffff;border-left-width:1pt;border-top-style:solid;border-left-style:solid;border-bottom-width:1pt;width:109.5pt;border-top-color:#ffffff;border-bottom-style:solid}}.c12{{border-right-style:solid;padding:-9.4pt -9.4pt -9.4pt -9.4pt;border-bottom-color:#ffffff;border-top-width:1pt;border-right-width:1pt;border-left-color:#ffffff;vertical-align:middle;border-right-color:#ffffff;border-left-width:1pt;border-top-style:solid;border-left-style:solid;border-bottom-width:1pt;width:218.2pt;border-top-color:#ffffff;border-bottom-style:solid}}.c0{{border-right-style:solid;padding:-9.4pt -9.4pt -9.4pt -9.4pt;border-bottom-color:#ffffff;border-top-width:1pt;border-right-width:1pt;border-left-color:#ffffff;vertical-align:middle;border-right-color:#ffffff;border-left-width:1pt;border-top-style:solid;border-left-style:solid;border-bottom-width:1pt;width:208.5pt;border-top-color:#ffffff;border-bottom-style:solid}}.c3{{padding-top:0pt;padding-bottom:0pt;line-height:1.15;orphans:2;widows:2;text-align:left;height:11pt}}.c10{{color:#000000;font-weight:700;text-decoration:none;vertical-align:baseline;font-size:11pt;font-family:"Arial";font-style:normal}}.c2{{color:#000000;font-weight:400;text-decoration:none;vertical-align:baseline;font-size:11pt;font-family:"Arial";font-style:normal}}.c8{{padding-top:0pt;padding-bottom:0pt;line-height:1.15;orphans:2;widows:2;text-align:left}}.c4{{padding-top:0pt;padding-bottom:0pt;line-height:1.0;text-align:left;height:11pt}}.c11{{text-decoration-skip-ink:none;-webkit-text-decoration-skip:none;color:#1155cc;text-decoration:underline}}.c15{{border-spacing:0;border-collapse:collapse;margin-right:auto}}.c6{{padding-top:0pt;padding-bottom:0pt;line-height:1.0;text-align:left}}.c9{{background-color:#ffffff;max-width:468pt;padding:72pt 72pt 72pt 72pt}}.c13{{color:inherit;text-decoration:inherit}}.c14{{font-weight:700}}.c1{{height:0pt}}.title{{padding-top:0pt;color:#000000;font-size:26pt;padding-bottom:3pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}.subtitle{{padding-top:0pt;color:#666666;font-size:15pt;padding-bottom:16pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}li{{color:#000000;font-size:11pt;font-family:"Arial"}}p{{margin:0;color:#000000;font-size:11pt;font-family:"Arial"}}h1{{padding-top:20pt;color:#000000;font-size:20pt;padding-bottom:6pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}h2{{padding-top:18pt;color:#000000;font-size:16pt;padding-bottom:6pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}h3{{padding-top:16pt;color:#434343;font-size:14pt;padding-bottom:4pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}h4{{padding-top:14pt;color:#666666;font-size:12pt;padding-bottom:4pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}h5{{padding-top:12pt;color:#666666;font-size:11pt;padding-bottom:4pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}h6{{padding-top:12pt;color:#666666;font-size:11pt;padding-bottom:4pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;font-style:italic;orphans:2;widows:2;text-align:left}}</style>
            </head>
            <body class="c9 doc-content">
                <p class="c8"><span>Finished downloading all Wikimedia assets for </span><span class="c14">{self.partner_name.upper()}. </span><span>Click </span><span class="c11"><a class="c13" href="{self.log_url}">here</a></span><span>&nbsp;to download the complete log file</span><span class="c10">.</span></p>
                <p class="c3"><span class="c10"></span></p>
                <hr>
                <p class="c3"><span class="c2"></span></p>
                <p class="c3"><span class="c2"></span></p>
                <a id="t.8c4053fb665b48bc85358613455d5b5f5ab451af"></a><a id="t.0"></a>
                <table class="c15">
                    <tr class="c1">
                        <td class="c5" colspan="1" rowspan="1">
                        <p class="c8"><span class="c10">DPLA records</span></p>
                        </td>
                        <td class="c7" colspan="1" rowspan="1">
                        <p class="c4"><span class="c2"></span></p>
                        </td>
                        <td class="c12" colspan="1" rowspan="1">
                        <p class="c6"><span class="c2">___DPLA_RECORDS</span></p>
                        </td>
                    </tr>
                    <tr class="c1">
                        <td class="c5" colspan="1" rowspan="1">
                        <p class="c8"><span class="c14">Images</span></p>
                        </td>
                        <td class="c7" colspan="1" rowspan="1">
                        <p class="c3"><span class="c2"></span></p>
                        </td>
                        <td class="c12" colspan="1" rowspan="1">
                        <p class="c4"><span class="c2"></span></p>
                        </td>
                    </tr>
                    <tr class="c1">
                        <td class="c5" colspan="1" rowspan="1">
                        <p class="c4"><span class="c2"></span></p>
                        </td>
                        <td class="c7" colspan="1" rowspan="1">
                        <p class="c8"><span class="c2">Attempted</span></p>
                        </td>
                        <td class="c12" colspan="1" rowspan="1">
                        <p class="c6"><span class="c2">{self.status.download_count + self.status.skip_count + self.status.fail_count}</span></p>
                        </td>
                    </tr>
                    <tr class="c1">
                        <td class="c5" colspan="1" rowspan="1">
                        <p class="c4"><span class="c2"></span></p>
                        </td>
                        <td class="c7" colspan="1" rowspan="1">
                        <p class="c8"><span class="c2">Downloaded</span></p>
                        </td>
                        <td class="c12" colspan="1" rowspan="1">
                        <p class="c6"><span class="c2">{self.status.download_count}</span></p>
                        </td>
                    </tr>
                    <tr class="c1">
                        <td class="c5" colspan="1" rowspan="1">
                        <p class="c4"><span class="c2"></span></p>
                        </td>
                        <td class="c7" colspan="1" rowspan="1">
                        <p class="c8"><span class="c2">Skipped</span></p>
                        </td>
                        <td class="c12" colspan="1" rowspan="1">
                        <p class="c6"><span class="c2">{self.status.skip_count}</span></p>
                        </td>
                    </tr>
                    <tr class="c1">
                        <td class="c5" colspan="1" rowspan="1">
                        <p class="c4"><span class="c2"></span></p>
                        </td>
                        <td class="c7" colspan="1" rowspan="1">
                        <p class="c8"><span class="c2">Failed</span></p>
                        </td>
                        <td class="c12" colspan="1" rowspan="1">
                        <p class="c6"><span class="c2">{self.status.fail_count}</span></p>
                        </td>
                    </tr>
                    <tr class="c1">
                        <td class="c0" colspan="2" rowspan="1">
                        <p class="c6"><span class="c10">File Information</span></p>
                        </td>
                        <td class="c12" colspan="1" rowspan="1">
                        <p class="c4"><span class="c2"></span></p>
                        </td>
                    </tr>
                    <tr class="c1">
                        <td class="c5" colspan="1" rowspan="1">
                        <p class="c4"><span class="c2"></span></p>
                        </td>
                        <td class="c7" colspan="1" rowspan="1">
                        <p class="c6"><span class="c2">Downloaded</span></p>
                        </td>
                        <td class="c12" colspan="1" rowspan="1">
                        <p class="c6"><span class="c2">___DOWNLOADED</span></p>
                        </td>
                    </tr>
                    <tr class="c1">
                        <td class="c5" colspan="1" rowspan="1">
                        <p class="c4"><span class="c2"></span></p>
                        </td>
                        <td class="c7" colspan="1" rowspan="1">
                        <p class="c6"><span class="c2">All records</span></p>
                        </td>
                        <td class="c12" colspan="1" rowspan="1">
                        <p class="c6"><span class="c2">{self.total_download}</span></p>
                        </td>
                    </tr>
                </table>
                <p class="c3"><span class="c2"></span></p>
            </body>
            </html>
        """

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

        Note: If your account is in the Amazon SES  sandbox, the source and
        destination email accounts must both be verified.

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
            print(
                "Couldn't send mail from %s to %s.", source, destination.tos)
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
