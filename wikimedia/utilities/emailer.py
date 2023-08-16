"""
"""

from botocore.exceptions import ClientError
from utilities.format import sizeof_fmt

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

            DPLA records: {self.tracker.dpla_count}
            ----------------------------------------
            Images
            - Attempted: {self.tracker.attempted}
            - Uploaded: {self.tracker.upload_count}
            - Skipped: {self.tracker.skip_count}
            - Failed: {self.tracker.fail_count}
            ----------------------------------------
            File information
            - Added: {sizeof_fmt(self.tracker.cumulative_size)}
            ----------------------------------------
            Log file available at {self.log_url}
        """

    def body_html(self):
        return f"""
            <html>
            <head>
                <meta content="text/html; charset=UTF-8" http-equiv="content-type">
                <style type="text/css">.lst-kix_ugfaekz7c25d-4>li:before{{content:"-  "}}.lst-kix_75qdqn1hz53m-5>li:before{{content:"-  "}}.lst-kix_75qdqn1hz53m-7>li:before{{content:"-  "}}.lst-kix_ugfaekz7c25d-1>li:before{{content:"-  "}}.lst-kix_ugfaekz7c25d-5>li:before{{content:"-  "}}.lst-kix_75qdqn1hz53m-4>li:before{{content:"-  "}}.lst-kix_75qdqn1hz53m-8>li:before{{content:"-  "}}.lst-kix_ugfaekz7c25d-0>li:before{{content:"-  "}}.lst-kix_ugfaekz7c25d-8>li:before{{content:"-  "}}.lst-kix_75qdqn1hz53m-1>li:before{{content:"-  "}}.lst-kix_75qdqn1hz53m-3>li:before{{content:"-  "}}.lst-kix_ugfaekz7c25d-6>li:before{{content:"-  "}}.lst-kix_ugfaekz7c25d-7>li:before{{content:"-  "}}ul.lst-kix_75qdqn1hz53m-7{{list-style-type:none}}.lst-kix_75qdqn1hz53m-2>li:before{{content:"-  "}}ul.lst-kix_75qdqn1hz53m-8{{list-style-type:none}}.lst-kix_75qdqn1hz53m-6>li:before{{content:"-  "}}ul.lst-kix_ugfaekz7c25d-0{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-1{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-2{{list-style-type:none}}ul.lst-kix_75qdqn1hz53m-5{{list-style-type:none}}ul.lst-kix_75qdqn1hz53m-6{{list-style-type:none}}ul.lst-kix_75qdqn1hz53m-3{{list-style-type:none}}ul.lst-kix_75qdqn1hz53m-4{{list-style-type:none}}ul.lst-kix_75qdqn1hz53m-1{{list-style-type:none}}ul.lst-kix_75qdqn1hz53m-2{{list-style-type:none}}.lst-kix_75qdqn1hz53m-0>li:before{{content:"-  "}}ul.lst-kix_75qdqn1hz53m-0{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-3{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-4{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-5{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-6{{list-style-type:none}}.lst-kix_ugfaekz7c25d-2>li:before{{content:"-  "}}ul.lst-kix_ugfaekz7c25d-7{{list-style-type:none}}ul.lst-kix_ugfaekz7c25d-8{{list-style-type:none}}.lst-kix_ugfaekz7c25d-3>li:before{{content:"-  "}}ol{{margin:0;padding:0}}table td,table th{{padding:0}}.c2{{border-right-style:solid;padding:-9.4pt -9.4pt -9.4pt -9.4pt;border-bottom-color:#ffffff;border-top-width:1pt;border-right-width:1pt;border-left-color:#ffffff;vertical-align:middle;border-right-color:#ffffff;border-left-width:1pt;border-top-style:solid;border-left-style:solid;border-bottom-width:1pt;width:99pt;border-top-color:#ffffff;border-bottom-style:solid}}.c1{{border-right-style:solid;padding:-9.4pt -9.4pt -9.4pt -9.4pt;border-bottom-color:#ffffff;border-top-width:1pt;border-right-width:1pt;border-left-color:#ffffff;vertical-align:middle;border-right-color:#ffffff;border-left-width:1pt;border-top-style:solid;border-left-style:solid;border-bottom-width:1pt;width:218.2pt;border-top-color:#ffffff;border-bottom-style:solid}}.c13{{border-right-style:solid;padding:-9.4pt -9.4pt -9.4pt -9.4pt;border-bottom-color:#ffffff;border-top-width:1pt;border-right-width:1pt;border-left-color:#ffffff;vertical-align:middle;border-right-color:#ffffff;border-left-width:1pt;border-top-style:solid;border-left-style:solid;border-bottom-width:1pt;width:109.5pt;border-top-color:#ffffff;border-bottom-style:solid}}.c10{{border-right-style:solid;padding:-9.4pt -9.4pt -9.4pt -9.4pt;border-bottom-color:#ffffff;border-top-width:1pt;border-right-width:1pt;border-left-color:#ffffff;vertical-align:middle;border-right-color:#ffffff;border-left-width:1pt;border-top-style:solid;border-left-style:solid;border-bottom-width:1pt;width:208.5pt;border-top-color:#ffffff;border-bottom-style:solid}}.c0{{color:#000000;font-weight:400;text-decoration:none;vertical-align:baseline;font-size:11pt;font-family:"Arial";font-style:normal}}.c3{{color:#000000;font-weight:700;text-decoration:none;vertical-align:baseline;font-size:11pt;font-family:"Arial";font-style:normal}}.c7{{padding-top:0pt;padding-bottom:0pt;line-height:1.15;orphans:2;widows:2;text-align:left;height:11pt}}.c11{{padding-top:0pt;padding-bottom:0pt;line-height:1.15;orphans:2;widows:2;text-align:left}}.c4{{padding-top:0pt;padding-bottom:0pt;line-height:1.0;text-align:left}}.c15{{text-decoration-skip-ink:none;-webkit-text-decoration-skip:none;color:#1155cc;text-decoration:underline}}.c8{{border-spacing:0;border-collapse:collapse;margin-right:auto}}.c9{{background-color:#ffffff;max-width:468pt;padding:72pt 72pt 72pt 72pt}}.c5{{color:inherit;text-decoration:inherit}}.c14{{font-weight:700}}.c6{{height:0pt}}.c12{{height:11pt}}.title{{padding-top:0pt;color:#000000;font-size:26pt;padding-bottom:3pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}.subtitle{{padding-top:0pt;color:#666666;font-size:15pt;padding-bottom:16pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}li{{color:#000000;font-size:11pt;font-family:"Arial"}}p{{margin:0;color:#000000;font-size:11pt;font-family:"Arial"}}h1{{padding-top:20pt;color:#000000;font-size:20pt;padding-bottom:6pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}h2{{padding-top:18pt;color:#000000;font-size:16pt;padding-bottom:6pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}h3{{padding-top:16pt;color:#434343;font-size:14pt;padding-bottom:4pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}h4{{padding-top:14pt;color:#666666;font-size:12pt;padding-bottom:4pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}h5{{padding-top:12pt;color:#666666;font-size:11pt;padding-bottom:4pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;orphans:2;widows:2;text-align:left}}h6{{padding-top:12pt;color:#666666;font-size:11pt;padding-bottom:4pt;font-family:"Arial";line-height:1.15;page-break-after:avoid;font-style:italic;orphans:2;widows:2;text-align:left}}</style>
            </head>
            <body class="c9 doc-content">
                <p class="c11"><span>Finished uploading all Wikimedia assets for </span><span class="c14">{self.partner.upper()}. </span><span>Click </span><span class="c15"><a class="c5" href="https://www.google.com/url?q=https://www.google.com/&amp;sa=D&amp;source=editors&amp;ust=1691999047767600&amp;usg=AOvVaw2jE6lkAYiPD6rIoFOjXsCz">here</a></span><span>&nbsp;to download the complete log file</span><span class="c3">.</span></p>
                <p class="c7"><span class="c3"></span></p>
                <hr>
                <p class="c7"><span class="c0"></span></p>
                <p class="c7"><span class="c0"></span></p>
                <a id="t.ada0f9698b83dc43aac32176437d71a8ee112aac"></a><a id="t.0"></a>
                <table class="c8">
                    <tr class="c6">
                        <td class="c2" colspan="1" rowspan="1">
                        <p class="c11"><span class="c3">DPLA records</span></p>
                        </td>
                        <td class="c13" colspan="1" rowspan="1">
                        <p class="c4 c12"><span class="c0"></span></p>
                        </td>
                        <td class="c1" colspan="1" rowspan="1">
                        <p class="c4"><span class="c0">{self.tracker.dpla_count}</span></p>
                        </td>
                    </tr>
                    <tr class="c6">
                        <td class="c2" colspan="1" rowspan="1">
                        <p class="c11"><span class="c14">Images</span></p>
                        </td>
                        <td class="c13" colspan="1" rowspan="1">
                        <p class="c7"><span class="c0"></span></p>
                        </td>
                        <td class="c1" colspan="1" rowspan="1">
                        <p class="c4 c12"><span class="c0"></span></p>
                        </td>
                    </tr>
                    <tr class="c6">
                        <td class="c2" colspan="1" rowspan="1">
                        <p class="c4 c12"><span class="c0"></span></p>
                        </td>
                        <td class="c13" colspan="1" rowspan="1">
                        <p class="c11"><span class="c0">Attempted</span></p>
                        </td>
                        <td class="c1" colspan="1" rowspan="1">
                        <p class="c4"><span class="c0">{self.tracker.attempted}</span></p>
                        </td>
                    </tr>
                    <tr class="c6">
                        <td class="c2" colspan="1" rowspan="1">
                        <p class="c4 c12"><span class="c0"></span></p>
                        </td>
                        <td class="c13" colspan="1" rowspan="1">
                        <p class="c11"><span class="c0">Uploaded</span></p>
                        </td>
                        <td class="c1" colspan="1" rowspan="1">
                        <p class="c4"><span class="c0">{self.tracker.upload_count}</span></p>
                        </td>
                    </tr>
                    <tr class="c6">
                        <td class="c2" colspan="1" rowspan="1">
                        <p class="c4 c12"><span class="c0"></span></p>
                        </td>
                        <td class="c13" colspan="1" rowspan="1">
                        <p class="c11"><span class="c0">Skipped</span></p>
                        </td>
                        <td class="c1" colspan="1" rowspan="1">
                        <p class="c4"><span class="c0">{self.tracker.skip_count}</span></p>
                        </td>
                    </tr>
                    <tr class="c6">
                        <td class="c2" colspan="1" rowspan="1">
                        <p class="c4 c12"><span class="c0"></span></p>
                        </td>
                        <td class="c13" colspan="1" rowspan="1">
                        <p class="c11"><span class="c0">Failed</span></p>
                        </td>
                        <td class="c1" colspan="1" rowspan="1">
                        <p class="c4"><span class="c0">{self.tracker.fail_count}</span></p>
                        </td>
                    </tr>
                    <tr class="c6">
                        <td class="c10" colspan="2" rowspan="1">
                        <p class="c4"><span class="c3">File Information</span></p>
                        </td>
                        <td class="c1" colspan="1" rowspan="1">
                        <p class="c4 c12"><span class="c0"></span></p>
                        </td>
                    </tr>
                    <tr class="c6">
                        <td class="c2" colspan="1" rowspan="1">
                        <p class="c4 c12"><span class="c0"></span></p>
                        </td>
                        <td class="c13" colspan="1" rowspan="1">
                        <p class="c4"><span class="c0">Size</span></p>
                        </td>
                        <td class="c1" colspan="1" rowspan="1">
                        <p class="c4"><span class="c0">{sizeof_fmt(self.tracker.cumulative_size)}</span></p>
                        </td>
                    </tr>
                </table>
                <p class="c7"><span class="c0"></span></p>
            </body>
            </html>"""

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
            - Attempted: {self.tracker.success_count + self.tracker.skip_count + self.tracker.fail_count}
            - Downloaded: {self.tracker.success_count}
            - Skipped: {self.tracker.skip_count}
            - Failed: {self.tracker.fail_count}
            ----------------------------------------
            File information
            - Downloaded: TBD
            - All records: {sizeof_fmt(self.tracker.cumulative_size)}
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
                <p class="c8"><span>Finished downloading all Wikimedia assets for </span><span class="c14">{self.partner.upper()}. </span><span>Click </span><span class="c11"><a class="c13" href="{self.log_url}">here</a></span><span>&nbsp;to download the complete log file</span><span class="c10">.</span></p>
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
                        <p class="c6"><span class="c2">{self.tracker.dpla_count}</span></p>
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
                        <p class="c6"><span class="c2">{self.tracker.attempted}</span></p>
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
                        <p class="c6"><span class="c2">{self.tracker.success_count}</span></p>
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
                        <p class="c6"><span class="c2">{self.tracker.skip_count}</span></p>
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
                        <p class="c6"><span class="c2">{self.tracker.fail_count}</span></p>
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
                        <p class="c6"><span class="c2">{sizeof_fmt(self.tracker.cumulative_size)}</span></p>
                        </td>
                    </tr>
                </table>
                <p class="c3"><span class="c2"></span></p>
            </body>
            </html>
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
