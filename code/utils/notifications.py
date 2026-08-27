"""
Email alert and MS Teams notification utilities.
"""

import logging
import os
from typing import Dict, List, Optional

import boto3
import requests
from botocore.exceptions import ClientError
from smartsheet_dataframe import get_sheet_as_df

from utils.aws import get_messenger_credentails, get_messanger_credentails

logger = logging.getLogger(__name__)


def clean_investigator_names(investigators: List[str]) -> str:
    """
    Formats a list of investigator names into a grammatically correct string.

    Examples
    --------
    ["Alice"] → "Alice"
    ["Alice", "Bob"] → "Alice and Bob"
    ["Alice", "Bob", "Carol"] → "Alice, Bob, and Carol"
    """
    if len(investigators) > 1:
        invest = ", ".join(investigators)
        idx = invest.rfind(",")
        invest = invest[:idx] + " and" + invest[idx + 1 :]
    else:
        invest = investigators[0]
    return invest


class AlertBot:
    """Sends alerts to an MS Teams webhook channel."""

    def __init__(self, url: Optional[str] = None):
        self.url = url

    @staticmethod
    def _create_body_text(message: str, extra_text: Optional[str]) -> dict:
        body: list = [
            {"type": "TextBlock", "size": "Medium", "weight": "Bolder", "text": message}
        ]
        if extra_text is not None:
            body.append({"type": "TextBlock", "text": extra_text})
        contents = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "type": "AdaptiveCard",
                        "body": body,
                        "$schema": (
                            "http://adaptivecards.io/schemas/adaptive-card.json"
                        ),
                        "version": "1.0",
                    },
                }
            ],
        }
        return contents

    def send_message(
        self, message: str, extra_text: Optional[str] = None
    ) -> Optional[requests.Response]:
        """
        Sends a message. If url is None, prints instead of posting.
        """
        if self.url is None:
            logger.info(f"Teams alert (webhook unset, not posted): {message}")
            return None
        else:
            contents = self._create_body_text(message, extra_text)
            response = requests.post(self.url, json=contents)
            return response


def send_alerts(
    mode: str,
    investigators: str,
    dataset: str,
):
    """
    Sends an SES email alert (legacy function).
    Reads SES_TOKEN_PATH, SMARTSHEET_ID, and SOURCE_EMAIL from environment.
    Silently skips if any variable is unset.
    """
    ses_token_path = os.getenv("SES_TOKEN_PATH")
    smartsheet_id = os.getenv("SMARTSHEET_ID")
    source_email = os.getenv("SOURCE_EMAIL")

    if not ses_token_path or not smartsheet_id or not source_email:
        logger.warning(
            "SES_TOKEN_PATH, SMARTSHEET_ID, or SOURCE_EMAIL not set; skipping send_alerts."
        )
        return None

    smartsheet_token = get_messenger_credentails(ses_token_path)
    ses_client = boto3.client("ses", region_name="us-west-2")

    email_df = get_sheet_as_df(
        token=smartsheet_token,
        sheet_id=int(smartsheet_id),
    )

    email_addresses = email_df.loc[
        email_df["Name"].isin(investigators), "Email"
    ].values.tolist()

    if len(investigators) > 1:
        invest = ", ".join(investigators)
        idx = invest.rfind(",")
        invest = invest[:idx] + " and" + invest[idx + 1 :]
    else:
        invest = investigators[0]

    if "dispatch" in mode:
        message_data = (
            f"Hi {invest},<br><br>This messsage is to inform you "
            f"that your dataset {dataset} has been uploaded to AWS and "
            "stitched images are now available for viewing.<br><br>"
            "Sincerely,<br>SmartSPIM Processing Team"
        )
        subject_data = f"Stitched images available for dataset {dataset}"

    elif "clean" in mode:
        message_data = (
            f"Hi {invest},<br><br>This messsage is to inform you "
            f"that your dataset {dataset} has completed the SmartSPIM "
            "pipeline. Segmented and registered images have been quantified "
            "and are now available for viewing.<br><br>"
            "Sincerely,<br>SmartSPIM Processing Team"
        )
        subject_data = f"SmartSPIM processing completed for dataset {dataset}"

    response = ses_client.send_email(
        Destination={"ToAddresses": email_addresses},
        Message={
            "Body": {"Html": {"Charset": "UTF-8", "Data": message_data}},
            "Subject": {"Charset": "UTF-8", "Data": subject_data},
        },
        Source=source_email,
    )

    return response


def send_ses_alerts(
    mode: str,
    alert_configs: dict,
    investigators: str,
    dataset: str,
    email_message_params: dict = {},
    source_email: str = None,
):
    """
    Sends an SES email alert to investigators based on the pipeline mode.
    Reads SOURCE_EMAIL from environment when source_email parameter is None.
    Silently skips if required configuration is absent.
    """
    source_email = source_email or os.getenv("SOURCE_EMAIL") or alert_configs.get("source_email")
    ses_token_path = os.getenv("SES_TOKEN_PATH") or alert_configs.get("ses_token_path")
    smartsheet_id_raw = os.getenv("SMARTSHEET_ID") or alert_configs.get("smartsheet_id")

    if not source_email or not ses_token_path or not smartsheet_id_raw:
        logger.warning(
            "SOURCE_EMAIL, SES_TOKEN_PATH, or SMARTSHEET_ID not set; skipping send_ses_alerts."
        )
        return None

    smartsheet_token_data = get_messanger_credentails(ses_token_path)
    if not smartsheet_token_data:
        logger.warning("Could not retrieve Smartsheet token from Secrets Manager.")
        return None

    smartsheet_token = smartsheet_token_data["token"]
    response = None

    if smartsheet_token:
        ses_client = boto3.client("ses", region_name="us-west-2")

        try:
            email_df = get_sheet_as_df(
                token=smartsheet_token,
                sheet_id=int(smartsheet_id_raw),
            )
        except Exception:
            logger.error("Not able to get the investigators smartsheet", exc_info=True)
            return

        email_addresses = email_df.loc[
            email_df["Name"].isin(investigators), "Email"
        ].values.tolist()

        if not len(email_addresses):
            logger.warning(f"No email addresses were found for investigators: {investigators}")
            return response

        invest = clean_investigator_names(investigators)
        aind_image_logo = (
            "https://allenneuraldynamics.github.io/assets/img/AIND_logo.png"
        )

        if "split_channels" in mode:
            message_data = f"""
                <html>
                <body>
                    <h3>Hello {invest},</h3>
                    <p>This is an email to inform you that your dataset <i>{dataset}</i> finished uploading and is being processed.</p>
                    <p>Sincerely,<br><b>SmartSPIM Processing Team.</b><p>
                    <img src="{aind_image_logo}" alt="Embedded Image" style="width:300px; height:auto;">
                </body>
                </html>
            """
            subject_data = f"SmartSPIM Notification - Pipeline Started - {dataset}"

        elif "dispatch" in mode:
            ng_link_path = email_message_params.get("ng_link_path")
            ng_link_path = (
                ng_link_path
                if ng_link_path
                else "Please, look at the dashboard or communicate with the pipeline administrator."
            )
            message_data = f"""
                <html>
                <body>
                    <h3>Hello {invest},</h3>
                    <p>This is an email to inform you that your dataset <i>{dataset}</i> is ready for visualization.</p>
                    <p>Please, copy and paste this link in your browser: <i><u>{ng_link_path}</u></i></p>
                    <p>Sincerely,<br><b>SmartSPIM Processing Team.</b><p>
                    <p style="font-size: smaller; color: gray;"><b>Note:</b> If you requested segmentation, you will be receiving another email in a day or two. Thanks for your patience.</p>
                    <img src="{aind_image_logo}" alt="Embedded Image" style="width:300px; height:auto;">
                </body>
                </html>
            """
            subject_data = f"SmartSPIM Notification - Stitched Images - {dataset}"

        elif "clean" in mode:
            message_data = f"""
                <html>
                <body>
                    <h3>Hello {invest},</h3>
                    <p>This is an email to inform you that your dataset <i>{dataset}</i> finished cell detection and quantification.</p>
                    <p>Please, check the SmartSPIM dashboard.</p>
                    <p>Sincerely,<br><b>SmartSPIM Processing Team.</b><p>
                    <img src="{aind_image_logo}" alt="Embedded Image" style="width:300px; height:auto;">
                </body>
                </html>
            """
            subject_data = f"SmartSPIM Notification - Pipeline Completed - {dataset}"

        else:
            logger.warning(f"Email alerts are not implemented for mode {mode}")
            return response

        try:
            response = ses_client.send_email(
                Destination={"ToAddresses": email_addresses},
                Message={
                    "Body": {"Html": {"Charset": "UTF-8", "Data": message_data}},
                    "Subject": {"Charset": "UTF-8", "Data": subject_data},
                },
                Source=source_email,
            )
        except ClientError as e:
            logger.error(f"SES send_email failed: {e.response['Error']['Message']}")

    else:
        logger.error("Problem retrieving the SES token from the secret manager")

    return response


def send_email_alerts(
    mode: str,
    alert_configs: dict,
    investigators: list,
    dataset_name: str,
    logger: logging.Logger,
    email_message_params: Dict = {},
    source_email: str = None,
):
    """
    Checks if there is investigator info and sends email.

    Parameters
    ----------
    mode : str
        The current mode of the dispatcher
    alert_configs : dict
        Information on accessing investigator email list
    investigators : list
        investigators that submitted the dataset
    dataset_name : str
        current dataset being processed
    logger : logging.Logger
        logger object
    source_email: str
        Source email. Reads SOURCE_EMAIL env var when None.
        Alerts are skipped when neither is provided.
    """
    if len(investigators) and len(investigators[0]):
        investigators = [
            inv["name"] if isinstance(inv, dict) else inv for inv in investigators
        ]
        response = send_ses_alerts(
            mode=mode,
            alert_configs=alert_configs,
            investigators=investigators,
            dataset=dataset_name,
            email_message_params=email_message_params,
            source_email=source_email,
        )
        logger.info(
            "Email alert sent",
            extra={
                "investigators": investigators,
                "status_code": getattr(response, "status_code", None),
            },
        )
    else:
        logger.info("Email not sent: No investigators were provided")
