"""Module with email notifier."""

import asyncio
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from posixpath import basename
from typing import Any

from lakehouse_engine.core.definitions import TerminatorSpec
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.terminators.notifier import Notifier
from lakehouse_engine.terminators.notifiers.exceptions import (
    NotifierConfigException,
    NotifierTemplateNotFoundException,
)
from lakehouse_engine.terminators.notifiers.notification_templates import (
    NotificationsTemplates,
)
from lakehouse_engine.utils.logging_handler import LoggingHandler


class EmailNotifier(Notifier):
    """Base Notification class."""

    _logger = LoggingHandler(__name__).get_logger()

    def __init__(self, notification_spec: TerminatorSpec):
        """Construct Email Notification instance.

        Args:
            notification_spec: notification specification.
        """
        super().__init__(notification_spec)

    def create_notification(self) -> None:
        """Creates the notification to be sent."""
        if "template" in self.notification.keys():
            template: dict = NotificationsTemplates.EMAIL_NOTIFICATIONS_TEMPLATES.get(
                self.notification["template"], {}
            )

            if template:
                self.notification["message"] = self._render_notification_field(
                    template["message"]
                )
                self.notification["subject"] = self._render_notification_field(
                    template["subject"]
                )
                self.notification["mimetype"] = template["mimetype"]

            else:
                raise NotifierTemplateNotFoundException(
                    f"""Template {self.notification["template"]} does not exist"""
                )

        elif "message" in self.notification.keys():
            self.notification["message"] = self._render_notification_field(
                self.notification["message"]
            )
            self.notification["subject"] = self._render_notification_field(
                self.notification["subject"]
            )
        else:
            raise NotifierConfigException("Malformed Notification Definition")

    def send_notification(self) -> None:
        """Sends the notification by using a series of methods."""
        self._validate_email_notification()

        server = self.notification["server"]
        notification_office_email_servers = ["smtp.office365.com"]

        if (
            ExecEnv.ENGINE_CONFIG.notif_disallowed_email_servers is not None
            and server in ExecEnv.ENGINE_CONFIG.notif_disallowed_email_servers
        ):
            raise NotifierConfigException(
                f"Trying to use disallowed smtp server: '{server}'.\n"
                f"Disallowed smtp servers: "
                f"{str(ExecEnv.ENGINE_CONFIG.notif_disallowed_email_servers)}"
            )
        elif server in notification_office_email_servers:
            self._authenticate_and_send_office365()
        else:
            self._authenticate_and_send_simple_smtp()

    def _authenticate_and_send_office365(self) -> None:
        """Authenticates and sends an email notification using Graph API."""
        from azure.identity.aio import ClientSecretCredential
        from msgraph import GraphServiceClient

        self._logger.info("Attempting authentication using Graph API.")

        request_body = self._create_graph_api_email_body()

        self._logger.info(f"Sending notification email with body: {request_body}")

        credential = ClientSecretCredential(
            tenant_id=self.notification["tenant_id"],
            client_id=self.notification["user"],
            client_secret=self.notification["password"],
        )
        client = GraphServiceClient(credentials=credential)

        import nest_asyncio

        nest_asyncio.apply()
        asyncio.get_event_loop().run_until_complete(
            client.users.by_user_id(self.notification["from"]).send_mail.post(
                body=request_body
            )
        )

        self._logger.info("Notification email sent successfully.")

    def _authenticate_and_send_simple_smtp(self) -> None:
        """Authenticates and sends an email notification using simple authentication."""
        with smtplib.SMTP(
            self.notification["server"], self.notification["port"]
        ) as smtp:
            try:
                smtp.starttls()
                smtp.login(
                    self.notification.get("user", ""),
                    self.notification.get("password", ""),
                )
            except smtplib.SMTPException as e:
                self._logger.exception(
                    f"Exception while authenticating to smtp: {str(e)}"
                )
                self._logger.exception(
                    "Attempting to send the notification without authentication"
                )

            mesg = MIMEMultipart()
            mesg["From"] = self.notification["from"]

            to = self.notification.get("to", [])
            cc = self.notification.get("cc", [])
            bcc = self.notification.get("bcc", [])

            mesg["To"] = ", ".join(to)
            mesg["CC"] = ", ".join(cc)
            mesg["BCC"] = ", ".join(bcc)
            mesg["Subject"] = self.notification["subject"]
            mesg["Importance"] = self._get_importance(
                self.notification.get("importance", "normal")
            )

            match self.notification.get("mimetype", "plain"):
                case "html" | "text/html":
                    mimetype = "html"
                case "text" | "text/plain" | "plain" | "text/text":
                    mimetype = "text"
                case _:
                    self._logger.warning(
                        f"""Unknown mimetype '{self.notification["mimetype"]}' """
                        f"provided. Defaulting to 'plain'."
                    )
                    mimetype = "text"

            body = MIMEText(self.notification["message"], mimetype)
            mesg.attach(body)

            for f in self.notification.get("attachments", []):
                with open(f, "rb") as fil:
                    part = MIMEApplication(fil.read(), Name=basename(f))
                part["Content-Disposition"] = 'attachment; filename="%s"' % basename(f)
                mesg.attach(part)

            try:
                smtp.sendmail(
                    self.notification["from"], to + cc + bcc, mesg.as_string()
                )
                self._logger.info("Email sent successfully.")
            except smtplib.SMTPException as e:
                self._logger.exception(f"Exception while sending email: {str(e)}")

    def _validate_email_notification(self) -> None:
        """Validates the email notification."""
        if not self.notification.get("from"):
            raise NotifierConfigException(
                "Email notification must contain 'from' field."
            )
        if not self.notification.get("server"):
            raise NotifierConfigException(
                "Email notification must contain 'server' field."
            )
        if not self.notification.get("port"):
            raise NotifierConfigException(
                "Email notification must contain 'port' field."
            )
        if (
            not self.notification.get("to")
            and not self.notification.get("cc")
            and not self.notification.get("bcc")
        ):
            raise NotifierConfigException(
                "No recipients provided. Please provide at least one recipient."
            )

    def _get_importance(self, importance: str) -> Any:
        """Get the importance of the email notification.

        Args:
            importance: Importance level of the email.

        Returns:
            Importance level for the email notification.
        """
        from msgraph.generated.models.importance import Importance

        match importance:
            case "critical" | "high":
                return Importance.High
            case "normal":
                return Importance.Normal
            case "low":
                return Importance.Low
            case _:
                self._logger.warning(
                    f"""Unknown importance '{importance}' provided. """
                    f"Defaulting to 'normal'."
                )
                return Importance.Normal

    def _create_graph_api_email_body(self) -> Any:
        """Create the email body for the Graph API.

        Returns:
            Email body for the Graph API.
        """
        from msgraph.generated.models.body_type import BodyType
        from msgraph.generated.models.file_attachment import FileAttachment
        from msgraph.generated.models.item_body import ItemBody
        from msgraph.generated.models.message import Message
        from msgraph.generated.users.item.send_mail.send_mail_post_request_body import (
            SendMailPostRequestBody,
        )

        request_body = SendMailPostRequestBody()
        message = Message()
        message.subject = self.notification["subject"]

        message_body = ItemBody()

        message_body.content = self.notification["message"]
        match self.notification.get("mimetype", "plain"):
            case "html" | "text/html":
                message_body.content_type = BodyType.Html
            case "text" | "text/plain" | "plain" | "text/text":
                message_body.content_type = BodyType.Text
            case _:
                self._logger.warning(
                    f"""Unknown mimetype '{self.notification["mimetype"]}' """
                    f"provided. Defaulting to 'text'."
                )
                message_body.content_type = BodyType.Text

        message.body = message_body

        attachments = []
        for attachment_file in self.notification.get("attachments", []):
            attachment_name = attachment_file.split("/")[-1]

            with open(attachment_file, "rb") as f:
                content = f.read()

            attachment = FileAttachment()
            attachment.name = attachment_name
            attachment.size = len(content)
            attachment.content_bytes = content

            attachments.append(attachment)

        message.attachments = attachments  # type: ignore

        message.to_recipients = self._set_graph_api_recipients("to")
        message.cc_recipients = self._set_graph_api_recipients("cc")
        message.bcc_recipients = self._set_graph_api_recipients("bcc")

        message.importance = self._get_importance(
            self.notification.get("importance", "normal")
        )

        request_body.message = message
        request_body.save_to_sent_items = False

        return request_body

    def _set_graph_api_recipients(self, recipient_type: str) -> list:
        """Set the recipients for the Graph API.

        Args:
            recipient_type: Type of recipient (to, cc or bcc).

        Returns:
            List of recipients for the Graph API.
        """
        from msgraph.generated.models.email_address import EmailAddress
        from msgraph.generated.models.recipient import Recipient

        recipients = []
        for email in self.notification.get(recipient_type, []):
            recipient = Recipient()
            recipient_address = EmailAddress()
            recipient_address.address = email
            recipient.email_address = recipient_address

            recipients.append(recipient)
        return recipients
