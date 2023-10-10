"""Module with email notifier."""
import asyncio
import smtplib
from copy import copy
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from azure.identity.aio import ClientSecretCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.email_address import EmailAddress
from msgraph.generated.models.item_body import ItemBody
from msgraph.generated.models.message import Message
from msgraph.generated.models.recipient import Recipient
from msgraph.generated.users.item.send_mail.send_mail_post_request_body import (
    SendMailPostRequestBody,
)

from lakehouse_engine.core.definitions import (
    NOTIFICATION_DISALLOWED_EMAIL_SERVERS,
    NOTIFICATION_OFFICE_EMAIL_SERVERS,
    TerminatorSpec,
)
from lakehouse_engine.terminators.notifier import Notifier
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
        given_args = self.notification.get("args", {})

        if "template" in self.notification.keys():
            template: dict = NotificationsTemplates.EMAIL_NOTIFICATIONS_TEMPLATES.get(
                self.notification["template"], {}
            )

            if template:
                template_args = template["args"]

                Notifier._check_args_are_correct(given_args, copy(template_args))

                self.notification["message"] = self._render_notification_field(
                    template["message"], given_args
                )
                self.notification["subject"] = self._render_notification_field(
                    template["subject"], given_args
                )
                self.notification["mimetype"] = template["mimetype"]

            else:
                raise ValueError(
                    f"""Template {self.notification["template"]} does not exist"""
                )

        elif "message" in self.notification.keys():
            if given_args:
                self.notification["message"] = self._render_notification_field(
                    self.notification["message"], given_args
                )
                self.notification["subject"] = self._render_notification_field(
                    self.notification["subject"], given_args
                )
        else:
            raise ValueError("Malformed Notification Definition")

    def send_notification(self) -> None:
        """Sends the notification by using a series of methods."""
        server = self.notification["server"]

        if server in NOTIFICATION_DISALLOWED_EMAIL_SERVERS:
            raise ValueError(
                f"Trying to use disallowed smtp server: '{server}'.\n"
                f"Disallowed smtp servers: {str(NOTIFICATION_DISALLOWED_EMAIL_SERVERS)}"
            )
        elif server in NOTIFICATION_OFFICE_EMAIL_SERVERS:
            self._authenticate_and_send_office365()
        else:
            self._authenticate_and_send_simple_smtp()

    def _authenticate_and_send_office365(self) -> None:
        """Authenticates and sends an email notification using Graph API."""
        self._logger.info("Attempting authentication using Graph API.")

        credential = ClientSecretCredential(
            tenant_id=self.notification["tenant_id"],
            client_id=self.notification["user"],
            client_secret=self.notification["password"],
        )
        client = GraphServiceClient(credentials=credential)

        request_body = SendMailPostRequestBody()
        message = Message()
        message.subject = self.notification["subject"]

        message_body = ItemBody()

        message_body.content = self.notification["message"]

        message.body = message_body

        recipients = []
        for email in self.notification["to"]:
            recipient = Recipient()
            recipient_address = EmailAddress()
            recipient_address.address = email
            recipient.email_address = recipient_address

            recipients.append(recipient)

        message.to_recipients = recipients

        request_body.message = message
        request_body.save_to_sent_items = False

        self._logger.info(f"Sending notification email with body: {request_body}")

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
            mesg["To"] = ", ".join(self.notification["to"])
            mesg["Subject"] = self.notification["subject"]

            body = MIMEText(self.notification["message"], self.notification["mimetype"])
            mesg.attach(body)
            try:
                smtp.sendmail(
                    self.notification["from"], self.notification["to"], mesg.as_string()
                )
                self._logger.info("Email sent successfully.")
            except smtplib.SMTPException as e:
                self._logger.exception(f"Exception while sending email: {str(e)}")
