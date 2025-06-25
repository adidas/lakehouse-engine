"""Module with notification terminator."""

from abc import ABC, abstractmethod

from jinja2 import Template

from lakehouse_engine.core.definitions import (
    NotificationRuntimeParameters,
    TerminatorSpec,
)
from lakehouse_engine.core.exec_env import ExecEnv
from lakehouse_engine.terminators.notifiers.notification_templates import (
    NotificationsTemplates,
)
from lakehouse_engine.utils.databricks_utils import DatabricksUtils
from lakehouse_engine.utils.logging_handler import LoggingHandler


class Notifier(ABC):
    """Abstract Notification class."""

    _logger = LoggingHandler(__name__).get_logger()

    def __init__(self, notification_spec: TerminatorSpec):
        """Construct Notification instances.

        Args:
            notification_spec: notification specification.
        """
        self.type = notification_spec.args.get("type")
        self.notification = notification_spec.args

    @abstractmethod
    def create_notification(self) -> None:
        """Abstract create notification method."""
        raise NotImplementedError

    @abstractmethod
    def send_notification(self) -> None:
        """Abstract send notification method."""
        raise NotImplementedError

    def _render_notification_field(self, template_field: str) -> str:
        """Render the notification given args.

        Args:
            template_field: Message with templates to be replaced.

        Returns:
            Rendered field
        """
        args = {}
        field_template = Template(template_field)
        if (
            NotificationRuntimeParameters.DATABRICKS_JOB_NAME.value in template_field
            or NotificationRuntimeParameters.DATABRICKS_WORKSPACE_ID.value
            in template_field
            or NotificationRuntimeParameters.JOB_EXCEPTION.value in template_field
        ):
            workspace_id, job_name = DatabricksUtils.get_databricks_job_information(
                ExecEnv.SESSION
            )
            args["databricks_job_name"] = job_name
            args["databricks_workspace_id"] = workspace_id
            args["exception"] = self.notification.get("exception")

        return field_template.render(args)

    @staticmethod
    def check_if_notification_is_failure_notification(
        spec: TerminatorSpec,
    ) -> bool:
        """Check if given notification is a failure notification.

        Args:
            spec: spec to validate if it is a failure notification.

        Returns:
            A boolean telling if the notification is a failure notification
        """
        notification = spec.args
        is_notification_failure_notification: bool = False

        if "template" in notification.keys():
            template: dict = NotificationsTemplates.EMAIL_NOTIFICATIONS_TEMPLATES.get(
                notification["template"], {}
            )

            if template:
                is_notification_failure_notification = notification.get(
                    "on_failure", True
                )
            else:
                raise ValueError(f"""Template {notification["template"]} not found.""")
        else:
            is_notification_failure_notification = notification.get("on_failure", True)

        return is_notification_failure_notification
