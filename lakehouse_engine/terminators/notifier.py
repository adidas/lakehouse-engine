"""Module with notification terminator."""
from abc import ABC, abstractmethod

from jinja2 import Template

from lakehouse_engine.core.definitions import (
    NOTIFICATION_RUNTIME_PARAMETERS,
    NotificationRuntimeParameters,
    TerminatorSpec,
)
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

    @staticmethod
    def _check_args_are_correct(given_args: dict, template_args: list) -> None:
        """Checking if all arguments in template were set.

        Args:
            given_args: args set in the terminator spec.
            template_args: args needed to be set in the template.
        """
        extra_args = []

        for key in given_args.keys():
            if key in template_args:
                template_args.remove(key)
            else:
                extra_args.append(key)

        if set(template_args) - set(NOTIFICATION_RUNTIME_PARAMETERS):
            raise ValueError(
                "The following template args have not been set: "
                + ", ".join(template_args)
            )

        if extra_args:
            Notifier._logger.info(
                "Extra parameters sent to template: " + ", ".join(extra_args)
            )

    @staticmethod
    def _render_notification_field(template_field: str, args: dict) -> str:
        """Render the notification given args.

        Args:
            template_field: Message with templates to be replaced.
            args: key/value pairs to be replaced in the message.

        Returns:
            Rendered field
        """
        field_template = Template(template_field)
        if (
            NotificationRuntimeParameters.DATABRICKS_JOB_NAME.value in template_field
            or NotificationRuntimeParameters.DATABRICKS_WORKSPACE_ID.value
            in template_field
        ):
            workspace_id, job_name = DatabricksUtils.get_databricks_job_information()
            args["databricks_job_name"] = job_name
            args["databricks_workspace_id"] = workspace_id

        return field_template.render(args)

    @staticmethod
    def check_if_notification_is_failure_notification(spec: TerminatorSpec) -> bool:
        """Check if given notification is a failure notification.

        Args:
            spec: spec to validate if it is a failure notification.

        Returns:
            A boolean telling if the notification is a failure notification
        """
        notification = spec.args

        if "template" in notification.keys():
            template: dict = NotificationsTemplates.EMAIL_NOTIFICATIONS_TEMPLATES.get(
                notification["template"], {}
            )

            if template:
                return notification.get("on_failure", True)
            else:
                raise ValueError(f"""Template {notification["template"]} not found.""")
        else:
            return notification.get("on_failure", True)
