"""Module for notifier factory."""

from lakehouse_engine.core.definitions import NotifierType, TerminatorSpec
from lakehouse_engine.terminators.notifier import Notifier
from lakehouse_engine.terminators.notifiers.email_notifier import EmailNotifier
from lakehouse_engine.terminators.notifiers.exceptions import NotifierNotFoundException


class NotifierFactory(object):
    """Class for notification factory."""

    NOTIFIER_TYPES = {NotifierType.EMAIL.value: EmailNotifier}

    @classmethod
    def get_notifier(cls, spec: TerminatorSpec) -> Notifier:
        """Get a notifier according to the terminator specs using a factory.

        Args:
            spec: terminator specification.

        Returns:
            Notifier: notifier that will handle notifications.
        """
        notifier_name = spec.args.get("type")
        notifier = cls.NOTIFIER_TYPES.get(notifier_name)

        if notifier:
            return notifier(notification_spec=spec)
        else:
            raise NotifierNotFoundException(
                f"The requested notification format {notifier_name} is not supported."
            )

    @staticmethod
    def generate_failure_notification(spec: list, exception: Exception) -> None:
        """Check if it is necessary to send a failure notification and generate it.

        Args:
            spec: List of termination specs
            exception: Exception that caused the failure.
        """
        notification_specs = []

        for terminator in spec:
            if terminator.function == "notify":
                notification_specs.append(terminator)

        for notification in notification_specs:
            failure_notification_spec = notification.args
            generate_failure_notification = failure_notification_spec.get(
                "generate_failure_notification", False
            )

            if generate_failure_notification or (
                Notifier.check_if_notification_is_failure_notification(notification)
            ):
                failure_notification_spec["exception"] = str(exception)

                if generate_failure_notification:
                    failure_notification_spec["template"] = (
                        f"""failure_notification_{failure_notification_spec["type"]}"""
                    )

                failure_spec = TerminatorSpec(
                    function="notification", args=failure_notification_spec
                )

                notifier = NotifierFactory.get_notifier(failure_spec)
                notifier.create_notification()
                notifier.send_notification()
