"""Email notification templates."""


class NotificationsTemplates(object):
    """Templates for notifications."""

    EMAIL_NOTIFICATIONS_TEMPLATES = {
        "failure_notification_email": {
            "subject": "Service Failure",
            "mimetype": "text/text",
            "message": """
            Job {{ databricks_job_name }} in workspace {{ databricks_workspace_id }} has
            failed with the exception: {{ exception }}""",
            "on_failure": True,
        },
    }
