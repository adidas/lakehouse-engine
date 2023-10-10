"""Email notification templates."""


class NotificationsTemplates(object):
    """Templates for notifications."""

    EMAIL_NOTIFICATIONS_TEMPLATES = {
        "failure_notification_email": {
            "subject": "Service Failure",
            "mimetype": "plain",
            "message": """
            Job {{ databricks_job_name }} in workspace {{ databricks_workspace_id }} has
            failed with the exception: {{ exception }}""",
            "args": [
                "exception",
            ],
            "on_failure": True,
        },
        "opsgenie_notification": {
            "subject": "{{ data_product }} - Failure Notification",
            "mimetype": "plain",
            "message": """
                Opsgenie notification:
                    - Data Product: {{ data_product }}
                    - Job name: {{ databricks_job_name }}
                    - Alias: {{ alias }}
                    - Priority: {{ priority }}
                    - Entity: {{ entity }}
                    - Tags: {{ tags }}
                    - Action: {{ action }}
                    - Description: {{ description }}
                    - Exception: {{ exception }}""",
            "args": [
                "data_product",
                "alias",
                "priority",
                "entity",
                "tags",
                "action",
                "description",
                "exception",
            ],
            "on_failure": True,
        },
    }
