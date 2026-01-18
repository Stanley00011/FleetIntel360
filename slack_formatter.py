def format_alert(
    title: str,
    rows,
    severity_column: str,
    entity_column: str,
    max_rows: int = 5
):
    emojis = {
        "CRITICAL": "🚨",
        "WARNING": "⚠️",
        "INFO": "ℹ️"
    }

    total = len(rows)
    rows = rows.head(max_rows)

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": title
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{total} alert(s) detected*"
            }
        },
        {"type": "divider"}
    ]

    for _, row in rows.iterrows():
        severity = row[severity_column]
        emoji = emojis.get(severity, "ℹ️")

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{emoji} *{severity}*\n"
                    f"*Entity:* `{row[entity_column]}`\n"
                    f"*Metric:* `{row['metric_name']}` = `{row['metric_value']}`\n"
                    f"*Info:* {row['description']}"
                )
            }
        })

    if total > max_rows:
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"_Showing {max_rows} of {total}. See dashboard for full details._"
                }
            ]
        })

    blocks.append({
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "View Dashboard"},
                "url": "http://localhost:8501/Data_Quality", 
                "style": "primary"
            }
        ]
    })

    return {
        "text": title,
        "blocks": blocks
    }
