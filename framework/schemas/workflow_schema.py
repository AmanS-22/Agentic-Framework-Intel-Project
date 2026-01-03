# framework/schemas/workflow_schema.py
"""
JSON Schema for defining agentic workflows
"""
WORKFLOW_SCHEMA = {
    "type": "object",
    "properties": {
        "workflow_id": {"type": "string"},
        "name": {"type": "string"},
        "description": {"type": "string"},
        "version": {"type": "string"},
        "triggers": {
            "type": "object",
            "properties": {
                "scheduled": {"type": "string"},
                "webhook": {"type": "string"},
                "manual": {"type": "boolean"}
            }
        },
        "tasks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                    "name": {"type": "string"},
                    "type": {"type": "string"},
                    "agent_type": {"type": "string"},
                    "config": {"type": "object"},
                    "dependencies": {"type": "array", "items": {"type": "string"}},
                    "human_approval_required": {"type": "boolean"},
                    "timeout_seconds": {"type": "integer"},
                    "retry_policy": {"type": "object"}
                },
                "required": ["task_id", "name", "type"]
            }
        },
        "context_schema": {"type": "object"},
        "output_schema": {"type": "object"}
    },
    "required": ["workflow_id", "name", "tasks"]
}