# framework/schemas/validation.py
import json
import jsonschema
from typing import Dict, Any

WORKFLOW_SCHEMA = {
    "type": "object",
    "properties": {
        "workflow_id": {"type": "string"},
        "name": {"type": "string"},
        "description": {"type": "string"},
        "version": {"type": "string"},
        "tasks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                    "name": {"type": "string"},
                    "type": {"type": "string"},
                    "config": {"type": "object"}
                },
                "required": ["task_id", "name", "type"]
            }
        }
    },
    "required": ["workflow_id", "name", "tasks"]
}

def validate_workflow(workflow_definition: Dict[str, Any]):
    """Validate workflow definition against schema"""
    try:
        jsonschema.validate(instance=workflow_definition, schema=WORKFLOW_SCHEMA)
    except jsonschema.ValidationError as e:
        raise ValueError(f"Workflow validation failed: {e.message}")