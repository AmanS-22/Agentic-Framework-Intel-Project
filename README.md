# Agentic Framework — Intel Project

Modular Python-based agentic framework to orchestrate intelligent agents, tasks, and workflows.
Built with FastAPI and an extensible architecture, with optional Docker infrastructure for Kafka and Airflow integrations.

## Quick start

1. Install Python dependencies:
```powershell
pip install -r requirements.txt
```

2. Start infrastructure services (optional — required for Kafka/Airflow integrations):
Make sure Docker Desktop is running.

```powershell
docker-compose up -d
```

3. Run the API server:
```powershell
# Recommended: run as a module to avoid relative import issues
python -m api.main

# Or run with Uvicorn for development (hot reload)
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

The server will start on http://localhost:8000 by default.

## Usage

- Open the interactive API docs at: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- Health check: `GET /health`
- Root/status: `GET /`

Define workflows using JSON files in the `workflows/` folder and execute them via the orchestrator (`framework/core/orchestrator.py`). Extend the framework by adding agents, integrations, or workflow schemas.

## API Endpoints (examples)

- `GET /` — Root endpoint (API status)
- `GET /health` — Health check
- `/docs` — Swagger UI
- `/redoc` — ReDoc documentation

## Development notes

- Run the app with `python -m api.main` to avoid relative import issues.
- Use `uvicorn api.main:app --reload` for a development server with hot reload.
- Docker services are optional for basic usage but required for Kafka/Airflow-based extensions.
- Tests: run `pytest` or `python -m pytest` if tests are present.

## Extensibility

The framework is designed to support:
- Custom agents (add under `framework/agents`)
- Workflow-driven execution and validation (`framework/schemas`)
- Event-based integrations (Kafka, Airflow)
- Human-in-the-loop task management (see `framework/integration/human_task_manager.py`)

## Contributing

- Follow existing code style and project organization.
- Add tests under `tests/` for new features.
- Update API docs and workflow examples when adding endpoints or schema changes.
- Open issues or PRs describing changes and test coverage.

