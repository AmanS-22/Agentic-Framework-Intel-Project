"""
Main orchestrator coordinating the entire workflow
"""
import json
import logging
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from enum import Enum
import asyncio
from dataclasses import dataclass, field
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class TaskType(Enum):
    """Types of tasks in the workflow"""
    AI_AGENT = "ai_agent"
    HUMAN_TASK = "human_task"
    SERVICE_CALL = "service_call"
    DECISION_GATE = "decision_gate"
    PARALLEL_TASK = "parallel_task"
    WEBHOOK = "webhook"
    DATABASE_QUERY = "database_query"


class TaskStatus(Enum):
    """Status of a task"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    WAITING_FOR_HUMAN = "waiting_for_human"
    HUMAN_COMPLETED = "human_completed"
    CANCELLED = "cancelled"


@dataclass
class TaskResult:
    """Result of a task execution"""
    task_id: str
    status: TaskStatus
    output: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    execution_time: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class WorkflowContext:
    """Shared context for workflow execution"""
    workflow_id: str
    execution_id: str
    start_time: datetime
    context_data: Dict[str, Any] = field(default_factory=dict)
    task_results: Dict[str, TaskResult] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def update_context(self, key: str, value: Any):
        """Update context data"""
        self.context_data[key] = value
    
    def get_context(self, key: str, default: Any = None) -> Any:
        """Get value from context"""
        return self.context_data.get(key, default)


class Orchestrator:
    """Main orchestrator for agentic workflows"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.workflows: Dict[str, Dict] = {}
        self.running_workflows: Dict[str, WorkflowContext] = {}
        self.agents = {}
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.lock = threading.RLock()
        
        # Initialize integrations
        from framework.integration.kafka_broker import KafkaMessageBroker
        from framework.integration.human_task_manager import HumanTaskManager
        
        self.message_broker = KafkaMessageBroker(
            bootstrap_servers=config.get('kafka_servers', 'localhost:9092')
        )
        self.human_task_manager = HumanTaskManager(config)
        
        logger.info("Orchestrator initialized")
    
    def register_workflow(self, workflow_definition: Dict[str, Any]) -> str:
        """Register a new workflow definition"""
        from framework.schemas.validation import validate_workflow
        
        # Validate workflow schema
        validate_workflow(workflow_definition)
        
        workflow_id = workflow_definition['workflow_id']
        self.workflows[workflow_id] = workflow_definition
        
        # Generate Airflow DAG
        from framework.integration.airflow_dag_builder import AirflowDAGBuilder
        dag_builder = AirflowDAGBuilder(workflow_definition)
        dag = dag_builder.build_dag()
        
        # Save DAG to Airflow DAGs folder
        self._save_dag_to_airflow(dag, workflow_id)
        
        logger.info(f"Workflow {workflow_id} registered successfully")
        return workflow_id
    
    def execute_workflow(self, workflow_id: str, 
                        trigger_data: Dict[str, Any] = None) -> str:
        """Execute a workflow"""
        if workflow_id not in self.workflows:
            raise ValueError(f"Workflow {workflow_id} not found")
        
        workflow_def = self.workflows[workflow_id]
        execution_id = f"{workflow_id}_{uuid.uuid4().hex[:8]}"
        
        # Create workflow context
        context = WorkflowContext(
            workflow_id=workflow_id,
            execution_id=execution_id,
            start_time=datetime.now(),
            context_data=trigger_data or {}
        )
        
        # Store context
        with self.lock:
            self.running_workflows[execution_id] = context
        
        # Start execution in background
        self.executor.submit(self._execute_workflow_async, workflow_def, context)
        
        # Publish workflow started event
        self.message_broker.publish_event(
            topic="workflow_events",
            event_type="WORKFLOW_STARTED",
            data={
                "workflow_id": workflow_id,
                "execution_id": execution_id,
                "trigger_data": trigger_data
            },
            workflow_id=workflow_id,
            task_id="orchestrator"
        )
        
        return execution_id
    
    def _execute_workflow_async(self, workflow_def: Dict, context: WorkflowContext):
        """Execute workflow asynchronously"""
        try:
            tasks = workflow_def['tasks']
            task_graph = self._build_task_graph(tasks)
            
            # Execute tasks in topological order
            for task_level in task_graph:
                # Execute parallel tasks at this level
                parallel_results = []
                for task_def in task_level:
                    future = self.executor.submit(
                        self._execute_task,
                        task_def,
                        context
                    )
                    parallel_results.append((task_def['task_id'], future))
                
                # Wait for all tasks at this level to complete
                for task_id, future in parallel_results:
                    try:
                        result = future.result(timeout=300)  # 5 minutes timeout
                        context.task_results[task_id] = result
                        
                        # Check if task failed and handle accordingly
                        if result.status == TaskStatus.FAILED:
                            self._handle_task_failure(task_id, result, context, workflow_def)
                            return
                            
                    except Exception as e:
                        logger.error(f"Task {task_id} failed: {e}")
                        self._handle_task_failure(task_id, TaskResult(
                            task_id=task_id,
                            status=TaskStatus.FAILED,
                            error=str(e)
                        ), context, workflow_def)
                        return
            
            # Workflow completed successfully
            self._finalize_workflow(context, TaskStatus.COMPLETED)
            
        except Exception as e:
            logger.error(f"Workflow execution failed: {e}")
            self._finalize_workflow(context, TaskStatus.FAILED, str(e))
    
    def _execute_task(self, task_def: Dict, context: WorkflowContext) -> TaskResult:
        """Execute a single task"""
        task_id = task_def['task_id']
        task_type = task_def.get('type', TaskType.AI_AGENT.value)
        
        logger.info(f"Executing task {task_id} of type {task_type}")
        
        start_time = datetime.now()
        
        try:
            # Publish task started event
            self.message_broker.publish_event(
                topic="task_events",
                event_type="TASK_STARTED",
                data={
                    "task_id": task_id,
                    "task_type": task_type,
                    "context": context.context_data
                },
                workflow_id=context.workflow_id,
                task_id=task_id
            )
            
            # Execute based on task type
            if task_type == TaskType.HUMAN_TASK.value:
                result = self._execute_human_task(task_def, context)
            elif task_type == TaskType.AI_AGENT.value:
                result = self._execute_ai_agent_task(task_def, context)
            elif task_type == TaskType.SERVICE_CALL.value:
                result = self._execute_service_task(task_def, context)
            elif task_type == TaskType.DECISION_GATE.value:
                result = self._execute_decision_task(task_def, context)
            else:
                result = TaskResult(
                    task_id=task_id,
                    status=TaskStatus.FAILED,
                    error=f"Unknown task type: {task_type}"
                )
            
            # Calculate execution time
            execution_time = (datetime.now() - start_time).total_seconds()
            result.execution_time = execution_time
            
            # Publish task completed event
            self.message_broker.publish_event(
                topic="task_events",
                event_type="TASK_COMPLETED",
                data={
                    "task_id": task_id,
                    "status": result.status.value,
                    "execution_time": execution_time,
                    "output": result.output
                },
                workflow_id=context.workflow_id,
                task_id=task_id
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Task {task_id} execution error: {e}")
            return TaskResult(
                task_id=task_id,
                status=TaskStatus.FAILED,
                error=str(e),
                execution_time=(datetime.now() - start_time).total_seconds()
            )
    
    def _execute_human_task(self, task_def: Dict, context: WorkflowContext) -> TaskResult:
        """Execute a human task"""
        task_id = task_def['task_id']
        
        # Create human task
        human_task = self.human_task_manager.create_task(
            task_id=task_id,
            workflow_id=context.workflow_id,
            execution_id=context.execution_id,
            task_config=task_def.get('config', {}),
            context=context.context_data
        )
        
        # Wait for human completion
        result = self.human_task_manager.wait_for_completion(
            task_id=task_id,
            timeout=task_def.get('timeout_seconds', 3600)  # Default 1 hour
        )
        
        if result:
            return TaskResult(
                task_id=task_id,
                status=TaskStatus.HUMAN_COMPLETED,
                output=result.get('output', {}),
                metadata=result.get('metadata', {})
            )
        else:
            return TaskResult(
                task_id=task_id,
                status=TaskStatus.FAILED,
                error="Human task timeout or cancelled"
            )
    
    def _execute_ai_agent_task(self, task_def: Dict, context: WorkflowContext) -> TaskResult:
        """Execute an AI agent task"""
        from framework.agents.llm_agent import LLMAgent
        
        agent_type = task_def.get('agent_type', 'default')
        agent_config = task_def.get('config', {})
        
        # Get or create agent
        agent_key = f"{agent_type}_{hash(json.dumps(agent_config, sort_keys=True))}"
        if agent_key not in self.agents:
            self.agents[agent_key] = LLMAgent(agent_config)
        
        agent = self.agents[agent_key]
        
        # Prepare input for agent
        agent_input = {
            **context.context_data,
            'task_config': task_def.get('config', {}),
            'previous_results': {
                task_id: result.output 
                for task_id, result in context.task_results.items()
            }
        }
        
        # Execute agent
        try:
            output = agent.execute(agent_input)
            
            return TaskResult(
                task_id=task_def['task_id'],
                status=TaskStatus.COMPLETED,
                output=output
            )
        except Exception as e:
            return TaskResult(
                task_id=task_def['task_id'],
                status=TaskStatus.FAILED,
                error=str(e)
            )
    
    def _build_task_graph(self, tasks: List[Dict]) -> List[List[Dict]]:
        """Build topological levels for task execution"""
        from collections import defaultdict, deque
        
        # Build adjacency list
        graph = defaultdict(list)
        in_degree = defaultdict(int)
        task_map = {task['task_id']: task for task in tasks}
        
        for task in tasks:
            task_id = task['task_id']
            in_degree[task_id] = len(task.get('dependencies', []))
            
            for dep in task.get('dependencies', []):
                graph[dep].append(task_id)
        
        # Topological sort
        queue = deque([task_id for task_id in task_map if in_degree[task_id] == 0])
        levels = []
        
        while queue:
            level_size = len(queue)
            current_level = []
            
            for _ in range(level_size):
                task_id = queue.popleft()
                current_level.append(task_map[task_id])
                
                for neighbor in graph[task_id]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
            
            levels.append(current_level)
        
        return levels
    
    def _handle_task_failure(self, task_id: str, result: TaskResult, 
                           context: WorkflowContext, workflow_def: Dict):
        """Handle task failure based on workflow configuration"""
        error_policy = workflow_def.get('error_policy', 'stop_on_failure')
        
        if error_policy == 'stop_on_failure':
            self._finalize_workflow(context, TaskStatus.FAILED, 
                                  f"Task {task_id} failed: {result.error}")
        elif error_policy == 'continue_on_failure':
            # Mark task as failed but continue
            context.task_results[task_id] = result
            # Continue with next tasks
            pass
        elif error_policy == 'retry_and_stop':
            # Implement retry logic
            pass
    
    def _finalize_workflow(self, context: WorkflowContext, 
                          status: TaskStatus, error: str = None):
        """Finalize workflow execution"""
        context.metadata['end_time'] = datetime.now()
        context.metadata['status'] = status.value
        
        # Publish workflow completed event
        self.message_broker.publish_event(
            topic="workflow_events",
            event_type="WORKFLOW_COMPLETED",
            data={
                "execution_id": context.execution_id,
                "status": status.value,
                "error": error,
                "execution_time": (datetime.now() - context.start_time).total_seconds(),
                "task_results": {
                    task_id: {
                        "status": result.status.value,
                        "execution_time": result.execution_time
                    }
                    for task_id, result in context.task_results.items()
                }
            },
            workflow_id=context.workflow_id,
            task_id="orchestrator"
        )
        
        # Clean up
        with self.lock:
            if context.execution_id in self.running_workflows:
                del self.running_workflows[context.execution_id]
        
        logger.info(f"Workflow {context.execution_id} completed with status: {status}")
    
    def _save_dag_to_airflow(self, dag, workflow_id: str):
        """Save generated DAG to Airflow DAGs folder"""
        import os
        
        dag_folder = self.config.get('airflow_dag_folder', './dags/generated_dags')
        os.makedirs(dag_folder, exist_ok=True)
        
        dag_file = os.path.join(dag_folder, f"{workflow_id}_dag.py")
        
        # This is a simplified version - in reality, you'd serialize the DAG properly
        with open(dag_file, 'w') as f:
            f.write(f"# Auto-generated DAG for workflow: {workflow_id}\n")
            f.write("# This DAG is managed by the Agentic Framework\n")
        
        logger.info(f"DAG saved to {dag_file}")
    
    def get_workflow_status(self, execution_id: str) -> Dict[str, Any]:
        """Get status of a workflow execution"""
        if execution_id not in self.running_workflows:
            # Check completed workflows in database
            return {"status": "unknown", "execution_id": execution_id}
        
        context = self.running_workflows[execution_id]
        
        return {
            "execution_id": execution_id,
            "workflow_id": context.workflow_id,
            "status": "running",
            "start_time": context.start_time.isoformat(),
            "completed_tasks": len(context.task_results),
            "current_context": context.context_data
        }