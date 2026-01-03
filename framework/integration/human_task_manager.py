"""
Manager for human-in-the-loop tasks
"""
import json
import logging
import uuid
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from enum import Enum
import threading
from collections import defaultdict

logger = logging.getLogger(__name__)


class HumanTaskStatus(Enum):
    """Status of human tasks"""
    PENDING = "pending"
    ASSIGNED = "assigned"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class HumanTaskManager:
    """Manages human tasks and their lifecycle"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.tasks: Dict[str, Dict] = {}
        self.task_results: Dict[str, Dict] = {}
        self.task_callbacks: Dict[str, List] = defaultdict(list)
        self.lock = threading.RLock()
        self.notification_service = None
        
        # Initialize storage (could be Redis, database, etc.)
        self._init_storage()
        
        # Start cleanup thread
        self._start_cleanup_thread()
        
        logger.info("Human Task Manager initialized")
    
    def _init_storage(self):
        """Initialize task storage"""
        # For simplicity, using in-memory storage
        # In production, use Redis or database
        self.tasks = {}
        self.task_results = {}
    
    def create_task(self, task_id: str, workflow_id: str, execution_id: str,
                   task_config: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new human task"""
        task_uuid = str(uuid.uuid4())
        
        task = {
            'task_uuid': task_uuid,
            'task_id': task_id,
            'workflow_id': workflow_id,
            'execution_id': execution_id,
            'config': task_config,
            'context': context,
            'status': HumanTaskStatus.PENDING.value,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
            'assigned_to': None,
            'priority': task_config.get('priority', 'medium'),
            'timeout_seconds': task_config.get('timeout_seconds', 3600),
            'instructions': task_config.get('instructions', ''),
            'expected_output_schema': task_config.get('output_schema', {}),
            'attachments': task_config.get('attachments', []),
            'metadata': task_config.get('metadata', {})
        }
        
        with self.lock:
            self.tasks[task_uuid] = task
        
        # Notify about new task
        self._notify_new_task(task)
        
        logger.info(f"Created human task {task_uuid} for workflow {workflow_id}")
        return task
    
    def get_pending_tasks(self, assignee: Optional[str] = None, 
                         priority: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get pending tasks, optionally filtered by assignee and priority"""
        with self.lock:
            pending_tasks = []
            
            for task in self.tasks.values():
                if task['status'] == HumanTaskStatus.PENDING.value:
                    if assignee and task.get('assigned_to') != assignee:
                        continue
                    if priority and task.get('priority') != priority:
                        continue
                    pending_tasks.append(task)
        
        return sorted(pending_tasks, 
                     key=lambda x: (x['priority'] == 'high', 
                                   x['created_at']),
                     reverse=True)
    
    def assign_task(self, task_uuid: str, assignee: str) -> bool:
        """Assign a task to a human user"""
        with self.lock:
            if task_uuid not in self.tasks:
                logger.error(f"Task {task_uuid} not found")
                return False
            
            task = self.tasks[task_uuid]
            
            if task['status'] != HumanTaskStatus.PENDING.value:
                logger.error(f"Task {task_uuid} is not pending")
                return False
            
            task['status'] = HumanTaskStatus.ASSIGNED.value
            task['assigned_to'] = assignee
            task['updated_at'] = datetime.now().isoformat()
            task['assigned_at'] = datetime.now().isoformat()
            
            self.tasks[task_uuid] = task
        
        # Notify assignee
        self._notify_task_assigned(task, assignee)
        
        logger.info(f"Task {task_uuid} assigned to {assignee}")
        return True
    
    def get_task_details(self, task_uuid: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a task"""
        with self.lock:
            if task_uuid in self.tasks:
                return self.tasks[task_uuid]
            elif task_uuid in self.task_results:
                return self.task_results[task_uuid].get('task', {})
        
        return None
    
    def submit_task_result(self, task_uuid: str, assignee: str, 
                          result: Dict[str, Any], notes: str = "") -> bool:
        """Submit result for a human task"""
        with self.lock:
            if task_uuid not in self.tasks:
                logger.error(f"Task {task_uuid} not found")
                return False
            
            task = self.tasks[task_uuid]
            
            if task['assigned_to'] != assignee:
                logger.error(f"Task {task_uuid} not assigned to {assignee}")
                return False
            
            if task['status'] not in [HumanTaskStatus.ASSIGNED.value, 
                                     HumanTaskStatus.IN_PROGRESS.value]:
                logger.error(f"Task {task_uuid} not in assignable state")
                return False
            
            # Validate result against schema if provided
            if not self._validate_result(result, task.get('expected_output_schema')):
                logger.error(f"Result validation failed for task {task_uuid}")
                return False
            
            # Store result
            task_result = {
                'task_uuid': task_uuid,
                'task_id': task['task_id'],
                'workflow_id': task['workflow_id'],
                'execution_id': task['execution_id'],
                'assignee': assignee,
                'result': result,
                'notes': notes,
                'submitted_at': datetime.now().isoformat(),
                'status': HumanTaskStatus.COMPLETED.value,
                'metadata': task.get('metadata', {})
            }
            
            self.task_results[task_uuid] = task_result
            
            # Update task status
            task['status'] = HumanTaskStatus.COMPLETED.value
            task['updated_at'] = datetime.now().isoformat()
            task['completed_at'] = datetime.now().isoformat()
            
            self.tasks[task_uuid] = task
        
        # Notify workflow about completion
        self._notify_task_completed(task, task_result)
        
        # Execute callbacks
        self._execute_callbacks(task_uuid, task_result)
        
        logger.info(f"Task {task_uuid} completed by {assignee}")
        return True
    
    def reject_task(self, task_uuid: str, assignee: str, 
                   reason: str = "") -> bool:
        """Reject a task (human cannot complete it)"""
        with self.lock:
            if task_uuid not in self.tasks:
                return False
            
            task = self.tasks[task_uuid]
            
            if task['assigned_to'] != assignee:
                return False
            
            task['status'] = HumanTaskStatus.REJECTED.value
            task['updated_at'] = datetime.now().isoformat()
            task['rejection_reason'] = reason
            
            self.tasks[task_uuid] = task
        
        # Notify about rejection
        self._notify_task_rejected(task, assignee, reason)
        
        logger.warning(f"Task {task_uuid} rejected by {assignee}: {reason}")
        return True
    
    def wait_for_completion(self, task_id: str, 
                          timeout: int = 3600) -> Optional[Dict[str, Any]]:
        """Wait for a specific task to complete"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            with self.lock:
                # Find task by task_id (need to search)
                task_uuid = None
                for uuid, task in self.tasks.items():
                    if task['task_id'] == task_id and \
                       task['status'] == HumanTaskStatus.COMPLETED.value:
                        task_uuid = uuid
                        break
                
                if task_uuid and task_uuid in self.task_results:
                    return self.task_results[task_uuid]['result']
            
            time.sleep(1)  # Poll every second
        
        return None
    
    def register_callback(self, task_uuid: str, callback: callable):
        """Register a callback for when task completes"""
        with self.lock:
            self.task_callbacks[task_uuid].append(callback)
    
    def _execute_callbacks(self, task_uuid: str, result: Dict[str, Any]):
        """Execute registered callbacks"""
        if task_uuid in self.task_callbacks:
            for callback in self.task_callbacks[task_uuid]:
                try:
                    callback(result)
                except Exception as e:
                    logger.error(f"Callback execution failed: {e}")
    
    def _validate_result(self, result: Dict[str, Any], 
                        schema: Optional[Dict[str, Any]]) -> bool:
        """Validate result against expected schema"""
        if not schema:
            return True
        
        # Simplified validation - in production use jsonschema
        try:
            for key, value_type in schema.items():
                if key not in result:
                    if 'required' in schema.get(key, {}):
                        return False
                else:
                    # Basic type checking
                    expected_type = schema[key].get('type', 'any')
                    
                    if expected_type == 'string' and not isinstance(result[key], str):
                        return False
                    elif expected_type == 'number' and not isinstance(result[key], (int, float)):
                        return False
                    elif expected_type == 'boolean' and not isinstance(result[key], bool):
                        return False
                    elif expected_type == 'array' and not isinstance(result[key], list):
                        return False
                    elif expected_type == 'object' and not isinstance(result[key], dict):
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False
    
    def _notify_new_task(self, task: Dict[str, Any]):
        """Notify about new task"""
        # In production, integrate with notification services
        # (Slack, Email, MS Teams, etc.)
        logger.info(f"New human task available: {task['task_uuid']}")
        
        # Example: Send to notification queue
        notification = {
            'type': 'new_human_task',
            'task_uuid': task['task_uuid'],
            'task_id': task['task_id'],
            'workflow_id': task['workflow_id'],
            'priority': task['priority'],
            'instructions': task['instructions'],
            'created_at': task['created_at']
        }
        
        # This would publish to Kafka or call a webhook
        # self.message_broker.publish_event(...)
    
    def _notify_task_assigned(self, task: Dict[str, Any], assignee: str):
        """Notify about task assignment"""
        logger.info(f"Task {task['task_uuid']} assigned to {assignee}")
    
    def _notify_task_completed(self, task: Dict[str, Any], result: Dict[str, Any]):
        """Notify about task completion"""
        logger.info(f"Task {task['task_uuid']} completed")
        
        # Notify workflow orchestrator
        notification = {
            'type': 'human_task_completed',
            'task_uuid': task['task_uuid'],
            'task_id': task['task_id'],
            'workflow_id': task['workflow_id'],
            'execution_id': task['execution_id'],
            'assignee': result['assignee'],
            'result': result['result'],
            'completed_at': result['submitted_at']
        }
    
    def _notify_task_rejected(self, task: Dict[str, Any], assignee: str, reason: str):
        """Notify about task rejection"""
        logger.warning(f"Task {task['task_uuid']} rejected by {assignee}: {reason}")
    
    def _start_cleanup_thread(self):
        """Start thread to clean up expired tasks"""
        def cleanup_loop():
            while True:
                try:
                    self._cleanup_expired_tasks()
                    time.sleep(60)  # Check every minute
                except Exception as e:
                    logger.error(f"Cleanup error: {e}")
                    time.sleep(60)
        
        thread = threading.Thread(target=cleanup_loop, daemon=True)
        thread.start()
    
    def _cleanup_expired_tasks(self):
        """Clean up tasks that have expired"""
        current_time = datetime.now()
        
        with self.lock:
            expired_tasks = []
            
            for task_uuid, task in self.tasks.items():
                if task['status'] == HumanTaskStatus.PENDING.value:
                    created_at = datetime.fromisoformat(task['created_at'])
                    timeout = task.get('timeout_seconds', 3600)
                    
                    if (current_time - created_at).total_seconds() > timeout:
                        task['status'] = HumanTaskStatus.EXPIRED.value
                        task['updated_at'] = current_time.isoformat()
                        expired_tasks.append(task_uuid)
            
            # Log expired tasks
            if expired_tasks:
                logger.warning(f"Expired {len(expired_tasks)} human tasks")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about human tasks"""
        with self.lock:
            total_tasks = len(self.tasks)
            pending_tasks = len([t for t in self.tasks.values() 
                                if t['status'] == HumanTaskStatus.PENDING.value])
            completed_tasks = len(self.task_results)
            
            return {
                'total_tasks': total_tasks,
                'pending_tasks': pending_tasks,
                'completed_tasks': completed_tasks,
                'assigned_tasks': len([t for t in self.tasks.values() 
                                      if t['status'] == HumanTaskStatus.ASSIGNED.value]),
                'expired_tasks': len([t for t in self.tasks.values() 
                                     if t['status'] == HumanTaskStatus.EXPIRED.value]),
                'timestamp': datetime.now().isoformat()
            }