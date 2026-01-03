"""
Airflow DAG builder for workflow orchestration
"""
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
from textwrap import dedent
import logging

logger = logging.getLogger(__name__)


class AirflowDAGBuilder:
    """Builds Airflow DAGs from workflow definitions"""
    
    def __init__(self, workflow_definition: Dict[str, Any]):
        self.workflow = workflow_definition
        self.dag_id = f"agentic_{workflow_definition['workflow_id']}"
        self.tasks = {}
        
    def build_dag_code(self) -> str:
        """Generate Python code for Airflow DAG"""
        
        # Get workflow schedule
        schedule_interval = self.workflow.get('triggers', {}).get('scheduled', None)
        
        # Start building DAG code
        dag_code = dedent(f'''
        """
        Auto-generated DAG for Agentic Framework workflow: {self.workflow['name']}
        Workflow ID: {self.workflow['workflow_id']}
        Generated: {datetime.now().isoformat()}
        """
        
        from datetime import datetime, timedelta
        from airflow import DAG
        from airflow.operators.python import PythonOperator
        from airflow.operators.dummy import DummyOperator
        from airflow.operators.branch import BaseBranchOperator
        from airflow.utils.task_group import TaskGroup
        from airflow.decorators import task
        import json
        import requests
        import logging
        
        logger = logging.getLogger(__name__)
        
        # Framework API URL
        FRAMEWORK_API = "http://framework-api:8000"
        
        def execute_agentic_task(task_id, workflow_id, **context):
            """Execute a task through the Agentic Framework API"""
            import requests
            
            execution_id = context['dag_run'].run_id
            
            # Prepare task data
            task_data = {{
                'task_id': task_id,
                'workflow_id': workflow_id,
                'execution_id': execution_id,
                'trigger_data': context['params'] if context['params'] else {{}}
            }}
            
            # Call framework API
            response = requests.post(
                f"{{FRAMEWORK_API}}/api/v1/tasks/execute",
                json=task_data,
                timeout=300
            )
            
            if response.status_code != 200:
                raise Exception(f"Task execution failed: {{response.text}}")
            
            result = response.json()
            
            if result.get('status') == 'failed':
                raise Exception(f"Task {{task_id}} failed: {{result.get('error')}}")
            
            # Push result to XCom
            context['ti'].xcom_push(key=task_id, value=result.get('output', {{}}))
            
            return result.get('output', {{}})
        
        def wait_for_human_task(task_id, workflow_id, **context):
            """Wait for human task completion"""
            import time
            import requests
            
            execution_id = context['dag_run'].run_id
            
            # Poll for human task completion
            max_wait_time = 3600  # 1 hour
            poll_interval = 30  # 30 seconds
            
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                try:
                    response = requests.get(
                        f"{{FRAMEWORK_API}}/api/v1/tasks/human/status/"
                        f"{{workflow_id}}/{{execution_id}}/{{task_id}}",
                        timeout=10
                    )
                    
                    if response.status_code == 200:
                        status_data = response.json()
                        
                        if status_data.get('status') == 'completed':
                            # Get task result
                            result_response = requests.get(
                                f"{{FRAMEWORK_API}}/api/v1/tasks/human/result/"
                                f"{{workflow_id}}/{{execution_id}}/{{task_id}}",
                                timeout=10
                            )
                            
                            if result_response.status_code == 200:
                                result = result_response.json()
                                context['ti'].xcom_push(key=task_id, value=result.get('output', {{}}))
                                return result.get('output', {{}})
                        
                        elif status_data.get('status') == 'failed':
                            raise Exception(f"Human task {{task_id}} failed")
                
                except Exception as e:
                    logger.warning(f"Error polling human task: {{e}}")
                
                time.sleep(poll_interval)
            
            raise Exception(f"Human task {{task_id}} timeout")
        
        def make_decision(**context):
            """Make decision based on previous task results"""
            # Get results from previous tasks
            task_results = {{}}
            
            for task_def in {json.dumps(self.workflow['tasks'])}:
                if task_def['task_id'] in context['ti'].xcom_pull():
                    task_results[task_def['task_id']] = context['ti'].xcom_pull(key=task_def['task_id'])
            
            # Simple decision logic - can be customized
            # In practice, this would call a decision agent
            return {{"next_step": "continue"}}
        
        # Default arguments for the DAG
        default_args = {{
            'owner': 'agentic_framework',
            'depends_on_past': False,
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 2,
            'retry_delay': timedelta(minutes=5),
            'start_date': datetime(2024, 1, 1),
        }}
        
        # Define the DAG
        with DAG(
            dag_id='{self.dag_id}',
            default_args=default_args,
            description='{self.workflow.get('description', 'Agentic Workflow')}',
            schedule_interval='{schedule_interval or 'None'}',
            catchup=False,
            tags=['agentic', '{self.workflow['workflow_id']}'],
            max_active_runs=1
        ) as dag:
            
            # Define tasks
        ''')
        
        # Add task definitions
        for task_def in self.workflow['tasks']:
            task_code = self._build_task_code(task_def)
            dag_code += task_code + "\n\n"
        
        # Add dependencies
        dag_code += "\n            # Set task dependencies\n"
        for task_def in self.workflow['tasks']:
            task_id = task_def['task_id']
            dependencies = task_def.get('dependencies', [])
            
            if dependencies:
                dep_string = " >> ".join([f"task_{dep}" for dep in dependencies] + [f"task_{task_id}"])
                dag_code += f"            {dep_string}\n"
        
        # Add documentation
        dag_code += dedent('''
        
            # DAG documentation
            dag.doc_md = f"""
            # {self.workflow['name']}
            
            {self.workflow.get('description', 'No description')}
            
            ## Tasks
            {task_list}
            
            ## Parameters
            - Workflow ID: {self.workflow['workflow_id']}
            - Version: {self.workflow.get('version', '1.0')}
            """
        
        return dag
        ''')
        
        return dag_code
    
    def _build_task_code(self, task_def: Dict[str, Any]) -> str:
        """Build Python code for a single task"""
        task_id = task_def['task_id']
        task_type = task_def.get('type', 'ai_agent')
        task_name = task_def.get('name', task_id)
        
        # Build operator based on task type
        if task_type == 'human_task':
            operator_code = dedent(f'''
                task_{task_id} = PythonOperator(
                    task_id='{task_id}',
                    python_callable=wait_for_human_task,
                    op_kwargs={{
                        'task_id': '{task_id}',
                        'workflow_id': '{self.workflow['workflow_id']}'
                    }},
                    provide_context=True,
                    dag=dag
                )
            ''')
        
        elif task_type == 'decision_gate':
            operator_code = dedent(f'''
                task_{task_id} = PythonOperator(
                    task_id='{task_id}',
                    python_callable=make_decision,
                    provide_context=True,
                    dag=dag
                )
            ''')
        
        else:  # ai_agent or service_call
            operator_code = dedent(f'''
                task_{task_id} = PythonOperator(
                    task_id='{task_id}',
                    python_callable=execute_agentic_task,
                    op_kwargs={{
                        'task_id': '{task_id}',
                        'workflow_id': '{self.workflow['workflow_id']}'
                    }},
                    provide_context=True,
                    dag=dag
                )
            ''')
        
        return operator_code
    
    def save_dag_file(self, output_dir: str = "./dags/generated_dags"):
        """Save generated DAG to file"""
        os.makedirs(output_dir, exist_ok=True)
        
        dag_code = self.build_dag_code()
        filename = os.path.join(output_dir, f"{self.dag_id}.py")
        
        with open(filename, 'w') as f:
            f.write(dag_code)
        
        logger.info(f"DAG saved to {filename}")
        return filename
    
    def build_and_deploy(self, airflow_webserver_url: str = "http://localhost:8080"):
        """Build and deploy DAG to Airflow"""
        dag_file = self.save_dag_file()
        
        # In production, you would:
        # 1. Save to Airflow's DAG folder
        # 2. Trigger Airflow to refresh DAGs
        # 3. Verify DAG is loaded correctly
        
        logger.info(f"DAG {self.dag_id} deployed successfully")
        return dag_file