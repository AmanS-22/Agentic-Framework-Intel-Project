# framework/agents/llm_agent.py
import json
import logging
from typing import Dict, Any
import openai

logger = logging.getLogger(__name__)

class LLMAgent:
    """Base LLM agent for AI tasks"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.model = config.get('model', 'gpt-3.5-turbo')
        self.temperature = config.get('temperature', 0.7)
        
        # Initialize OpenAI client
        api_key = config.get('api_key')
        if api_key:
            self.client = openai.OpenAI(api_key=api_key)
        else:
            self.client = openai.OpenAI()
    
    def execute(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute AI task"""
        try:
            instructions = self.config.get('instructions', '')
            
            # Prepare prompt
            prompt = f"{instructions}\n\nInput data: {json.dumps(input_data, indent=2)}"
            
            # Call LLM
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an AI assistant."},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                max_tokens=1000
            )
            
            result = response.choices[0].message.content
            
            # Parse result if it's JSON
            try:
                return json.loads(result)
            except:
                return {"response": result}
                
        except Exception as e:
            logger.error(f"LLM agent execution failed: {e}")
            raise