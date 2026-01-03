"""
Kafka message broker for inter-agent communication
"""
import json
import logging
from typing import Dict, List, Any, Optional, Callable
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import threading
import uuid
from datetime import datetime

logger = logging.getLogger(__name__)


class KafkaMessageBroker:
    """Message broker using Apache Kafka"""
    
    def __init__(self, bootstrap_servers='localhost:9092', group_id='agentic_framework'):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.producer = None
        self.consumers = {}
        self.callbacks = {}
        self.running = False
        self.consumer_threads = {}
        
        # Initialize producer
        self._init_producer()
        
        # Create default topics
        self.default_topics = [
            'workflow_events',
            'task_events',
            'human_tasks',
            'agent_messages',
            'system_alerts'
        ]
        
    def _init_producer(self):
        """Initialize Kafka producer"""
        producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'agentic-framework-producer',
            'acks': 'all',
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,
            'enable.idempotence': True
        }
        
        try:
            self.producer = Producer(**producer_config)
            logger.info("Kafka producer initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def publish_event(self, topic: str, event_type: str, data: Dict[str, Any],
                     workflow_id: str, task_id: str, **kwargs) -> bool:
        """Publish event to Kafka topic"""
        if not self.producer:
            logger.error("Kafka producer not initialized")
            return False
        
        try:
            message = {
                'message_id': str(uuid.uuid4()),
                'timestamp': datetime.now().isoformat(),
                'event_type': event_type,
                'workflow_id': workflow_id,
                'task_id': task_id,
                'data': data,
                'metadata': {
                    'source': 'agentic_framework',
                    'version': '1.0',
                    **kwargs.get('metadata', {})
                }
            }
            
            # Serialize message
            message_bytes = json.dumps(message).encode('utf-8')
            
            # Produce message
            self.producer.produce(
                topic=topic,
                value=message_bytes,
                key=workflow_id.encode('utf-8'),
                callback=self._delivery_callback
            )
            
            # Flush to ensure delivery
            self.producer.flush(timeout=5)
            
            logger.debug(f"Published event to {topic}: {event_type}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish event to {topic}: {e}")
            return False
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery"""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def subscribe(self, topic: str, callback: Callable[[Dict[str, Any]], None],
                  consumer_group: Optional[str] = None) -> str:
        """Subscribe to a Kafka topic"""
        consumer_id = str(uuid.uuid4())
        
        # Store callback
        self.callbacks[consumer_id] = {
            'topic': topic,
            'callback': callback,
            'group_id': consumer_group or self.group_id
        }
        
        # Start consumer thread if not already running for this topic
        if topic not in self.consumer_threads:
            self._start_consumer_thread(topic, consumer_group or self.group_id)
        
        return consumer_id
    
    def _start_consumer_thread(self, topic: str, group_id: str):
        """Start a consumer thread for a topic"""
        if topic in self.consumer_threads:
            return
        
        thread = threading.Thread(
            target=self._consume_messages,
            args=(topic, group_id),
            daemon=True
        )
        thread.start()
        
        self.consumer_threads[topic] = thread
        logger.info(f"Started consumer thread for topic: {topic}")
    
    def _consume_messages(self, topic: str, group_id: str):
        """Consume messages from Kafka topic"""
        consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'max.poll.interval.ms': 300000
        }
        
        try:
            consumer = Consumer(consumer_config)
            consumer.subscribe([topic])
            
            self.running = True
            
            while self.running:
                try:
                    msg = consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            logger.error(f"Consumer error: {msg.error()}")
                            break
                    
                    # Parse message
                    try:
                        message_data = json.loads(msg.value().decode('utf-8'))
                        
                        # Find and execute callbacks for this topic
                        for consumer_id, callback_info in self.callbacks.items():
                            if callback_info['topic'] == topic:
                                try:
                                    callback_info['callback'](message_data)
                                except Exception as e:
                                    logger.error(f"Callback execution failed: {e}")
                    
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse message: {e}")
                    
                except Exception as e:
                    logger.error(f"Error in consumer loop: {e}")
                    break
            
            consumer.close()
            
        except Exception as e:
            logger.error(f"Consumer thread failed: {e}")
    
    def unsubscribe(self, consumer_id: str):
        """Unsubscribe from a topic"""
        if consumer_id in self.callbacks:
            del self.callbacks[consumer_id]
            logger.info(f"Unsubscribed consumer: {consumer_id}")
    
    def create_topic(self, topic_name: str, partitions: int = 3, replication: int = 1):
        """Create a new Kafka topic"""
        # Note: In production, you'd use Kafka AdminClient
        # This is a simplified version
        logger.info(f"Topic creation requested: {topic_name}")
        
        # In a real implementation, you'd use:
        # from confluent_kafka.admin import AdminClient, NewTopic
        
    def get_topic_metrics(self, topic: str) -> Dict[str, Any]:
        """Get metrics for a topic"""
        # Simplified metrics
        return {
            'topic': topic,
            'consumer_count': len([
                cid for cid, info in self.callbacks.items()
                if info['topic'] == topic
            ]),
            'status': 'active'
        }
    
    def close(self):
        """Close all Kafka connections"""
        self.running = False
        
        # Close producer
        if self.producer:
            self.producer.flush(timeout=10)
        
        logger.info("Kafka broker shut down")