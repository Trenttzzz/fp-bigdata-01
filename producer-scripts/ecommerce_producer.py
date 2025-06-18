import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EcommerceDataProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv('AUTOMQ_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            # AutoMQ optimization configs
            compression_type='gzip',
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432
        )
        self.fake = Faker()
        self.topic = os.getenv('AUTOMQ_TOPIC', 'ecommerce-events')
        
        # Load Amazon dataset if available
        self.load_amazon_data()
        
    def load_amazon_data(self):
        try:
            dataset_path = '/app/datasets/amazon_reviews.csv'
            if os.path.exists(dataset_path):
                self.amazon_data = pd.read_csv(dataset_path).to_dict('records')
                logger.info(f"Loaded {len(self.amazon_data)} Amazon records")
            else:
                self.amazon_data = []
                logger.info("No Amazon dataset found, generating synthetic data")
        except Exception as e:
            logger.error(f"Error loading Amazon data: {e}")
            self.amazon_data = []
    
    def generate_event(self):
        event_types = ['purchase', 'view', 'cart_add', 'cart_remove', 'search', 'review']
        event_type = random.choice(event_types)
        
        # Use Amazon data if available, otherwise generate synthetic
        if self.amazon_data and random.random() < 0.7:
            base_data = random.choice(self.amazon_data)
            product_id = base_data.get('asin', self.fake.uuid4())
            product_title = base_data.get('title', self.fake.catch_phrase())
            category = base_data.get('category', 'Electronics')
            price = float(base_data.get('price', random.uniform(10, 1000)))
            rating = base_data.get('rating', random.uniform(1, 5))
        else:
            product_id = self.fake.uuid4()
            product_title = self.fake.catch_phrase()
            category = random.choice(['Electronics', 'Books', 'Clothing', 'Home', 'Sports'])
            price = random.uniform(10, 1000)
            rating = random.uniform(1, 5)
        
        event = {
            'event_id': self.fake.uuid4(),
            'event_type': event_type,
            'timestamp': datetime.utcnow().isoformat(),
            'user_id': self.fake.uuid4(),
            'session_id': self.fake.uuid4(),
            'product_id': product_id,
            'product_title': product_title,
            'category': category,
            'price': round(price, 2),
            'quantity': random.randint(1, 5) if event_type == 'purchase' else 1,
            'user_agent': self.fake.user_agent(),
            'ip_address': self.fake.ipv4(),
            'country': self.fake.country(),
            'city': self.fake.city(),
            'device_type': random.choice(['mobile', 'desktop', 'tablet'])
        }
        
        if event_type == 'search':
            event['search_query'] = ' '.join(self.fake.words(nb=random.randint(1, 4)))
        elif event_type == 'review':
            event['rating'] = round(rating, 1)
            event['review_text'] = self.fake.text(max_nb_chars=200)
            
        return event
    
    def run(self):
        interval = int(os.getenv('PRODUCER_INTERVAL', 3))
        logger.info(f"Starting AutoMQ producer with {interval}s interval")
        
        while True:
            try:
                event = self.generate_event()
                future = self.producer.send(
                    self.topic, 
                    value=event, 
                    key=event['user_id']
                )
                # Optional: wait for send confirmation
                # future.get(timeout=10)
                
                logger.info(f"Sent to AutoMQ: {event['event_type']} - {event['product_title'][:50]}...")
                time.sleep(interval)
            except Exception as e:
                logger.error(f"Error sending to AutoMQ: {e}")
                time.sleep(10)

if __name__ == "__main__":
    producer = EcommerceDataProducer()
    producer.run()