import json
import time
import os
import logging
import pandas as pd
from kafka import KafkaProducer

# Konfigurasi logging untuk memantau progres
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            compression_type='gzip',
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432
        )
        logger.info("Koneksi ke Kafka berhasil dibuat.")
        return producer
    except Exception as e:
        logger.error(f"Gagal terhubung ke Kafka: {e}")
        time.sleep(30)
        return create_kafka_producer()

def run_producer():
    producer = create_kafka_producer()
    topic = os.getenv('KAFKA_TOPIC', 'ecommerce-events')
    
    # Path ke file dataset di dalam container
    csv_path = '/app/datasets/Reviews.csv'
    text_path = '/app/datasets/ReviewText.txt'
    
    # Ukuran potongan data yang akan dibaca per iterasi
    CHUNK_SIZE = 10000
    
    try:
        logger.info(f"Mulai memproses dataset dalam potongan berukuran {CHUNK_SIZE} baris.")
        
        # Menggunakan iterator untuk membaca CSV dalam potongan
        metadata_iterator = pd.read_csv(csv_path, chunksize=CHUNK_SIZE)
        
        # Membuka file teks untuk dibaca baris per baris
        with open(text_path, 'r', encoding='utf-8') as text_file:
            chunk_num = 0
            for chunk_df in metadata_iterator:
                chunk_num += 1
                logger.info(f"Memproses potongan (chunk) ke-{chunk_num}...")
                
                # Membaca jumlah baris teks yang sesuai dengan ukuran chunk
                text_lines = []
                for _ in range(len(chunk_df)):
                    line = text_file.readline()
                    if not line:
                        break
                    text_lines.append(line.strip())
                
                # Menambahkan kolom teks ke chunk DataFrame
                if len(text_lines) == len(chunk_df):
                    chunk_df['Text'] = text_lines
                else:
                    logger.warning(f"Ketidakcocokan jumlah baris di chunk {chunk_num}. Melewati chunk ini.")
                    continue

                # Mengirim setiap baris dari chunk yang sudah digabungkan
                for index, row in chunk_df.iterrows():
                    event = row.to_dict()
                    producer.send(topic, value=event, key=str(event.get('Id')))
                
                logger.info(f"Selesai mengirim {len(chunk_df)} event dari potongan ke-{chunk_num}.")
                time.sleep(15 * 60) # Delay 15 menit antara pengiriman setiap event 
                
        logger.info("Semua data telah berhasil diproses dan dikirim.")
        producer.flush()

    except FileNotFoundError:
        logger.error(f"Error: Salah satu file tidak ditemukan. Periksa path: {csv_path} dan {text_path}")
    except Exception as e:
        logger.error(f"Terjadi error yang tidak terduga saat produksi data: {e}", exc_info=True)
    finally:
        if producer:
            producer.close()
            logger.info("Koneksi Kafka ditutup.")

if __name__ == "__main__":
    run_producer()