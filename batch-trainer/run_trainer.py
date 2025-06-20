import time
import schedule
import logging
from batch_training_engine import BatchTrainingEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Batch Training Service...")
    trainer = BatchTrainingEngine()

    # Schedule jobs
    schedule.every(30).minutes.do(trainer.check_and_train)
    schedule.every().day.at("02:00").do(trainer.full_retrain)

    logger.info("Running initial training on startup...")
    trainer.initial_training()

    logger.info("Batch training scheduler started.")
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()