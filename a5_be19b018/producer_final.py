import time
import csv
from google.cloud import storage
from kafka import KafkaProducer
from json import dumps

GCS_BUCKET_NAME = "a5_lab8"
KAFKA_TOPIC_NAME = "test-topic"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
BATCH_SIZE = 10
TOTAL_RECORDS = 100


def get_kafka_producer():
    return KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")
    cloud_storage_client = storage.Client()
    bucket = cloud_storage_client.bucket(GCS_BUCKET_NAME)
    # Get blob iterator from GCS bucket
    blob_iterator = bucket.list_blobs()

    # Initialize variables for tracking batch and record counts
    batch_count = 0
    record_count = 0

    batch_list = []
    for blob in blob_iterator:

        # Get blob content
        content = blob.download_as_string().decode("utf-8")

        reader = csv.reader(content.splitlines())
        # Iterate through the csv file content
        for row in reader:

            if row:
                batch_list.append(','.join(row))

                record_count += 1

                # Check if batch size is reached
                if len(batch_list) == BATCH_SIZE:

                    # Get Kafka producer
                    producer = get_kafka_producer()

                    # Write batch to Kafka
                    for record in batch_list:
                        producer.send(KAFKA_TOPIC_NAME, value=record)


                    producer.flush()

                    # Increment batch count
                    batch_count += 1


                    # Check if total record count is reached
                    if record_count == TOTAL_RECORDS:
                        break

                    # Sleep for 10 seconds
                    time.sleep(10)

                    # Clear batch list
                    batch_list = []

        # Check if total record count is reached
        if record_count == TOTAL_RECORDS:
            break

    print("Kafka Producer Application Completed. ")
