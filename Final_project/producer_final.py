from kafka import KafkaProducer
from google.cloud import storage

client = storage.Client()
bucket = client.get_bucket("yelp_test")

#fetch test dataset blobs
test_blobs = []
blob_iterator = bucket.list_blobs()


for blob in blob_iterator:
	x = blob.download_as_string()
	x = x.decode('utf-8')
	producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer = lambda k: k.encode('utf-8'))
	data = x.split("\n")
	for row in data:
		producer.send("yelp-nlp", row)
		print(row)
		producer.flush()