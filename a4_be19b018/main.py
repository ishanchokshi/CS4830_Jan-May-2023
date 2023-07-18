def pubsub_test(data, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         data (dict): data payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    from google.cloud import pubsub_v1
    publisher = pubsub_v1.PublisherClient() # Create a PublisherClient object
    topic_name = publisher.topic_path('be19b018', 'lab6') # Create a topic object
    pubsub_message = bytes(data['name'], 'utf-8') # Convert the name of the received file to bytes
    publish = publisher.publish(topic_name, pubsub_message, spam = 'eggs') # Publish the message to the specified topic  along with the received filename
    print(publish.result()) # This is dont to wait for the message to be successfully published before continuing

    