from google.cloud import storage
from google.cloud import pubsub_v1


#Define a function to be called when a message is received
def callback(message):
    # Get the filename from the message payload
    filename = message.data.decode("utf-8") 
    blob  = storage.Client().get_bucket('be19b018_lab6').blob(filename)
    # Download the contents of the blob as a string and convert them into to a UTF-8 string
    contents = blob.download_as_string()
    contents = contents.decode('utf-8') 
    lines = contents.split('\n') # Split the string into individual lines and count them
    print("There are {} lines in the file".format(len(lines))) 
    
    # Acknowledge the message and tell Google Cloud Pub/Sub that it has been processed
    message.ack()


subscriber = pubsub_v1.SubscriberClient() #Create a subscriber client
#Create a subscription object to listen for messages
subscription_location = subscriber.subscription_path('be19b018', 'lab6-sub')
#Subscribe to the specified subscription and register the callback function to be called when a message is received
future = subscriber.subscribe(subscription_location, callback=callback)

#Wait for messages to be received and processed unless the program is interrupted by the user
try:
    future.result()
except KeyboardInterrupt:
    future.cancel()