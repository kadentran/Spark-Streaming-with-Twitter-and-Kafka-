import tweepy
import sys
from tweepy.streaming import Stream
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer

consumer_key = "RF07C0oQndwzo2lk5OPWaHOTF"
consumer_secret = "IlpDoqW1a9O1w4hqMlCJovaed8uVPMzRleWWk1Z7PsM0dbo2AX"
access_token = "1000276183-P4o31A7jhIgUFogHfsERhWa9lj6w4kGh12QJnj1"
access_secret = "tf7Iffg5OmvUk6sncT5jwKlgvNanaxQuTL02eGUfs6dFn"

#bootstrapServers = 'localhost:9092'
bootstrapServers = sys.argv[1]
#topics = 'test'
topics = sys.argv[2]
searchTerm = sys.argv[3]
language = sys.argv[4]

producer = KafkaProducer(bootstrap_servers=bootstrapServers)
future = producer.send(topics, b'raw_bytes')
try:
    record_metadata = future.get(timeout=10)
except KafkaError:
    log.exception()
    pass

class Printer(Stream):
    def on_status(self, status):
        #print(status.text)
        print("streaming")
        producer.send(topics, bytes(status.text, "utf-8"))

printer = Printer(consumer_key, consumer_secret, access_token, access_secret)

if __name__ == "__main__":
    printer.filter(track=[searchTerm], languages=[language])
