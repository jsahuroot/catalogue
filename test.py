import io
import struct
import json

from avro.io import BinaryDecoder, DatumReader
from confluent_kafka import Consumer
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError

# Please adjust your server and url

# KAFKA BROKER URL
consumer = Consumer({
    'bootstrap.servers': 'vodafone-irl-staging-admin-0:9092',
    'group.id': 'consumer-test',
    'auto.offset.reset': 'smallest'
})

# SCHEMA URL
register_client = CachedSchemaRegistryClient(url="http://vodafone-irl-staging-admin-0:8082")
consumer.subscribe(['catserver-vodafone-cz-catalog'])

MAGIC_BYTES = 0


def decode_avro(payload):
    magic, schema_id = struct.unpack('>bi', payload[:5])

    # Get Schema registry
    # Avro value format
    if magic == MAGIC_BYTES:
        schema = register_client.get_by_id(schema_id)
        reader = DatumReader(schema)
        output = BinaryDecoder(io.BytesIO(payload[5:]))
        decoded = reader.read(output)
        return decoded, schema.name
    # no magic bytes, something is wrong
    else:
        raise ValueError

def get_data():
    while True:
        try:
            msg = consumer.poll(10)
        except SerializerError as e:
            print("Message deserialization failed for {}: {}".format(msg, e))
            raise SerializerError

        if msg.error():
            print("AvroConsumer error: {}".format(msg.error()))
            return

        id = msg.key()
        if msg.value() is None:
            message = None
            type = None
        else:
            message, type = decode_avro(msg.value())
        a = open('test.txt', 'w')
        #a.write("ID: {}, Type: {}, Content: {}".format(id, type, message))
        d = "ID: {}, Type: {}, Content: {}".format(id, type, message)
        s = json.dumps(message)
        #print(s)

if __name__ == '__main__':
    get_data()
