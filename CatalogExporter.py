import io
import struct
import json
import gzip
from collections import OrderedDict

from avro.io import BinaryDecoder, DatumReader
from confluent_kafka import Consumer
from confluent_kafka import TopicPartition
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError

# Please adjust your server and url

# KAFKA BROKER URL
consumer = Consumer({
    'bootstrap.servers': 'vodafone-irl-staging-admin-0:9092',
    'group.id': 'consumer-test',
    'auto.offset.reset': 'earliest'
})

# SCHEMA URL
register_client = CachedSchemaRegistryClient(url="http://vodafone-irl-staging-admin-0:8082")

def my_on_assign(consumer, partitions):
    for p in partitions:
        # some starting offset, or use OFFSET_BEGINNING, et, al.
        # the default offset is STORED which means use committed offsets, and if
        # no committed offsets are available use auto.offset.reset config (default latest)
        p.offset = 0
        # call assign() to start fetching the given partitions.
    consumer.assign(partitions)

consumer.subscribe(['catserver-vodafone-cz-catalog'], on_assign=my_on_assign)
#consumer.subscribe(catalogTopicPartition, on_assign=my_on_assign)

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
    op = gzip.open("output.gz", "wt")
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
        catalogTopicPartition = [TopicPartition("catserver-vodafone-cz-catalog", 0)]
        lastMsgToRead = consumer.get_watermark_offsets(catalogTopicPartition[0])
        lastMMsgToReadTest = 10
        import pdb; pdb.set_trace()
        config = yaml.load(open('exporter_cfg.yaml', 'r'), Loader=yaml.FullLoader)
        s = json.dumps(OrderedDict([("id", id.decode("utf-8")), ("type", type), ("offset", msg.offset()), ("content", message)]))
        op.write(s + "\n")

if __name__ == '__main__':
    get_data()
