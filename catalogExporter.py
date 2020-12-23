import io
import struct
import json
import gzip
import yaml
from collections import OrderedDict

from avro.io import BinaryDecoder, DatumReader
from confluent_kafka import Consumer
from confluent_kafka import TopicPartition
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError

MAGIC_BYTES = 0

class CatalogExporter():
    def __init__(self):
        self.catalogId = 'vodafone-cz'
        self.config = yaml.load(open('exporter_cfg.yaml', 'r'), Loader=yaml.FullLoader)[self.catalogId]
        self.consumer = self.init_consumer()


    def init_consumer(self):
        bootstrap_server = self.config['bootstrap-server']
        schema_url = self.config['schema-registery-url']
        # KAFKA BROKER URL
        consumer = Consumer({
            'bootstrap.servers': bootstrap_server, 
            'group.id': 'consumer-test',
            'auto.offset.reset': 'earliest'
        })

        # SCHEMA URL
        self.register_client = CachedSchemaRegistryClient(url=schema_url)
        consumer.subscribe(['catserver-%s-catalog' % self.catalogId], on_assign=self.my_on_assign)
        return consumer

    def my_on_assign(self, consumer, partitions):
        for p in partitions:
            # some starting offset, or use OFFSET_BEGINNING, et, al.
            # the default offset is STORED which means use committed offsets, and if
            # no committed offsets are available use auto.offset.reset config (default latest)
            p.offset = 0
            # call assign() to start fetching the given partitions.
        consumer.assign(partitions)

    def decode_avro(self, payload):
        magic, schema_id = struct.unpack('>bi', payload[:5])

        # Get Schema registry
        # Avro value format
        if magic == MAGIC_BYTES:
            schema = self.register_client.get_by_id(schema_id)
            reader = DatumReader(schema)
            output = BinaryDecoder(io.BytesIO(payload[5:]))
            decoded = reader.read(output)
            return decoded, schema.name
        # no magic bytes, something is wrong
        else:
            raise ValueError

    def generate_catalog(self):
        op = gzip.open("output.gz", "wt")
        catalogTopicPartition = [TopicPartition("catserver-vodafone-cz-catalog", 0)]
        lastMsgToRead = self.consumer.get_watermark_offsets(catalogTopicPartition[0])
        lastMsgToReadTest = 10
        current_offset = 0
        while current_offset < lastMsgToReadTest:  
            try:
                msg = self.consumer.poll(10)
            except SerializerError as e:
                print("Message deserialization failed for {}: {}".format(msg, e))
                raise SerializerError

            if msg.error():
                print("AvroConsumer error: {}".format(msg.error()))
                return

            id = msg.key()
            if msg.value() is None:
                message = None
                content_type = "PROGRAM"
            else:
                message, content_type = self.decode_avro(msg.value())
                s = json.dumps(OrderedDict([("id", id.decode("utf-8")), ("type", content_type), ("offset", msg.offset()), ("content", message)]))
                op.write(s + "\n")
            current_offset = msg.offset()

if __name__ == '__main__':
    exporterObject = CatalogExporter()
    exporterObject.generate_catalog()
