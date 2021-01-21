import io
import struct
import json
import gzip
import yaml
import os
import sys
import time
from collections import OrderedDict

from avro.io import BinaryDecoder, DatumReader
from confluent_kafka import Consumer
from confluent_kafka import TopicPartition
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError

import s3_utils
from vtv_task import vtv_task_main, VtvTask
from vtv_utils import make_dir

MAGIC_BYTES = 0

ids_dict = {}

class CatalogExporter(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        self.catalogId = self.options.cust_id
        self.config = yaml.load(open(self.options.config_file, 'r'), Loader=yaml.FullLoader)[self.catalogId]
        self.consumer = self.init_consumer()
        self.out_dir = self.config['out_dir']
        make_dir(self.out_dir)
        self.s3Location = self.config['s3Location']


    def init_consumer(self):
        bootstrap_server = self.config['bootstrap-server']
        schema_url = self.config['schema-registery-url']
        # KAFKA BROKER URL
        consumer = Consumer({
            'bootstrap.servers': bootstrap_server, 
            'group.id': 'catalog-export-%s' %self.catalogId,
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
        output_file = os.path.join(self.out_dir, "catalog-%s-%s.gz" % (self.catalogId, round(time.time() * 1000)))
        output_file_fp = gzip.open(output_file, "wt")
        catalogTopicPartition = TopicPartition("catserver-%s-catalog" % self.catalogId, 0, 0)
        lastMsgToRead = self.consumer.get_watermark_offsets(catalogTopicPartition)[1] - 1
        print(lastMsgToRead)
        record_list = []
        cnt, rec, older_offsets, value_none = 0, 0, 0, 0
        batch_size = 500
        current_offset = lastMsgToRead - batch_size + 1
        while current_offset >= 0: 
            catalogTopicPartition = TopicPartition("catserver-%s-catalog" % self.catalogId, 0, current_offset)
            self.consumer.seek(catalogTopicPartition)
            try:
                msg_list = self.consumer.consume(batch_size, 100)
            except SerializerError as e:
                print("Message deserialization failed for {}: {}".format(msg_list, e))
                raise SerializerError

            for msg in reversed(msg_list):
                if msg.error():
                    print("AvroConsumer error: {}".format(msg.error()))
                    return
                if msg.value() is not None:
                    msg_id = msg.key().decode("utf-8")
                    #print(msg.offset(),msg_id)
                    offset_val = ids_dict.get(msg_id)
                    if offset_val is None:
                        ids_dict[msg_id] = msg.offset()
                        message, content_type = self.decode_avro(msg.value())
                        rec = rec + 1
                        record_list.append(OrderedDict([("id", msg_id), ("type", content_type), ("offset", msg.offset()), ("content", message)]))
                        if len(record_list) >= 10000:
                            output_file_fp.write('\n'.join(json.dumps(record) for record in record_list) + "\n")
                            del record_list[:]
                    else:
                        older_offsets = older_offsets + 1
                        self.logger.info("msg_id: {}, latest offset_val in dict: {}, older msg_offset: {}".format(msg_id, offset_val, msg.offset()))
                else:
                    value_none = value_none + 1
                    self.logger.info("For msg_id: {} msg.value() is None".format(msg_id))
                #count  = msg.offset()
                cnt = cnt + 1
            current_offset = current_offset - batch_size
            self.logger.info("Continuing to the processes. Currently at offset {}/{}".format(current_offset, lastMsgToRead))
                #catalogTopicPartition = TopicPartition("catserver-%s-catalog" % self.catalogId, 0, 0)
                #self.consumer.seek(catalogTopicPartition)
        self.consumer.close()

        if len(record_list) > 0:
            output_file_fp.write('\n'.join(json.dumps(record) for record in record_list) + "\n")

        output_file_fp.close()
        if os.stat(output_file).st_size > 0:
            print(s3_utils.upload_file_to_s3(output_file, self.s3Location, self.logger))
        print("size in Bytes: %d" % sys.getsizeof(ids_dict))
        print("unique records:  %d" % len(ids_dict))
        print("Last msg offset: %d" % lastMsgToRead)
        print("No of records: %d" % cnt)
        print("No of written records: %d" % rec)
        print("No of empty records: %d" % value_none)
        print("No of older_offset entries: %d" % older_offsets)

    def run_main(self):
        self.generate_catalog()

    def set_options(self):
        config_file = os.path.join(self.system_dirs.VTV_ETC_DIR, 'exporter_cfg.yaml')
        self.parser.add_option('-c', '--config-file', default=config_file, help='configuration file')
        self.parser.add_option('-t', '--cust-id', help="name of the customer")

    def cleanup(self):
        self.move_logs(self.out_dir, [('.', '*log')])


if __name__ == '__main__':
    vtv_task_main(CatalogExporter)
