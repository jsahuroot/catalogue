import datetime
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
OUTFILE_MAP = {}
AVAILABLE_IDS = set([])
AVAILABLE_IDS_FIRST = set([])
#AVAILABLE_IDS_FILE = 'available_ids.data'
AVAILABLE_IDS_FILE = ''
UNIQUE_IDS = {}
DATE_TODAY = datetime.datetime.now().date()
TIME_STAMP = round(time.time() * 1000)
AVAILABLE_IDS_FILE = "available_ids-%s.data" % (TIME_STAMP)

class CatalogExporter(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)
        self.catalogId = self.options.cust_id
        print(self.catalogId)
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
        #output_file = os.path.join(self.out_dir, "catalog-%s-%s.gz" % (self.catalogId, round(time.time() * 1000)))
        output_file = os.path.join(self.out_dir, "catalog-%s-%s.gz" % (self.catalogId, TIME_STAMP))
        output_file_fp = OUTFILE_MAP[output_file]
        available_ids_file = os.path.join(self.out_dir, AVAILABLE_IDS_FILE)
        catalogTopicPartition = TopicPartition("catserver-%s-catalog" % self.catalogId, 0, 0)
        lastMsgToRead = self.consumer.get_watermark_offsets(catalogTopicPartition)[1] - 1
        current_offset = 0
        print(lastMsgToRead)
        record_list = []
        ps = self.consumer.list_topics("catserver-%s-catalog" % self.catalogId)
        print(ps.topics)
        cnt, rec, offer_content = 0, 0, 0
        batch_size = 500
        first_pass = True
        while current_offset < lastMsgToRead:  
            try:
                msg_list = self.consumer.consume(batch_size, 100)
            except SerializerError as e:
                print("Message deserialization failed for {}: {}".format(msg_list, e))
                raise SerializerError

            for msg in msg_list:
                if msg.error():
                    print("AvroConsumer error: {}".format(msg.error()))
                    return
                if msg.key() is None:
                    print("Key is None for offset {}".format(msg.offset()))

                msg_id = msg.key().decode("utf-8")
                if first_pass:
                    existing_offset = UNIQUE_IDS.get(msg_id)
                    if existing_offset is None:
                        if msg.value() is not None:
                            UNIQUE_IDS[msg_id] = msg.offset()
                            #message, content_type = self.decode_avro(msg.value())
                            #if content_type in ("VodOffer", "LinearBlock"):
                            #    self.parse_availability_first(content_type, message)
                            self.logger.info("msg_id: {}, msg_offset: {} added for the first time in dict".format(msg_id, msg.offset()))
                    elif msg.value() is None:
                        del UNIQUE_IDS[msg_id]
                        self.logger.info("Content is null for msg_id: {}, msg_offset: {}, existing offset: {}. Remove from dict".format(msg_id, msg.offset(), existing_offset))
                    else:
                        UNIQUE_IDS[msg_id] = msg.offset()
                        #message, content_type = self.decode_avro(msg.value())
                        #if content_type in ("VodOffer", "LinearBlock"):
                        #    self.parse_availability_first(content_type, message)
                else:
                    existing_offset = UNIQUE_IDS.get(msg_id)
                    if existing_offset is None:
                        self.logger.info("msg_id: {}, msg_offset: {} not present, msg.value(): {}".format(msg_id, msg.offset(), msg.value()))
                    elif existing_offset == msg.offset():
                        message, content_type = self.decode_avro(msg.value())
                        if content_type in ("VodOffer", "LinearBlock"):
                            offer_content = offer_content + 1
                            self.parse_availability(available_ids_file, content_type, message)
                        else:
                            rec = rec + 1
                            record_list.append(OrderedDict([("id", msg_id), ("type", content_type), ("offset", msg.offset()), ("content", message)]))
                            if len(record_list) >= 10000:
                                output_file_fp.write('\n'.join(json.dumps(record) for record in record_list) + "\n")
                                del record_list[:]

                current_offset = msg.offset()
                cnt = cnt + 1
                self.logger.info("Continuing to the processes. Currently at offset {}/{}".format(current_offset, lastMsgToRead))
            if first_pass and current_offset == lastMsgToRead:
                first_pass = False
                current_offset = 0
                print("No of records: %d first pass" % cnt)
                cnt = 0
                self.consumer.seek(catalogTopicPartition)
        self.consumer.close()

        if len(record_list) > 0:
            output_file_fp.write('\n'.join(json.dumps(record) for record in record_list) + "\n")

        '''
        output_file_fp.close()
        available_ids_file = os.path.join(self.out_dir, AVAILABLE_IDS_FILE)
        print(os.path.exists(available_ids_file))
        if os.stat(output_file).st_size > 0 and os.path.exists(available_ids_file):
            s3_upload_status = s3_utils.upload_file_to_s3(output_file, self.s3Location, self.logger)
            available_ids_file_upload = s3_utils.upload_file_to_s3(AVAILABLE_IDS_FILE, self.s3Location, self.logger)
            print("catalogue upload: %d, available_ids_file_upload: %d" % (s3_upload_status, available_ids_file_upload))
            self.logger.info("catalogue upload: {}, available_ids_file_upload: {}".format(s3_upload_status, available_ids_file_upload))
        '''
        print("size in Bytes: %d" % sys.getsizeof(UNIQUE_IDS))
        print("unique records:  %d" % len(UNIQUE_IDS))
        print("Last msg offset: %d" % lastMsgToRead)
        print("No of records: %d" % cnt)
        print("No of written records: %d" % rec)
        print("No of offer content records: %d" % offer_content)
        print("No of available IDs: %d" % len(AVAILABLE_IDS))

    def parse_availability(self, filename, content_type, content_info):
        #filename = os.path.join(self.out_dir, AVAILABLE_IDS_FILE)
        fp = OUTFILE_MAP.get(filename)
        #if not fp:
        #    fp = open(filename, 'w')
        #    OUTFILE_MAP[filename] = fp
        if not content_info:
            self.logger.info("Content Empty for ID: %s" % self.sk)
            return
        if not content_info:
            self.logger.info("Content Empty for Record: %s" % json_record)
            return
        if content_type == "LinearBlock":
            offers = self.get_value(content_info, "offers", [])
            for offer in offers:
                work_id = self.get_value(offer, "workId", "")
                series_id = self.get_value(offer, "seriesId", "")
                end_time = self.get_value(offer, "endTime", "")
                if end_time:
                    end_date = datetime.datetime.strptime(end_time.split('T')[0], '%Y-%m-%d').date()
                else:
                    end_date = DATE_TODAY
                if work_id and end_date >= DATE_TODAY:
                    fp.write('%s\n' % work_id)
                    AVAILABLE_IDS.add(work_id)
                if series_id and end_date >= DATE_TODAY:
                    fp.write('%s\n' % series_id)
                    AVAILABLE_IDS.add(series_id)
        else:
            work_id = self.get_value(content_info, "workId", "")
            series_id = self.get_value(content_info, "seriesId", "")
            end_time = self.get_value(content_info, "endTime", "")
            if end_time:
                end_date = datetime.datetime.strptime(end_time.split('T')[0], '%Y-%m-%d').date()
            else:
                end_date = DATE_TODAY
            if work_id and end_date >= DATE_TODAY:
                fp.write('%s\n' % work_id)
                AVAILABLE_IDS.add(work_id)
            if series_id and end_date >= DATE_TODAY:
                fp.write('%s\n' % series_id)
                AVAILABLE_IDS.add(series_id)

    def get_value(self, d, key, default):
        value = d.get(key)
        if value == None:
            value = default
        return value

    def create_files(self):
        try:
            catalogue_output_file = os.path.join(self.out_dir, "catalog-%s-%s.gz" % (self.catalogId, TIME_STAMP))
            catalogue_output_file_p = gzip.open(catalogue_output_file, "wt")
            OUTFILE_MAP[catalogue_output_file] = catalogue_output_file_p 
        except Exception as e:
            print(e)
            return -1

        try:
            available_ids_file = os.path.join(self.out_dir, AVAILABLE_IDS_FILE)
            available_ids_file_p = open(available_ids_file, 'w')
            OUTFILE_MAP[available_ids_file] = available_ids_file_p
        except Exception as e:
            print(e)
            return -1
        return 0


    def close(self):
        for outf in OUTFILE_MAP.values():
            outf.close()

    def upload(self):
        for key in OUTFILE_MAP:
            s3_upload_status = s3_utils.upload_file_to_s3(key, self.s3Location, self.logger)
            print("s3_upload_status: %d" % (s3_upload_status))
            self.logger.info("s3 upload status: {}".format(s3_upload_status))

    def run_main(self):
        if(self.create_files() == 0):
            self.generate_catalog()
            self.close()
            self.upload()

    def set_options(self):
        config_file = os.path.join(self.system_dirs.VTV_ETC_DIR, 'exporter_cfg.yaml')
        self.parser.add_option('-c', '--config-file', default=config_file, help='configuration file')
        self.parser.add_option('-t', '--cust-id', help="name of the customer")

    def cleanup(self):
        self.move_logs(self.out_dir, [('.', '*log')])


if __name__ == '__main__':
    vtv_task_main(CatalogExporter)
