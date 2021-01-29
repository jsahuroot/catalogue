#!/usr/bin/env python
import os
import json
import jsonschema
import time
import sys
from vtv_utils import make_dir_list, remove_dir_files, copy_file_list, copy_file, move_file_forcefully, vtv_send_html_mail_2
from vtv_task import VtvTask, vtv_task_main
from s3_utils import download_s3_file, get_s3_file_by_wildcard

class DigitalSmithFetchException(Exception):
    def __init__(self, msg):
        self.msg = msg

class DigitalSmithFetcher(VtvTask):
    ''' The fetcher '''
    def __init__(self):
        ''' '''
        VtvTask.__init__(self)
        self.config = self.load_config()
        process_dir = self.config["process_dir"]
        self.PROCESS_DIR = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, process_dir)
        self.DATA_DIR = os.path.join(self.PROCESS_DIR, self.config["data_dir"])
        self.REPORTS_DIR = os.path.join(self.system_dirs.VTV_REPORTS_DIR, process_dir)
        if os.path.exists(self.DATA_DIR):
            remove_dir_files(self.DATA_DIR, self.logger)
        create_list = [self.PROCESS_DIR, self.DATA_DIR, self.REPORTS_DIR]
        make_dir_list(create_list, self.logger)
        self.options.report_file_name = os.path.join(self.REPORTS_DIR, '%s.html' % self.name_prefix)
        self.tgzs_to_keep = 1
        self.tar_file_prefix = self.config['tar_file_prefix']
        self.s3path = ''
        self.available_ids_path = ''
        self.tar_files = []
        self.file_name = self.config["input_catalog"]
        self.available_ids_file = self.config["available_ids"]
	aux_info = {
            "PATH": "s3://ds-veveo-voice/vodafone/pt-staging/datagen/nlu_catalogs/catalog-*.gz",
            "AVAILABLE_IDS_PATH": "s3://ds-veveo-voice/vodafone/pt-staging/datagen/nlu_catalogs/available_ids-*.data",
      		"APPLICATIONS_PATH": "s3://vodafone-voice-digitalsmiths.net/stage/pt-PT/datagen/navigation_data/navigation_targets_*",
      		"SETTINGS_PATH": "s3://vodafone-voice-digitalsmiths.net/stage/pt-PT/datagen/settings_data/settings_*"
    	}
        '''
        aux_info = {
            "PATH": "s3://vodafone-voice-digitalsmiths.net/stage/de-DE/datagen/catalogs/catalog-*.gz",
            "APPLICATIONS_PATH": "s3://vodafone-voice-digitalsmiths.net/stage/de-DE/datagen/navigation_data/navigation_targets_*",
            "SETTINGS_PATH": "s3://vodafone-voice-digitalsmiths.net/stage/de-DE/datagen/settings_data/settings_*"
        }
        aux_info = {
            "PATH": "s3://ds-veveo-voice/vodafone/de-staging/datagen/nlu_catalogs/catalog-*.gz",
            "AVAILABLE_IDS_PATH": "s3://ds-veveo-voice/vodafone/de-staging/datagen/nlu_catalogs/available_ids-*.data",
            "APPLICATIONS_PATH": "s3://vodafone-voice-digitalsmiths.net/stage/de-DE/datagen/navigation_data/navigation_targets_*",
            "SETTINGS_PATH": "s3://vodafone-voice-digitalsmiths.net/stage/de-DE/datagen/settings_data/settings_*"
        }
        aux_info = {
            "PATH": "s3://ds-veveo-voice/vodafone/de-staging/datagen/nlu_catalogs/catalog-*.gz",
            "AVAILABLE_IDS_PATH": "s3://ds-veveo-voice/vodafone/de-staging/datagen/nlu_catalogs/available_ids-*.data",
            "APPLICATIONS_PATH": "s3://ds-veveo-voice/vodafone/de-staging/datagen/navigation_targets_*",
            "SETTINGS_PATH": "s3://ds-veveo-voice/vodafone/de-staging/datagen/settings_*"
        }
        '''
        #if 'AUX_INFO' in os.environ:
        if aux_info is not None:
            #aux_info = eval(os.environ['AUX_INFO'])
            self.s3path = aux_info.get('PATH', '')
            self.available_ids_path = aux_info.get('AVAILABLE_IDS_PATH', '')
            self.channel_s3path = aux_info.get('CHANNELS_PATH', '')
            if self.channel_s3path != '':
                self.channel_file_name = self.config["input_channel_file"]
                self.is_validate = self.config["VALIDATION_FLAG"]
                if self.is_validate:
                    self.email_recipiants = self.config["EMAIL_LIST"] 
                    self.sending_mail_retries = self.config["MAIL_RETRIES"]
                    self.sender = self.config["SENDER"]
                    self.server = self.config["SERVER"]
                    self.channel_schema_file = self.config["CHANNEL_SCHEMA_FILE"]

    def load_config(self):
        ''' Load config '''
        config = {}
        with open(self.options.config_file) as json_file:
            config = json.load(json_file)
        return config

    def download_data(self):
        status, filenames = get_s3_file_by_wildcard(self.s3path, self.logger)
        if filenames:
            if status:
                self.logger.error("Failed to find files in %s" %self.s3path)
                raise DigitalSmithFetchException("Failed to find files in %s" %self.s3path)
            filenames.sort()
            file_name = filenames[-1]
            status, path = download_s3_file(file_name, self.PROCESS_DIR, self.logger)
            if status:
                self.logger.error("Failed to download file %s" %file_name)
                raise DigitalSmithFetchException("Failed to download file %s" %file_name)
            new_path = path.replace(os.path.basename(path),self.file_name)
            self.logger.info("Decompressing %s into %s", path, new_path)
            cmd = 'gzip -dv --stdout {gz_file} > {outfile}'
            self.start_process(
                    "decompress_%s" % path,
                    cmd.format(gz_file=path, outfile=new_path)
                )
            copy_file_list([new_path], self.DATA_DIR, self.logger)
            self.tar_files.append(os.path.basename(new_path))
        else:
            raise DigitalSmithFetchException("No files in %s" %self.s3path)

    def download_available_id_data(self):
        status, filenames = get_s3_file_by_wildcard(self.available_ids_path, self.logger)
        if filenames:
            if status:
                self.logger.error("Failed to find files in %s" %self.available_ids_path)
                raise DigitalSmithFetchException("Failed to find files in %s" %self.available_ids_path)
            filenames.sort()
            file_name = filenames[-1]
            status, path = download_s3_file(file_name, self.PROCESS_DIR, self.logger)
            if status:
                self.logger.error("Failed to download file %s" %file_name)
                raise DigitalSmithFetchException("Failed to download file %s" %file_name)
            new_path = os.path.join(self.PROCESS_DIR, self.available_ids_file)
            os.rename(path, new_path)
            copy_file_list([new_path], self.DATA_DIR, self.logger)
        else:
            raise DigitalSmithFetchException("No files in %s" %self.available_ids_path)

    def download_channels_data(self):
        status, filenames = get_s3_file_by_wildcard(self.channel_s3path, self.logger)
        if filenames:
            if status:
                self.logger.error("Failed to find files in %s" %self.channel_s3path)
                raise DigitalSmithFetchException("Failed to find files in %s" %self.channel_s3path)
            filenames.sort()
            file_name = filenames[-1]
            status, path = download_s3_file(file_name, self.PROCESS_DIR, self.logger)
            if status:
                self.logger.error("Failed to download file %s" %file_name)
                raise DigitalSmithFetchException("Failed to download file %s" %file_name)
            new_path = path.replace(os.path.basename(path),self.channel_file_name)
            self.logger.info("Moving %s into %s", path, new_path)
            move_file_forcefully(path, new_path)
            copy_file_list([new_path], self.DATA_DIR, self.logger)
            self.tar_files.append(os.path.basename(new_path))
            if self.is_validate:
                self.validate_schema(new_path, file_name, self.email_recipiants)
        else:
            raise DigitalSmithFetchException("No files in %s" %self.channel_s3path)

    def validate_schema(self, filename, s3path, email_list):
        if not self.is_validate:
            return
        file_name = os.path.basename(filename)
        schema_file = self.channel_schema_file
        self.logger.info('Validating file %s ' % filename)
        self.logger.info('Schema file %s ' % schema_file)
        schema=open(schema_file).read()
        json_schema = json.loads(schema)
        self.logger.info('Validating file %s ' % filename)
        error = ''
        try:
            fp = open(filename, 'rb')
            for line in fp:
                line = line.decode('utf-8').strip()
                if not line:
                    continue
                data=json.loads(line)
                jsonschema.validate(data, json_schema)
        except jsonschema.ValidationError as e:
            error = 'json schema validation error: %s' % e
        except jsonschema.SchemaError as e:
            error = 'json schema error: %s' % e
        except ValueError as e:
            error = 'invalid json data: %s' % e
        if error:
            self.send_error_mail(s3path, error, email_list, line)
            sys.exit(error)
        else:
            self.send_success_mail(s3path, email_list)

    def send_error_mail(self, filename, error, email_list, line):
        subject = "VALIDATION FAILED FOR VERIZON FIOS CHANNEL AKA FILE"
        mail_sent = False
        html = '<table border=\"1\" style=\"width:100\%\"><tr><th>' + '</th><th>'.join(["File", "Reason", "Record"]) + '</th></tr>'
        html += '<tr><td>' + '</td><td>'.join([filename, error, line]) + '</td></tr>'
        body = subject + "<br><br>" + html
        for i in xrange(self.sending_mail_retries):
            try:
                vtv_send_html_mail_2(self.logger, self.server, self.sender, email_list, subject, None, body, None)
                mail_sent = True
                break
            except:
                self.logger.info("Failed to send mail, retrying in 1 minute")
                time.sleep(60)
            if mail_sent:
                self.logger.info("mail sent successfully")
            else:
                self.logger.error("Error sending mail server:%s, sender: %s, recipients: %s" % (self.server, self.sender, email_list))

    def send_success_mail(self, filename, email_list):
        subject = "VALIDATION SUCCESSFULL FOR VERIZON FIOS CHANNEL AKA FILE"
        mail_sent = False
        html = '<table border=\"1\" style=\"width:100\%\"><tr><th>' + '</th><th>'.join(["File", "Status"]) + '</th></tr>'
        html += '<tr><td>' + '</td><td>'.join([filename, "SUCCESS"]) + '</td></tr>'
        body = subject + "<br><br>" + html
        for i in xrange(self.sending_mail_retries):
            try:
                vtv_send_html_mail_2(self.logger, self.server, self.sender, email_list, subject, None, body, None)
                mail_sent = True
                break
            except:
                self.logger.info("Failed to send mail, retrying in 1 minute")
                time.sleep(60)
            if mail_sent:
                self.logger.info("mail sent successfully")
            else:
                self.logger.error("Error sending mail server:%s, sender: %s, recipients: %s" % (self.server, self.sender, email_list))

    def cleanup(self):
        path_suffix_list = [('.', '%s*.log' % self.script_prefix)]
        self.move_logs(self.PROCESS_DIR, path_suffix_list)
        self.remove_old_dirs(self.PROCESS_DIR, self.logs_dir_prefix,
                             self.log_dirs_to_keep, check_for_success=False)

    def set_options(self):
        config_file = os.path.join(self.system_dirs.VTV_ETC_DIR, 'digitalsmith_fetcher.json')
        self.parser.add_option('-c', '--config-file', default=config_file,
                               help='configuration file')
        self.parser.add_option('-s', '--section', default='')
        self.parser.add_option("--lang", default='' ,help="language data to be used")

    def run_main(self):
        self.download_data()
        self.download_available_id_data()
        if self.channel_s3path != '':
            self.download_channels_data()

        self.archive_data_files(
            self.PROCESS_DIR, self.DATA_DIR,
            self.tar_files, self.tar_file_prefix,
            tgz_creation_only=True
        )

if __name__ == "__main__":
    vtv_task_main(DigitalSmithFetcher)

