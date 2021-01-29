#!/usr/bin/env python

import os
import io
import sys
import json
import copy
import glob
import string
import datetime
import unidecode
import codecs

from data_schema import TVPUB_MC_SCHEMA, get_schema
from genericFileInterfaces import writeSingleRecord, fileIterator
from vtv_utils import get_compact_traceback, make_dir, VTV_ETC_DIR, move_file_list, append_file
from vtv_task import VtvTask, vtv_task_main
from vtv_dir import VTV_DATAGEN_DIR, VTV_DATAGEN_CURRENT_DIR
from StringUtil import parseMultiValuedVtvString, parseMultiValuedPairedVtvString
from vtv_utils import make_dir_list, remove_dir_files, copy_file_list

from data_constants import CONTENT_TYPE_TVSERIES, CONTENT_TYPE_MOVIE, CONTENT_TYPE_TVVIDEO, \
                           CONTENT_TYPE_EPISODE, CONTENT_TYPE_PERSON, CONTENT_TYPE_CHANNEL, \
                           CONTENT_TYPE_GENRE, CONTENT_TYPE_TEAM, CONTENT_TYPE_SPORT, CONTENT_TYPE_TOURNAMENT, \
                           CONTENT_TYPE_LANGUAGE

from data_schema import VALUE_SEPARATOR, MOVIE_CAST_PAIR_SEPARATOR, get_schema, TVPUB_MC_SCHEMA
from guidPairLoaderUtils import guidloadGuidPairList

GENDER_MAP = {
    'MALE'    : 'M',
    'FEMALE'  : 'F',
}

'''
VT_MAPPER = {
    "SERIES"              : CONTENT_TYPE_TVSERIES,
    "PROGRAM"             : CONTENT_TYPE_TVVIDEO,
    "STATION"             : CONTENT_TYPE_CHANNEL,
    "SPORTS_ORGANIZATION" : CONTENT_TYPE_SPORT,
    "SPORTS_TEAM"         : CONTENT_TYPE_TEAM,
    "VOD_PROVIDER"        : CONTENT_TYPE_CHANNEL
}
'''

VT_MAPPER = {
    "Series"              : CONTENT_TYPE_TVSERIES,
    "Program"             : CONTENT_TYPE_TVVIDEO,
    "Station"             : CONTENT_TYPE_CHANNEL,
    "SportsOrganization" : CONTENT_TYPE_SPORT,
    "SportsTeam"         : CONTENT_TYPE_TEAM,
    "VodProvider"        : CONTENT_TYPE_CHANNEL
    }


ALLOWED_TYS = set([ CONTENT_TYPE_MOVIE, CONTENT_TYPE_TVSERIES, CONTENT_TYPE_TVVIDEO, \
                    CONTENT_TYPE_EPISODE, CONTENT_TYPE_PERSON, CONTENT_TYPE_CHANNEL, \
                    CONTENT_TYPE_GENRE, CONTENT_TYPE_TEAM, CONTENT_TYPE_SPORT, CONTENT_TYPE_TOURNAMENT ])

GI_INDEX = 0
TI_INDEX = 1
VT_INDEX = 2
SK_INDEX = 4
EP_INDEX = 5
PI_INDEX = 7
RY_INDEX = 8
OD_INDEX = 9
AK_INDEX = 10
DE_INDEX = 14
DI_INDEX = 16
PR_INDEX = 17
HO_INDEX = 18
GE_INDEX = 22
LL_INDEX = 25
SN_INDEX = 27
EN_INDEX = 28
MI_INDEX = 29
PA_INDEX = 32
SP_INDEX = 35
CS_INDEX = 43
FD_INDEX = 50
TG_INDEX = 57

SCHEMA = copy.copy(TVPUB_MC_SCHEMA)
SX_INDEX = len(SCHEMA)
SCHEMA['Sx'] = SX_INDEX
BD_INDEX = len(SCHEMA)
SCHEMA['Bd'] = BD_INDEX

# Global Dictionaries
OUTFILE_MAP = {}
TVSERIES_IDS = set([])
PERSON_IDS = set([])
ALL_SKS = set([])
GUID_MERGE = {}
KG_GID_VT = {}
SPORTS_GUID_MERGE = {}
CHANNEL_GUID_MERGE = {}
CUSTOMER_CHANNEL_AKA = {}
KG_MERGE_SCHEMA = get_schema(['Gi', 'Sk'])
ALL_GENRES = {}
GENRES_AKA = {}
COLLECTION_IDS_FILE = 'collection_ids.data'
ENG_LL = 'english'
ENG_LL_ID = 'HYDRALANG1'
dummy_sp_id_prefix = 'HYDRASP'
dummy_sp_id_count = 0
title_sp_id_hash = {}
DATE_TODAY = datetime.datetime.now().date()
class HydraRecord:
    def __init__(self, _logger, json_record, process_dir, lang, parse_all, publish_alt_ids):
        self.logger = _logger
        self.json_record = json_record # For Reference
        self.hosts = []
        self.producers = []
        self.directors = []
        self.cast = []
        self.genres = []
        self.publish_alt_ids = publish_alt_ids
        self.lang_list = lang.split(',')#languages in order of priority
        self.lang = self.lang_list[0]
        self.parse_all = parse_all
        self.PROCESS_DIR = process_dir
        not_needed_string = '$& !+-*/^%#@~{}'
        replace_string = '.' * len(not_needed_string)
        self.normalizer_table = string.maketrans(not_needed_string, replace_string)
        self.parse(json_record)

    def get_value_list(self, d, key_list, default_value_list, parse_all):
        value = ""
        if parse_all:
            value = self.get_any_value_list(d, key_list, default_value_list)
            value = value.replace('\r', '').replace('\n', '')
            return value
        for key, default in zip(key_list, default_value_list):
            value = d.get(key)
            if value == None:
                value = default
            d = value
        value = value.replace('\r', '').replace('\n', '')
        return value

    def get_any_value_list(self, d, key_list, default_value_list):
        value = ""
        for i, key in enumerate(key_list):
            default = default_value_list[i]
            value = d.get(key)
            if i != len(key_list) - 1:
                if value == None:
                    value = default
            else:
                if value == None:
                    for val in d.values():
                        value = val
                        if value:
                            break
                    if value == None:
                        value = default
            d = value
        return value

    def get_value(self, d, key, default):
        value = d.get(key)
        if value == None:
            value = default
        return value

    def normalize_sk(self, sk):
        norm_sk = sk.encode('utf8').translate(self.normalizer_table).decode('utf8').strip()
        return norm_sk

    def normalize_channel_sk(self,sk):
        norm_sk = unidecode.unidecode(sk)
        if norm_sk != sk:
            self.logger.info("Normalizing channel gid from %s to %s" %(sk, norm_sk))
        return norm_sk

    def parse(self, json_record):
        self.orig_sk = json_record["id"]
        self.sk = self.normalize_sk(self.orig_sk)
        self.alt_ids = []

        self.is_vod_channel = False
        self.is_channel = False
        if json_record["type"] == 'VOD_PROVIDER':
            self.is_vod_channel = True
            self.json_record = json_record
        if json_record["type"] == 'STATION':
            self.is_channel = True
            self.json_record = json_record
            self.sk = self.normalize_channel_sk(self.sk)

        self.vt = VT_MAPPER.get(json_record["type"], json_record["type"]).lower()

        self.write = True

        if self.vt not in ALLOWED_TYS:
            return

        content_info = json_record["content"]
        self.rovi_id = ''
        for alt_id_info in self.get_value(content_info, "altIds", []):
            ns = alt_id_info.get("ns")
            if ns in ('sourceSeriesId', 'tivo:sourceProgramId'):
                self.rovi_id = alt_id_info.get("id")
            if self.publish_alt_ids and ns in self.publish_alt_ids:
                self.alt_ids.append('%s#%s' % (ns, alt_id_info.get("id") ))

        self.title = ''
        for lang in self.lang_list:
            if not self.title:
                self.title = self.get_value_list(content_info, ["title", lang], [{}, ""], self.parse_all)
            if not self.title:
                self.title = self.get_value_list(content_info, ["name", lang], [{}, ""], self.parse_all)
            if self.title:
                self.lang = lang
                break
        self.description = self.get_value_list(content_info, ["description", self.lang], [{}, ""], self.parse_all)
        self.release_year = self.get_value(content_info, "releaseYear", "")
        if self.release_year:
            self.release_year = str(self.release_year)
        self.release_date = self.get_value(content_info, "releaseDate", "")

        self.parse_credits(self.get_value(content_info, "credits", {}))
        self.parse_genres(self.get_value(content_info, "genres", {}))

        if self.vt == CONTENT_TYPE_PERSON:
            self.parse_person_special(content_info)
        elif self.vt == CONTENT_TYPE_EPISODE:
            self.parse_episode_special(content_info)
        elif self.vt == CONTENT_TYPE_CHANNEL:
            self.parse_channel_special(content_info)
        elif self.vt == CONTENT_TYPE_TEAM:
            self.parse_team_special(content_info)
        elif self.vt == CONTENT_TYPE_SPORT:
            self.parse_sport_special(content_info)
        elif self.vt == CONTENT_TYPE_GENRE:
            self.parse_genre_special(content_info)
            if self.orig_sk not in ALL_GENRES:
                ALL_GENRES[self.orig_sk] = self.title
            else:
                if self.title != ALL_GENRES[self.orig_sk]:
                    if self.orig_sk not in GENRES_AKA:
                        GENRES_AKA[self.orig_sk] = set([self.title])
                    else:
                        GENRES_AKA[self.orig_sk].add(self.title)
    '''
    def parse_availability(self, json_record):
        filename = os.path.join(self.PROCESS_DIR, AVAILABLE_IDS_FILE)
        fp = OUTFILE_MAP.get(filename)
        if not fp:
            fp = open(filename, 'w')
            OUTFILE_MAP[filename] = fp
        content_info = json_record["content"]
        if not content_info:
            self.logger.info("Content Empty for ID: %s" % self.sk)
            return
        if not content_info:
            self.logger.info("Content Empty for Record: %s" % json_record)
            return
        if json_record["type"] == "LINEAR_BLOCK":
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
    '''

    def parse_credits(self, credits_info):
        for credit_info in credits_info:
            person_id = credit_info["personId"]
            if not person_id:
                continue
            person_name = credit_info["name"]
            role = credit_info["role"]
            person_info_str = self.normalize_sk(person_id)
            if role == "DIRECTOR":
                self.directors.append(person_info_str)
            elif role == "PRODUCER":
                self.producers.append(person_info_str)
            elif role == "HOST":
                self.hosts.append(person_info_str)
            elif role == "ACTOR":
                # Actor has ROLE but since ids are not there keeping that info empty
                cast_role_info_str = ""
                self.cast.append("<>".join([person_info_str, cast_role_info_str]))

    def parse_genres(self, genres_info):
        for genre_info in genres_info:
            genre_name = self.get_value_list(genre_info, ["name", self.lang], [{}, ""], self.parse_all)
            genre_name =  genre_name.replace('{', '.')
            genre_name =  genre_name.replace('}', '.')
            if not genre_name:
                continue
            genre_id = genre_info["id"]
            if not genre_id:
                continue
            if genre_id not in ALL_GENRES:
                ALL_GENRES[genre_id] = genre_name
            else:
                if genre_name != ALL_GENRES[genre_id]:
                    if genre_id not in GENRES_AKA:
                        GENRES_AKA[genre_id] = set([genre_name])
                    else:
                        GENRES_AKA[genre_id].add(genre_name)
            self.genres.append(self.normalize_sk(genre_id))

    def parse_episode_special(self, content_info):
        self.parent = self.normalize_sk(self.get_value(content_info, "seriesId", ""))
        self.season_num = self.get_value(content_info, "seasonNum", "")
        if self.season_num:
            self.season_num = str(self.season_num)
        self.episode_num = self.get_value(content_info, "episodeNum", "")
        if self.episode_num:
            self.episode_num = str(self.episode_num)
        self.episode_title = self.get_value_list(content_info, ["episodeTitle", self.lang], [{}, ""], self.parse_all)

    def parse_person_special(self, content_info):
        self.person_name_info = content_info['personName']
        self.title = self.person_name_info["full"]
        self.birth_date = content_info['birthDate']
        self.death_date = content_info['deathDate']
        self.gender = GENDER_MAP.get(content_info['gender'])
        if 'tivo:pn.' in self.sk:
            self.rovi_id = self.sk.split("tivo:pn.")[1]

    def parse_channel_special(self, content_info):
        self.video_quality = self.get_value(content_info, "videoQuality", "")
        self.callsign = self.get_value(content_info, "callSign", "")
        self.station_group = self.get_value(content_info, "stationGroup", "")
        self.attributes = self.get_value(content_info, "attributes", [])
        for alt_id_info in self.get_value(content_info, "altIds", []):
            ns = alt_id_info.get("ns")
            if ns in ("tivo:sourceStationId", "roviSourceId"):
                self.rovi_id = alt_id_info.get("id")
        metadata = self.get_value(content_info, "extendedMetadata", {})
        self.channel_aka = []
        if metadata:
            self.channel_aka = self.get_value(metadata, "channelAKA", [])

    def parse_team_special(self, content_info):
        self.nickname = self.get_value_list(content_info, ["nickname", self.lang], [{}, ""], self.parse_all)
        self.abbreviation = self.get_value_list(content_info, ["abbreviation", self.lang], [{}, ""], self.parse_all)
        self.sport = ''
        for organization_info in self.get_value(content_info, "organizations", {}):
            org_type = organization_info.get("orgType")
            if org_type == "SPORT":
                self.sport = self.get_value_list(organization_info, ["name", self.lang], [{}, ""], self.parse_all)
        for alt_id_info in self.get_value(content_info, "altIds", []):
            ns = alt_id_info.get("ns")
            if ns == "roviOrgId":
                self.rovi_id = alt_id_info.get("id")

    def parse_sport_special(self, content_info):
        self.nickname = self.get_value_list(content_info, ["nickname", self.lang], [{}, ""], self.parse_all)
        self.abbreviation = self.get_value_list(content_info, ["abbreviation", self.lang], [{}, ""], self.parse_all)
        org_type = content_info.get("orgType")
        if org_type == "LEAGUE":
            for alt_id_info in self.get_value(content_info, "altIds", []):
                ns = alt_id_info.get("ns")
                self.vt = CONTENT_TYPE_TOURNAMENT
                if ns == "roviLeagueId":
                    self.rovi_id = alt_id_info.get("id")

    def parse_genre_special(self, content_info):
        if 'rovi:genre:' in self.sk:
            self.rovi_id = self.sk.split("rovi:genre:")[1]

    def serialize(self, program_guid_merge_fp, team_guid_merge_fp, tournament_guid_merge_fp, kg_merge_fp, sk_prefix):
        if self.vt not in ALLOWED_TYS:
            self.logger.info("ID: %s, VT: %s -- not written, VT is ignored for now" % (self.sk, self.vt))
            return

        if self.vt == CONTENT_TYPE_GENRE:
            return

        if not self.write:
            return

        if not self.title:
            self.logger.info("ID: %s, VT: %s -- not written, NO English Title" % (self.sk, self.vt))
            return

        if self.vt == CONTENT_TYPE_EPISODE and not self.parent:
            self.logger.info("ID: %s, VT: %s -- not written, episode does not have seriesId" % (self.sk, self.vt))
            return

        if self.sk in ALL_SKS:
            return

        collection_filename = os.path.join(self.PROCESS_DIR, COLLECTION_IDS_FILE)
        collection_fp = OUTFILE_MAP.get(collection_filename)
        if not collection_fp:
            collection_fp = open(collection_filename, 'w')
            OUTFILE_MAP[collection_filename] = collection_fp

        record = ['']*len(SCHEMA)
        record[GI_INDEX] = self.sk
        record[SK_INDEX] = self.sk
        record[TI_INDEX] = self.title
        record[VT_INDEX] = self.vt
        if self.alt_ids:
            fd = '<>'.join(self.alt_ids)
            record[FD_INDEX] = "%s#%s<>%s" % (sk_prefix, self.orig_sk,fd)
        else:
            record[FD_INDEX] = "%s#%s" % (sk_prefix, self.orig_sk)
        record[LL_INDEX] = ENG_LL_ID

        if self.vt in (CONTENT_TYPE_MOVIE, CONTENT_TYPE_TVSERIES, CONTENT_TYPE_EPISODE, CONTENT_TYPE_TVVIDEO):
            record[DI_INDEX] = VALUE_SEPARATOR.join(self.directors)
            record[PR_INDEX] = VALUE_SEPARATOR.join(self.producers)
            record[HO_INDEX] = VALUE_SEPARATOR.join(self.hosts)
            record[PA_INDEX] = MOVIE_CAST_PAIR_SEPARATOR.join(self.cast)
            record[RY_INDEX] = self.release_year
            record[OD_INDEX] = self.release_date
            record[GE_INDEX] = VALUE_SEPARATOR.join(self.genres)

            if 'tivo:cl' in self.sk:
                collection_fp.write('%s\n' % self.sk)

            if record[TG_INDEX]:
                record[TG_INDEX] = '%s%sOFFERS_AVAILABLE' % (record[TG_INDEX], VALUE_SEPARATOR)
            else:
                record[TG_INDEX] = 'OFFERS_AVAILABLE'

        if self.vt == CONTENT_TYPE_EPISODE:
            record[MI_INDEX] = self.parent
            record[EP_INDEX] = self.episode_title
            record[EN_INDEX] = self.episode_num
            record[SN_INDEX] = self.season_num
        elif self.vt == CONTENT_TYPE_CHANNEL:
            record[CS_INDEX] = self.callsign
            if self.video_quality or self.attributes:
                record[TG_INDEX] = VALUE_SEPARATOR.join([self.video_quality, VALUE_SEPARATOR.join(self.attributes)])
            if self.channel_aka:
                record[AK_INDEX] = VALUE_SEPARATOR.join(self.channel_aka)
            if self.is_vod_channel:
                if record[TG_INDEX]:
                    record[TG_INDEX] = '%s%sVOD_CHANNEL' % (record[TG_INDEX], VALUE_SEPARATOR)
                else:
                    record[TG_INDEX] = 'VOD_CHANNEL'
            channel_id = self.orig_sk
            # remove prefix 'st-' if present; was needed for verizon fios
            prefix = 'st-'
            if channel_id.startswith(prefix):
                channel_id = channel_id[len(prefix):]
            if channel_id and channel_id in CUSTOMER_CHANNEL_AKA.keys():
                record[AK_INDEX] = VALUE_SEPARATOR.join(CUSTOMER_CHANNEL_AKA[channel_id])
        elif self.vt == CONTENT_TYPE_PERSON:
            record[BD_INDEX] = self.birth_date
            record[SX_INDEX] = self.gender
        elif self.vt == CONTENT_TYPE_TEAM:
            if self.sport:
                sport = self.sport.lower()
                if sport in title_sp_id_hash:
                    record[SP_INDEX] = title_sp_id_hash[sport]
                else:
                    global dummy_sp_id_count
                    sp_id = "%s%s" %(dummy_sp_id_prefix, dummy_sp_id_count)
                    dummy_sp_id_count += 1
                    title_sp_id_hash[sport] = sp_id
                    record[SP_INDEX] = sp_id
            record[AK_INDEX] = VALUE_SEPARATOR.join([l for l in (self.nickname, self.abbreviation) if l])
        elif self.vt == CONTENT_TYPE_SPORT:
            self.vt = CONTENT_TYPE_GENRE
            record[VT_INDEX] = self.vt
            record[AK_INDEX] = VALUE_SEPARATOR.join([l for l in (self.nickname, self.abbreviation) if l])
        elif self.vt == CONTENT_TYPE_TOURNAMENT:
            record[AK_INDEX] = VALUE_SEPARATOR.join([l for l in (self.nickname, self.abbreviation) if l])

        if self.vt == 'tvseries':
            TVSERIES_IDS.add(self.sk)
        elif self.vt == 'person':
            PERSON_IDS.add(self.sk)

        ALL_SKS.add(self.sk)

        if True:
            vt = self.vt
            if vt == CONTENT_TYPE_SPORT:
                vt = CONTENT_TYPE_GENRE
            filename = os.path.join(self.PROCESS_DIR, '%s.data' % vt)
            fp = OUTFILE_MAP.get(filename)
            if not fp:
                fp = open(filename, 'w')
                OUTFILE_MAP[filename] = fp
            writeSingleRecord(fp, record, SCHEMA, skip_empty=True)
            if self.is_vod_channel:
                filename = os.path.join(self.PROCESS_DIR, 'vod_channels.json')
                fp = OUTFILE_MAP.get(filename)
                if not fp:
                    fp = open(filename, 'w')
                    OUTFILE_MAP[filename] = fp
                fp.write('%s\n' % self.json_record)
            if self.is_channel:
                filename = os.path.join(self.PROCESS_DIR, 'channels.json')
                fp = OUTFILE_MAP.get(filename)
                if not fp:
                    fp = open(filename, 'w')
                    OUTFILE_MAP[filename] = fp
                fp.write('%s\n' % self.json_record)

        #vt_seed_dict = GUID_MERGE.get(vt, {})
        merged_guid = GUID_MERGE.get(self.rovi_id)
        if self.vt in (CONTENT_TYPE_TEAM, CONTENT_TYPE_TOURNAMENT):
            merged_guid = SPORTS_GUID_MERGE.get(self.rovi_id, '')
        if self.vt == CONTENT_TYPE_CHANNEL:
            if self.rovi_id:
                merged_guid = CHANNEL_GUID_MERGE.get(self.rovi_id, '')
        if merged_guid:
            serialize_sk = self.sk
            if self.vt == CONTENT_TYPE_CHANNEL:
                kg_merge_record = [merged_guid, serialize_sk]
                writeSingleRecord(kg_merge_fp, kg_merge_record, KG_MERGE_SCHEMA, skip_empty=True)
            elif self.vt == CONTENT_TYPE_TEAM:
                team_guid_merge_fp.write('%s|%s|%s\n' % (self.title, serialize_sk, merged_guid))
            elif self.vt == CONTENT_TYPE_TOURNAMENT:
                tournament_guid_merge_fp.write('%s|%s|%s\n' % (self.title, serialize_sk, merged_guid))
            else:
                if merged_guid in KG_GID_VT:
                    seed_vt = KG_GID_VT[merged_guid]
                    if self.vt != CONTENT_TYPE_TVVIDEO and seed_vt != self.vt:
                        self.logger.info('Ignoring merge mapping %s , %s because of vt mismatch' % (merged_guid, serialize_sk))
                    else:
                        program_guid_merge_fp.write("%s<>%s\n" % (merged_guid, serialize_sk))
                else:
                    program_guid_merge_fp.write("%s<>%s\n" % (merged_guid, serialize_sk))


class StandardCatalogParser(VtvTask):
    def __init__(self):
        VtvTask.__init__(self)

        self.seed_dir = self.system_dirs.VTV_CONTENT_VDB_DIR

        self.config = self.load_config()
        self.lang = self.options.lang
        self.parse_all = self.options.parse_all
        self.publish_alt_ids = []
        if self.options.publish_alt_ids:
            self.publish_alt_ids = self.options.publish_alt_ids.split(',')
        process_dir = self.config["process_dir"]
        self.FETCH_DATA_DIR = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, self.config["fetcher_dir"])
        self.PROCESS_DIR = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, process_dir)
        self.DATA_DIR = os.path.join(self.PROCESS_DIR, self.config["data_dir"])
        self.REPORTS_DIR = os.path.join(self.system_dirs.VTV_REPORTS_DIR, process_dir)
        if os.path.exists(self.DATA_DIR):
            remove_dir_files(self.DATA_DIR, self.logger)
        create_list = [self.PROCESS_DIR, self.DATA_DIR, self.REPORTS_DIR]
        make_dir_list(create_list, self.logger)
        self.options.report_file_name = os.path.join(self.REPORTS_DIR, '%s.html' % self.name_prefix)
        self.tgzs_to_keep = 1
        self.tar_files = []
        self.tar_file_prefix = self.config['tar_file_prefix']
        self.input_file = self.config['input_catalog']
        self.input_channel_file = 'customer-channels'

        self.program_guid_merge_fp = open(os.path.join(self.PROCESS_DIR, 'guid_merge.list'), 'w')
        self.team_guid_merge_fp = io.open(os.path.join(self.PROCESS_DIR, 'tms_team_override.txt'), mode='w' ,encoding="utf-8")
        self.tournament_guid_merge_fp = io.open(os.path.join(self.PROCESS_DIR, 'tnmt_override.txt'), mode='w',encoding="utf-8")
        self.kg_merge_fp = open(os.path.join(self.PROCESS_DIR, 'channel.merge'), 'w')
        self.ids_file = os.path.join(self.datagen_dirs.rovi_data_dir , 'id.json')
        self.channel_ids_file = os.path.join(self.datagen_dirs.rovi_channel_data_dir , 'id.json')
        self.sports_ids_file = os.path.join(self.datagen_dirs.sports_data_dir , 'id.json')
        self.guid_merge = {}
        self.sk_prefix = 'hydra'
        customer_id = os.environ.get('CUSTOMER_ID', '')
        if customer_id:
            self.sk_prefix = customer_id.lower()

    def generate_language_file(self):
        self.logger.info("Generating Language Data ...")
        lang_file = os.path.join(self.PROCESS_DIR, 'language.data')
        outf = open(lang_file, 'w')
        record = ['']*len(SCHEMA)
        record[GI_INDEX] = ENG_LL_ID
        record[TI_INDEX] = ENG_LL
        record[SK_INDEX] = ENG_LL_ID
        record[VT_INDEX] = CONTENT_TYPE_LANGUAGE
        writeSingleRecord(outf, record, SCHEMA, skip_empty=True)

    def generate_program_data(self, hydra_record):
        self.logger.info("Generating Program Data ...")

        program_filename = os.path.join(self.PROCESS_DIR, 'hydra.data')
        outf = open(program_filename, 'w')

        for vt in (CONTENT_TYPE_MOVIE, CONTENT_TYPE_EPISODE, CONTENT_TYPE_TVVIDEO, CONTENT_TYPE_TVSERIES):
            vt_filename = os.path.join(self.PROCESS_DIR, '%s.data' % vt)

            if os.path.exists(vt_filename):
                self.logger.info("Processing file: %s" % vt_filename)

                for record in fileIterator(vt_filename, SCHEMA):
                    if record[VT_INDEX] == CONTENT_TYPE_EPISODE:
                        parent_guid = record[MI_INDEX]
                        if parent_guid not in TVSERIES_IDS:
                            self.logger.info("ID: %s, VT: %s -- not written, episode does not have seriesId" % (record[SK_INDEX], record[VT_INDEX]))
                            continue

                    for idx in (HO_INDEX, DI_INDEX, PR_INDEX):
                        record[idx] = VALUE_SEPARATOR.join([sk for sk in record[idx].split(VALUE_SEPARATOR) if sk in PERSON_IDS])

                    new_val = []
                    for pa_str in record[PA_INDEX].split(MOVIE_CAST_PAIR_SEPARATOR):
                        if pa_str:
                            cast, role = pa_str.split(VALUE_SEPARATOR)
                            if cast in PERSON_IDS:
                                new_val.append('%s<>' % cast)
                    record[PA_INDEX] = MOVIE_CAST_PAIR_SEPARATOR.join(new_val)

                    writeSingleRecord(outf, record, SCHEMA, skip_empty=True)

                self.logger.info("Processing file: %s ... DONE" % vt_filename)
            else:
                self.logger.info("File: %s ... not present" % vt_filename)

        outf.close()

        episode_filename = os.path.join(self.PROCESS_DIR, '%s.data' % CONTENT_TYPE_CHANNEL)
        cmd = "cat %s >> %s" % (episode_filename, program_filename)
        self.logger.info("Appending %s to %s, CMD: %s" % (episode_filename, program_filename, cmd))
        os.system(cmd)

        self.logger.info("Generating Genre Data ...")
        genre_filename = os.path.join(self.PROCESS_DIR, 'genre.data')
        genre_fp = open(genre_filename, 'a')

        for genre_id, genre_name in ALL_GENRES.iteritems():
            record = ['']*len(SCHEMA)
            record[GI_INDEX] = hydra_record.normalize_sk(genre_id)
            record[SK_INDEX] = hydra_record.normalize_sk(genre_id)
            record[TI_INDEX] = genre_name
            if genre_id in GENRES_AKA:
                record[AK_INDEX] = VALUE_SEPARATOR.join(GENRES_AKA[genre_id])
            record[VT_INDEX] = CONTENT_TYPE_GENRE
            record[FD_INDEX] = "%s#%s" % (self.sk_prefix, genre_id)
            record[LL_INDEX] = ENG_LL_ID
            writeSingleRecord(genre_fp, record, SCHEMA, skip_empty=True)

        for sp_title, sp_id in title_sp_id_hash.iteritems():
            record = ['']*len(SCHEMA)
            record[GI_INDEX] = sp_id
            record[SK_INDEX] = sp_id
            record[TI_INDEX] = sp_title
            record[VT_INDEX] = CONTENT_TYPE_GENRE
            record[LL_INDEX] = ENG_LL_ID
            writeSingleRecord(genre_fp, record, SCHEMA, skip_empty=True)

        genre_fp.close()
        self.logger.info("Generating Genre Data ... DONE")

        self.logger.info("Generating Program Data ... DONE")

    def load_guid_merge(self,):
        guid_merge_file = os.path.join(self.seed_dir, "guid_merge.list")
        guidloadGuidPairList(guid_merge_file, self.guid_merge)

    def load_channel_ids(self):
        self.logger.info("Loading CHANNEL id json file... ")
        with open(self.channel_ids_file) as fp:
            for line in fp:
                json_data = json.loads(line)
                for gid, fd_value in json_data.iteritems():
                    p_gid = self.guid_merge.get(gid,gid)
                    for source, value in fd_value.iteritems():
                        if source == 'channel':
                            for each_val in value.get('ids', {}):
                                for each_val_key, each_val_val in each_val.iteritems():
                                    if each_val_key.endswith('id') and each_val_val:
                                        if each_val_key == 'id':
                                            CHANNEL_GUID_MERGE[each_val_val] = p_gid
        self.logger.info("Loading CHANNEL id json ... DONE")

    def load_customer_channel_aks(self):
        input_channel_file = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, self.FETCH_DATA_DIR, self.input_channel_file)
        if os.path.exists(input_channel_file):
            self.logger.info("Loading CUSTOMER CHANNEL Aka json file... ")
            with open(input_channel_file) as fp:
                for line in fp:
                    json_data = json.loads(line)
                    aka_list = []
                    for field, value in json_data.iteritems():
                        if field == 'aka':
                            aka_list = value
                        if field == 'ref':
                            channel_id = value.get('id','')
                    if aka_list != [] and channel_id != '':
                        # remove prefix 'st-' if present; was needed for verizon fios
                        prefix = 'st-'
                        if channel_id.startswith(prefix):
                            channel_id = channel_id[len(prefix):]
                        CUSTOMER_CHANNEL_AKA[channel_id] = aka_list
            self.logger.info("Loading CUSTOMER CHANNEL Aka json ... DONE")
        else:
            self.logger.info("CUSTOMER CHANNEL Aka not available")


    def load_seed(self):
        self.logger.info("Loading SEED ... ")
        with open(self.ids_file) as fp:
            for line in fp:
                json_data = json.loads(line)
                for gid, fd_value in json_data.iteritems():
                    p_gid = self.guid_merge.get(gid,gid)
                    for source, value in fd_value.iteritems():
                        if source == 'rovi':
                            space = value.pop('space', '').title()
                            for each_val in value.get('ids', {}):
                                #lang = each_val.get('lang', 'ENG').lower()
                                for each_val_key, each_val_val in each_val.iteritems():
                                    if each_val_key.endswith('id') and each_val_val:
                                        if each_val_key == 'id':
                                            #d = GUID_MERGE.setdefault(vt, {})
                                            GUID_MERGE[each_val_val] = p_gid
        self.logger.info("Loading SEED ... DONE")

       # for vt, vt_d in GUID_MERGE.iteritems():
       #     self.logger.info("VT: %s, Count: %d" % (vt, len(vt_d)))

    def load_sports_ids(self):
        self.logger.info("Loading SPORTS id json file... ")
        with open(self.sports_ids_file) as fp:
            for line in fp:
                json_data = json.loads(line)
                for gid, fd_value in json_data.iteritems():
                    p_gid = self.guid_merge.get(gid,gid)
                    for source, value in fd_value.iteritems():
                        if source == 'rovi':
                            space = value.pop('space', '').title()
                            for each_val in value.get('ids', []):
                                #lang = each_val.get('lang', 'ENG').lower()
                                for each_val_key, each_val_val in each_val.iteritems():
                                    if each_val_key.endswith('id') and each_val_val:
                                        if each_val_key == 'id':
                                            #d = GUID_MERGE.setdefault(vt, {})
                                            SPORTS_GUID_MERGE[each_val_val] = p_gid
        self.logger.info("Loading SPORTS id json ... DONE")

    def cleanup(self):
        path_suffix_list = [('.', '%s*.log' % self.script_prefix)]
        self.move_logs(self.PROCESS_DIR, path_suffix_list)
        self.remove_old_dirs(self.PROCESS_DIR, self.logs_dir_prefix,
                             self.log_dirs_to_keep, check_for_success=False)

    def load_config(self):
        ''' Load config '''
        config = {}
        with open(self.options.config_file) as json_file:
            config = json.load(json_file)
        return config

    def set_options(self):
        config_file = os.path.join(self.system_dirs.VTV_ETC_DIR, 'hydra_parser.json')
        self.parser.add_option('-c', '--config-file', default=config_file,
                               help='configuration file')
        self.parser.add_option("--lang", default="en",help="language data to be used")
        self.parser.add_option("--parse_only_avail", default=False, help="Parse only available data")
        self.parser.add_option("--parse-all", action="store_true", default=False, help="parse entire catalog irrespective of language")
        self.parser.add_option("--publish-alt-ids", default="", help="alt ids list to be published in fd field")

    def load_kg_gids_set(self):
        vt_file = 'gid_vt.txt'
        file_list = glob.glob(os.path.join(os.path.join(self.datagen_dirs.seed_data_dir, '*.%s' % ('data'))))
        if file_list:
            new_file_list = [a for a in file_list if a not in (os.path.join(self.datagen_dirs.seed_data_dir,'keyword.data'), os.path.join(self.datagen_dirs.seed_data_dir,'description.data'))]
            print new_file_list
            cmd = 'egrep -h "^Gi|^Vt" %s > %s' % (' '.join(new_file_list), vt_file)
            self.run_cmd(cmd)

        fh = open(vt_file)
        record_data = {}
        gid = ''
        kg_gid_to_vt_map = {}
        for inputLine in fh:
            inputLine = inputLine.rstrip('\n')
            token = inputLine[:2]
            data = inputLine[4:].strip()
            if token == 'Gi':
                if gid:
                    vt = record_data.get('Vt','')
                    KG_GID_VT[gid] = vt
                gid = data
                record_data.clear()
            else:
                record_data[token] = data
        fh.close()

    def run_main(self):
        self.load_guid_merge()
        self.load_seed() # Should be ONE TIME when new seed is consumed
        self.load_sports_ids() # Should be ONE TIME when new seed is consumed
        self.load_channel_ids() # Should be ONE TIME when new seed is consumed
        self.load_kg_gids_set()
        self.load_customer_channel_aks()

        #available_ids = set([_id.strip() for _id in codecs.open(os.path.join(self.system_dirs.VTV_DATAGEN_DIR, self.FETCH_DATA_DIR, 'available_ids.data'), 'r', 'utf-8')])
        #print('avaialble ids size: %d' % len(available_ids))

        input_file = os.path.join(self.system_dirs.VTV_DATAGEN_DIR, self.FETCH_DATA_DIR, self.input_file)
        
        #self.first_pass_for_version(input_file)

        inputf = open(input_file, 'r')

        index = 0
        for l in inputf:
            index += 1
            if index % 100000 == 0:
                self.logger.info("Processed: %d" % index)

            json_record = json.loads(l.strip())
            try:
                hydra_record = HydraRecord(self.logger, json_record, self.PROCESS_DIR, self.lang, self.parse_all, self.publish_alt_ids)
                hydra_record.serialize(self.program_guid_merge_fp, self.team_guid_merge_fp, self.tournament_guid_merge_fp, self.kg_merge_fp, self.sk_prefix)
            except Exception, e:
                self.logger.info("Exception: %s, json_record: %s" % (get_compact_traceback(e), json_record))
                raise

        self.logger.info("Final Processed: %d" % index)
        inputf.close()

        for outf in OUTFILE_MAP.itervalues():
            outf.close()

        self.program_guid_merge_fp.close()
        self.team_guid_merge_fp.close()
        self.tournament_guid_merge_fp.close()
        self.kg_merge_fp.close()

        team_override_file = os.path.join(self.PROCESS_DIR, 'tms_team_override.txt')
        tournament_override_file = os.path.join(self.PROCESS_DIR, 'tnmt_override.txt')
        append_file(team_override_file, os.path.join(VTV_ETC_DIR, 'tms_team_override.txt'), self.logger)
        #move_file_forcefully(team_override_file, VTV_ETC_DIR, self.logger)
        #move_file_forcefully(tournament_override_file, VTV_ETC_DIR, self.logger)


        self.generate_program_data(hydra_record)
        self.generate_language_file()
        cwd = os.getcwd()
        os.chdir(self.PROCESS_DIR)
        outfiles = []
        output_files_list = []
        for pattern in ('DATA_*', '*.data', '*.data.avail', '*.list', '*.json' , 'channel.merge'):
            output_files = glob.glob('%s' % pattern)
            output_files_list.extend(output_files)
        os.chdir(cwd)

        copy_list = [os.path.join(self.PROCESS_DIR, x) for x in output_files_list]
        copy_file_list(copy_list, self.DATA_DIR, self.logger)

        self.archive_data_files(
            self.PROCESS_DIR, self.DATA_DIR,
            output_files_list, self.tar_file_prefix,
            tgz_creation_only=True
        )
        #print('Available ids: %d' % len())

if __name__ == '__main__':
    vtv_task_main(StandardCatalogParser)

'''
SPORTS_EVENT
        set([u'seriesId', u'version', u'sortTitle', u'releaseDate', u'duration', u'connectors', u'altIds', u'releaseYear', u'keywords', u'shortDescription', u'id', u'ratings', u'genres', u'title', u'availableDate', u'gameTime', u'colorCode', u'content', u'playoffRound', u'releaseVariant', u'qualityRatings', u'type', u'metadata', u'eventTitle', u'description', u'descriptors', u'images', u'longDescription', u'credits', u'moods', u'offset', u'organizations', u'parentCategories', u'origAudioLang', u'venue', u'season', u'teams', u'providerId', u'seasonType'])
LINEAR_BLOCK
        set([u'version', u'content', u'offers', u'stationId', u'altIds', u'offset', u'date', u'type', u'id', u'metadata'])
EPISODE
        set([u'seriesId', u'seasonPremier', u'sortTitle', u'releaseDate', u'duration', u'connectors', u'altIds', u'releaseYear', u'keywords', u'shortDescription', u'id', u'ratings', u'genres', u'seasonFinale', u'title', u'availableDate', u'colorCode', u'seasonNum', u'version', u'releaseVariant', u'sequenceInSeries', u'qualityRatings', u'content', u'type', u'metadata', u'seasonId', u'description', u'descriptors', u'images', u'longDescription', u'credits', u'seriesPremier', u'moods', u'offset', u'episodeTitle', u'parentCategories', u'seriesFinale', u'origAudioLang', u'episodeNum', u'providerId'])
STATION
        set([u'altIds', u'broadcast', u'videoQuality', u'affiliateOf', u'hdVersionOf', u'stationGroup', u'callSign', u'content', u'version', u'offset', u'images', u'attributes', u'metadata', u'type', u'id', u'name'])
MOVIE
        set([u'sortTitle', u'releaseDate', u'duration', u'connectors', u'altIds', u'releaseYear', u'keywords', u'shortDescription', u'id', u'ratings', u'genres', u'title', u'availableDate', u'colorCode', u'content', u'version', u'releaseVariant', u'descriptors', u'type', u'metadata', u'description', u'qualityRatings', u'images', u'longDescription', u'credits', u'moods', u'offset', u'parentCategories', u'origAudioLang', u'providerId'])
PERSON
        set([u'altIds', u'birthPlace', u'parentCategories', u'personName', u'gender', u'altNames', u'birthDate', u'content', u'version', u'offset', u'images', u'deathDate', u'type', u'id', u'metadata'])
PROGRAM
        set([u'sortTitle', u'releaseDate', u'duration', u'connectors', u'altIds', u'releaseYear', u'keywords', u'shortDescription', u'id', u'ratings', u'genres', u'title', u'availableDate', u'colorCode', u'content', u'version', u'releaseVariant', u'descriptors', u'type', u'metadata', u'description', u'qualityRatings', u'images', u'longDescription', u'credits', u'moods', u'offset', u'parentCategories', u'origAudioLang', u'providerId'])
SPORTS_TEAM
        set([u'altIds', u'organizations', u'name', u'nickname', u'alternateNicknames', u'alternateAbbreviations', u'content', u'version', u'location', u'abbreviation', u'offset', u'images', u'type', u'id', u'metadata'])
SERIES
        set([u'sortTitle', u'releaseDate', u'duration', u'connectors', u'altIds', u'releaseYear', u'keywords', u'shortDescription', u'id', u'totalSeasons', u'ratings', u'genres', u'title', u'availableDate', u'totalEpisodes', u'colorCode', u'content', u'version', u'releaseVariant', u'qualityRatings', u'maxEpisodeReleaseDate', u'type', u'metadata', u'description', u'descriptors', u'images', u'longDescription', u'credits', u'moods', u'offset', u'parentCategories', u'parentSeriesId', u'origAudioLang', u'providerId'])
GENRE
        set([u'version', u'content', u'altIds', u'offset', u'metadata', u'type', u'id', u'name'])
SPORTS_ORGANIZATION
        set([u'altIds', u'abbreviation', u'name', u'eventType', u'parentId', u'orgType', u'content', u'version', u'offset', u'images', u'type', u'id', u'metadata'])
CHANNEL_LINEUP
        set([u'altIds', u'channels', u'name', u'mvpd', u'metadata', u'postalCodes', u'lineupType', u'content', u'version', u'offset', u'country', u'type', u'id', u'headendId'])

{"id":"26","type":"MOVIE","version":0,"offset":9,"content":{"id":"26","altIds":[{"ns":"rovi.groupId","id":"3824241"},{"ns":"groupId","id":"3824241"}],"metadata":{"sources":["rovi"],"countryOfOrigin":["IT"]},"title":{"en":"The Worst Secret Agents"},"sortTitle":null,"description":{"en":"Two zanies are pursued by a mysterious spy called Goldginger. Franco Franchi, Ciccio Ingrassia, Fernando Rey, Andrea Bosic, Gloria Paul. Lucio Fulci directed."},"shortDescription":null,"longDescription":null,"providerId":null,"ratings":null,"qualityRatings":null,"origAudioLang":"it","duration":4980,"releaseYear":1965,"releaseDate":"1964-10-10","availableDate":null,"credits":[{"personId":"rovi:person:6777922","name":"Lucio Fulci","role":"OTHER","otherRole":"Screenwriter","characterName":null},{"personId":"rovi:person:15268337","name":"Sergio Canevari","role":"OTHER","otherRole":"Special Effects","characterName":null},{"personId":"rovi:person:15610457","name":"Giuseppe Ranieri","role":"OTHER","otherRole":"Art Director","characterName":null},{"personId":"rovi:person:15734735","name":"Ornella Micheli","role":"OTHER","otherRole":"Editor","characterName":null},{"personId":"rovi:person:15750210","name":"Vittorio Metz","role":"OTHER","otherRole":"Screenwriter","characterName":null},{"personId":"rovi:person:18491115","name":"Amadeo Sollazzo","role":"OTHER","otherRole":"Screenwriter","characterName":null},{"personId":"rovi:person:15491111","name":"Adalberto Albertini","role":"OTHER","otherRole":"Cinematographer","characterName":null},{"personId":"rovi:person:18512440","name":"Piero Umiliani","role":"OTHER","otherRole":"Composer","characterName":null},{"personId":"rovi:person:15928630","name":"Franco Franchi","role":"ACTOR","otherRole":null,"characterName":"Franco"},{"personId":"rovi:person:15276481","name":"Nando Angelini","role":"ACTOR","otherRole":null,"characterName":null},{"personId":"rovi:person:15668769","name":"Ciccio Ingrassia","role":"ACTOR","otherRole":null,"characterName":"Ciccio"},{"personId":"rovi:person:15260750","name":"Carla Calo","role":"ACTOR","otherRole":null,"characterName":"Russian Agent"},{"personId":"rovi:person:18495874","name":"Ingrid Schoeller","role":"ACTOR","otherRole":null,"characterName":"Wife"},{"personId":"rovi:person:15299234","name":"Aroldo Tieri","role":"ACTOR","otherRole":null,"characterName":"Husband"},{"personId":"rovi:person:15343760","name":"Annie Gorassini","role":"ACTOR","otherRole":null,"characterName":null},{"personId":"rovi:person:6777922","name":"Lucio Fulci","role":"DIRECTOR","otherRole":null,"characterName":null},{"personId":null,"name":"Mega Film","role":"OTHER","otherRole":"Production Company","characterName":null},{"personId":"rovi:person:15657602","name":"Luca Sportelli","role":"ACTOR","otherRole":null,"characterName":null},{"personId":"rovi:person:15500561","name":"Poldo Bendandi","role":"ACTOR","otherRole":null,"characterName":null}],"genres":[{"id":"2681","altIds":null,"metadata":null,"name":{"en":"Comedy"}}],"moods":null,"keywords":null,"descriptors":{"general":null,"moods":null,"themes":["Twins and Lookalikes","Assumed Identities","Space Travel"],"tones":["Easygoing","Goofy","Humorous"],"subjects":null,"settings":null,"timePeriods":null,"audiences":null,"characters":null,"topics":null},"connectors":{"spinoffIds":null,"superSeriesIds":null,"seriesSpecialIds":null,"crossoverIds":null,"franchises":null,"remakeIds":null,"similarIds":["443490","471478","1072230","1075293","1137679"]},"images":null,"releaseVariant":null,"colorCode":null,"parentCategories":null}} 

{"id":"st-923863026","type":"STATION","version":6,"offset":10057809,"content":{"id":"st-923863026","altIds":null,"metadata":null,"name":{"en":"Teletruria"},"callSign":"TRURIA","stationGroup":"TRURIA","affiliateOf":null,"hdVersionOf":null,"videoQuality":"SD","attributes":["CABLE"],"broadcast":null,"images":null}}

{"id":"tivo:tm.365218533","type":"SPORTS_TEAM","version":13,"offset":10638306,"content":{"id":"tivo:tm.365218533","altIds":[{"ns":"tmsBrandId","id":"66"},{"ns":"roviOrgId","id":"68972787"}],"metadata":{"stadium":{"name":"Louis Crews Stadium","country":"USA","state":"Alabama","city":"Huntsville"},"genderType":"Men"},"name":{"en":"Alabama A&M"},"nickname":{"en":"Bulldogs"},"alternateNicknames":null,"abbreviation":{"en":"AAMU"},"alternateAbbreviations":null,"location":{"en":"Alabama Agricultural and Mechanical University"},"organizations":[{"id":"tivo:sp.365217325","altIds":null,"metadata":null,"parentId":null,"orgType":"SPORT","name":{"en":"Football"},"abbreviation":null,"eventType":"EVENT","images":null},{"id":"tivo:lg.365217537","altIds":null,"metadata":null,"parentId":"tivo:sp.365217325","orgType":"LEAGUE","name":{"en":"NCAA Football"},"abbreviation":{"en":"NCAAF"},"eventType":"GAME","images":null}],"images":null}}

{"id":"tivo:tm.365218533","type":"SPORTS_TEAM","version":13,"offset":10638306,"content":{"id":"tivo:tm.365218533","altIds":[{"ns":"tmsBrandId","id":"66"},{"ns":"roviOrgId","id":"68972787"}],"metadata":{"stadium":{"name":"Louis Crews Stadium","country":"USA","state":"Alabama","city":"Huntsville"},"genderType":"Men"},"name":{"en":"Alabama A&M"},"nickname":{"en":"Bulldogs"},"alternateNicknames":null,"abbreviation":{"en":"AAMU"},"alternateAbbreviations":null,"location":{"en":"Alabama Agricultural and Mechanical University"},"organizations":[{"id":"tivo:sp.365217325","altIds":null,"metadata":null,"parentId":null,"orgType":"SPORT","name":{"en":"Football"},"abbreviation":null,"eventType":"EVENT","images":null},{"id":"tivo:lg.365217537","altIds":null,"metadata":null,"parentId":"tivo:sp.365217325","orgType":"LEAGUE","name":{"en":"NCAA Football"},"abbreviation":{"en":"NCAAF"},"eventType":"GAME","images":null}],"images":null}}
'''
