#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

from os.path import dirname, basename, abspath
from datetime import datetime
import logging
import sys
import json

from meta import dy
from meta import st

from elasticsearch import Elasticsearch,helpers
from elasticsearch.exceptions import TransportError
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk

class esmgr:

    default_host = "192.168.71.133:9200"

    st_mapping = {
        "MMSI": {
            "type": "integer"
        },
        "ShipName": {
            "type": "text",
            "analyzer": "english"
        },
        "ShipTypeEN": {
            "type": "text",
            "analyzer": "english"
        },
        "Length": {
            "type": "double"
        },
        "Width": {
            "type": "double"
        },
        "CallSign": {
            "type": "text",
            "analyzer": "english"
        },
        "IMO": {
            "type": "integer"
        },
    }

    dy_mapping = {
        "MMSI": {
            "type": "integer"
        },
        "NavStatusEN": {
            "type": "text",
            "analyzer": "english"
        },
        "Draught": {
            "type": "double"
        },
        "Heading": {
            "type": "double"
        },
        "Course": {
            "type": "double"
        },
        "HeadCourse": {
            "type": "double"
        },
        "Speed": {
            "type": "double"
        },
        "Rot": {
            "type": "double"
        },
        "Dest": {
            "type": "text",
            "analyzer": "english"
        },
        "ETA": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss"
        },
        "Receivedtime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss"
        },
        "UnixTime": {
            "type": "long"
        },
        "POSITION": {
            "type": "geo_point",
        },
    }

    st_index_body = {
        'mappings': {
            'StaticInfo': {
                'properties': st_mapping
            }
        }
    }

    dy_index_body = {
        'mappings': {
            'DynamicInfo': {
                'properties': dy_mapping
            }
        }
    }

    def __init__(self,host=default_host):
        # get trace logger and set level
        tracer = logging.getLogger('elasticsearch.trace')
        tracer.setLevel(logging.INFO)
        tracer.addHandler(logging.FileHandler('/tmp/es_trace.log'))

        self.es = Elasticsearch(host)
        self.create_ais_index('st', self.st_index_body)
        self.create_ais_index('dy', self.dy_index_body)
        pass

    def create_ais_index(self, index, body):
        # create empty index
        try:
            if not self.es.indices.exists(index=index):
                self.es.indices.create(index=index, body=body, )
        except TransportError as e:
            # ignore already existing index
            if e.error == 'index_already_exists_exception':
                pass
            else:
                raise

    def enqueue_DY(self, d):
        def construt_body(d):
            body = {
                "MMSI": d.MMSI,
                "NavStatusEN":d.NavStatusEN,
                "Draught": d.Draught,
                "Heading":d.Heading,
                "Course": d.Course,
                "HeadCourse": d.HeadCourse,
                "Speed": d.Speed,
                "Rot": d.Rot,
                "Dest": d.Dest,
                "ETA": d.ETA.strftime('%Y-%m-%d %H:%M:%S'),
                "Receivedtime": d.Receivedtime.strftime('%Y-%m-%d %H:%M:%S'),
                "UnixTime": d.UnixTime,
                "POSITION": [d.Lon_d, d.Lat_d]
            }
            print(json.dumps(body))
            yield json.dumps(body)
        # Speed Slowly!
        self.es.index(index='dy',doc_type='DynamicInfo',body = construt_body(d))

    def enqueue_ST(self, s):
        def construt_body(s):
            body = {
                "MMSI": s.MMSI,
                "ShipName":s.ShipName,
                "ShipTypeEN": s.ShipTypeEN,
                "Length":s.Length,
                "Width": s.Width,
                "CallSign": s.CallSign,
                "IMO": s.IMO,
            }
            print(construt_body(s))
            return json.dumps(body)
        self.es.index(index='st',doc_type='StaticInfo',id=s.MMSI, body = construt_body(s))

    def fetch_DY(self, MMSI):
        def construt_query(MMSI):
            query = {
                "query":{"match":{'MMSI':MMSI}}
            }
            return json.dumps(query)
        res = self.es.search(index='dy',doc_type='DynamicInfo',body = construt_query(MMSI))
        print(res)
        ret = []
        for hit in res['hits']['hits']:
            hit = hit['_source']
            d = dy([hit['MMSI'],hit['NavStatusEN'],hit['Draught'],hit['Heading'],hit['Course'],hit['HeadCourse'],
                    hit['Speed'],hit['Rot'],hit['Dest'],hit['ETA'],hit['Receivedtime'],hit['UnixTime'],hit['POSITION'][1],
                   hit['POSITION'][0]])
            ret.append(d)
            print(d)
        return ret

    def delete_DY(self,_id):
        try:
            ret = self.es.delete(index='dy',doc_type='DynamicInfo',id=_id,timeout='1m')
        except elasticsearch.exceptions.ConnectionTimeout as e:
            print(e)
            return None
        print(ret)
        return ret

    def fetch_ST(self, MMSI):
        def construt_query(MMSI):
            query = {
                "query":{"match":{'MMSI':MMSI}}
            }
            return json.dumps(query)
        res = self.es.get(index='st', doc_type='StaticInfo', id = MMSI)
        if res['found']:
            hit = res['_source']
            s = st([hit['MMSI'], hit['ShipName'], hit['ShipTypeEN'], hit['Length'], hit['Width'], hit['CallSign'],hit['IMO']])
            print(s)
            return s
        return None

    def import_dy_bulk(self,dy):
        def parse_commits(dy):
            for d in dy:
                body = {
                    "MMSI": d.MMSI,
                    "NavStatusEN": d.NavStatusEN,
                    "Draught": d.Draught,
                    "Heading": d.Heading,
                    "Course": d.Course,
                    "HeadCourse": d.HeadCourse,
                    "Speed": d.Speed,
                    "Rot": d.Rot,
                    "Dest": d.Dest,
                    "ETA": d.ETA.strftime('%Y-%m-%d %H:%M:%S'),
                    "Receivedtime": d.Receivedtime.strftime('%Y-%m-%d %H:%M:%S'),
                    "UnixTime": d.UnixTime,
                    "POSITION": [d.Lon_d, d.Lat_d]
                }
                yield body

        for ok, result in streaming_bulk(self.es,parse_commits(dy),index='dy',doc_type='DynamicInfo',
                                         chunk_size=5000,raise_on_error=False,raise_on_exception=False):  # keep the
            # batch sizes small for
            # appearances only
            action, result = result.popitem()
            _id = '/dy/DynamicInfo/%s' % (result['_id'])
            # process the information from ES whether the document has been
            # successfully indexed
            if not ok:
                print('Failed to %s document %s: %r' % (action, _id, result))
            else:
                print(_id)

    def import_st_bulk(self, st):
        def parse_commits(st):
            for s in st:
                action = {
                    "_id":s.MMSI,
                    "_source":{
                        "MMSI": s.MMSI,
                        "ShipName":s.ShipName,
                        "ShipTypeEN": s.ShipTypeEN,
                        "Length":s.Length,
                        "Width": s.Width,
                        "CallSign": s.CallSign,
                        "IMO": s.IMO,
                        }
                }
                yield action

        for ok, result in streaming_bulk(self.es, parse_commits(st), index='st', doc_type='StaticInfo',
                                         chunk_size=5000):  # keep the batch sizes small for appearances only
            action, result = result.popitem()
            _id = '/st/StaticInfo/%s' % (result['_id'])
            # process the information from ES whether the document has been
            # successfully indexed
            if not ok:
                print('Failed to %s document %s: %r' % (action, _id, result))
            else:
                print(_id)

    def import_dy_parallel_bulk(self,dy):
        def parse_commits(dy):
            for d in dy:
                body = {
                    "MMSI": d.MMSI,
                    "NavStatusEN": d.NavStatusEN,
                    "Draught": d.Draught,
                    "Heading": d.Heading,
                    "Course": d.Course,
                    "HeadCourse": d.HeadCourse,
                    "Speed": d.Speed,
                    "Rot": d.Rot,
                    "Dest": d.Dest,
                    "ETA": d.ETA.strftime('%Y-%m-%d %H:%M:%S'),
                    "Receivedtime": d.Receivedtime.strftime('%Y-%m-%d %H:%M:%S'),
                    "UnixTime": d.UnixTime,
                    "POSITION": [d.Lon_d, d.Lat_d]
                }
                yield body

        for ok, result in parallel_bulk(self.es,parse_commits(dy),index='dy',doc_type='DynamicInfo',
                                         chunk_size=5000,raise_on_error=False,raise_on_exception=False):  # keep the
            # batch sizes small for
            # appearances only
            try:
                action, result = result.popitem()
                _id = '/dy/DynamicInfo/%s' % (result['_id'])
            # process the information from ES whether the document has been
            # successfully indexed
            except KeyError as e:
                print(e)
                continue

            if not ok:
                print('Failed to %s document %s: %r' % (action, _id, result))
            else:
                print(_id)

    def import_st_parallel_bulk(self, st):
        def parse_commits(st):
            for s in st:
                action = {
                    "_id":s.MMSI,
                    "_source":{
                        "MMSI": s.MMSI,
                        "ShipName":s.ShipName,
                        "ShipTypeEN": s.ShipTypeEN,
                        "Length":s.Length,
                        "Width": s.Width,
                        "CallSign": s.CallSign,
                        "IMO": s.IMO,
                        }
                }
                yield action

        for ok, result in parallel_bulk(self.es, parse_commits(st), index='st', doc_type='StaticInfo',
                                         chunk_size=5000):  # keep the batch sizes small for appearances only
            action, result = result.popitem()
            _id = '/st/StaticInfo/%s' % (result['_id'])
            # process the information from ES whether the document has been
            # successfully indexed
            if not ok:
                print('Failed to %s document %s: %r' % (action, _id, result))
            else:
                print(_id)

    def set_search_optional(self):
        es_search_options = {
            "query": {"match_all": {}}
        }
        return es_search_options

    def get_search_result(self, es_search_options, scroll='5m', size=5000,index='dy', doc_type='DynamicInfo', \
                                                                                            timeout='1m'):
        es_result = helpers.scan(
            client=self.es,
            query=es_search_options,
            scroll=scroll,
            index=index,
            doc_type=doc_type,
            timeout=timeout
        )
        return es_result

    def get_result_list(self, es_result):
        result = []
        for item in es_result:
            # result.append(item['_source'])
            print(item['_source'])
        return None

    def get_remove_duplicate(self): # remove_duplicate
        es_search_options = self.set_search_optional()
        es_result = self.get_search_result(es_search_options)
        final_result = self.remove_duplicate(es_result)
        return final_result

    def remove_duplicate(self, es_result):
        for item in es_result:
            MMSI = item['_source']['MMSI']
            ETA  = item['_source']['ETA']
            _id = []
            es_search_options = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"ETA": ETA}},
                            {"match": {"MMSI": MMSI}}
                        ]
                    }
                },
                "sort": {"ETA": {"order": "desc"}}
            }
            for p in self.get_search_result(es_search_options):
                _id.append(p['_id'])

            print(_id)
            if len(_id) != 1:
                for id in _id[1:]:
                    self.delete_DY(id)