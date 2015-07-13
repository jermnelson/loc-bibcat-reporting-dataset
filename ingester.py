"""
This module ingests XML exported from the Library of Congress, converts to 
BIBFRAME entities and stores into a running BIBFRAME Datastore.

To use the ingester with the sample LOC queries, run the following command:

    $ python ingester.py load

"""
__author__ = "Jeremy Nelson"
__license__ = "GPLv3"

import argparse
import configparser
import datetime
import logging
import os
import rdflib
import requests
import socket
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as etree
from datetime import datetime

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(os.path.join(CURRENT_DIR, "lib/bibframe-datastore/src"))
import semantic_server.repository.utilities.bibframe as bibframe
from semantic_server.repository.resources.fedora import Resource
from semantic_server.repository.utilities.namespaces import *

logging.basicConfig(filename='error.log',
                    format='%(asctime)s %(funcName)s %(message)s',
                    level=logging.ERROR)
logging.basicConfig(filename='info.log',
                    format='%(asctime)s %(funcName)s %(message)s',
                    level=logging.INFO)



# SPARQL Statements
INSTANCE_SPARQL = """PREFIX rdf: <{}>
PREFIX bf: <{}>
SELECT DISTINCT *
WHERE {{
  ?subject rdf:type bf:Instance .
}}""".format(RDF, BF)


# Local config
CONFIG = configparser.ConfigParser()
CONFIG['DEFAULT'] = {"host": "localhost"}
CONFIG['TOMCAT'] = { 'port': 8080}
CONFIG['BLAZEGRAPH'] = {'path': 'bigdata'} 
CONFIG['FEDORA'] = {'path': 'fedora'}
CONFIG['ELASTICSEARCH'] = {'path': 'elasticsearch'}

etree.register_namespace("", "http://www.loc.gov/MARC21/slim")
etree.register_namespace("zs", "http://www.loc.gov/zing/srw/")

LOC_Z3950_URL = 'http://z3950.loc.gov:7090/'
LOC_MEDIA_URL = 'http://lcweb2.loc.gov/diglib/media/loc.natlib.lcdb.{}/001.tif/{}'


def add_cover_art(record, bf_graph):
    """Function adds a new bf:CoverArt resource 

    Args:
        record -- Record XML element
        bf_graph -- BIBFRAME graph
    """
    field001 = record.find(
        "{http://www.loc.gov/MARC21/slim}controlfield[@tag='001']")
    #control_fields= record.findall(
    #    "{http://www.loc.gov/MARC21/slim}controlfield")
    #if len(control_fields) < 1:
    #    control_fields= record.findall(
    #        "controlfield")
  
    #for field in control_fields:
    #    if field.get('tag') == '001':
    #        field001 = field
    #        break
    media_url = LOC_MEDIA_URL.format(field001.text, 100)
    result = requests.get(media_url)
    if result.status_code < 400:
        searcher = bibframe.BIBFRAMESearch(config=CONFIG)
        cover_art_graph = bibframe.default_graph()
        cover_art_subject = rdflib.BNode()
        instance_query = bf_graph.query(INSTANCE_SPARQL)    
        for row in instance_query.bindings:
            local_uri = row.get('?subject')
            instance_url = searcher.triplestore.__sameAs__(str(local_uri))
            cover_art_graph.add((cover_art_subject,
                                 BF.coverArtFor,
                                 rdflib.URIRef(instance_url)))
        cover_art_graph.add((cover_art_subject, RDF.type, BF.CoverArt))
        cover_art_graph.add((cover_art_subject, 
                             SCHEMA.isBasedOnUrl,
                             rdflib.URIRef(media_url)))
        cover_art = Resource(CONFIG, bibframe.BIBFRAMESearch(config=CONFIG))
        cover_art_url = cover_art.__create__(
            rdf=cover_art_graph,
            index='bibframe',
            doc_type='CoverArt', 
            binary=result.content, 
            mimetype='image/jpeg')
        logging.info("Cover art found media_url={} cover art url={}".format(
            media_url, cover_art_url))
        return cover_art_url

def build_voyager_url(phrase, start=1, max_recs=10):
    return urllib.parse.urljoin(
        LOC_Z3950_URL,
        'voyager?' + urllib.parse.urlencode(
            {'operation': 'searchRetrieve',
            'version': '1.1',
            'query': '"{}"'.format(phrase),
            'recordSchema': 'opacxml',
            'startRecord': start,
            'maximumRecords': max_recs,
            'recordPacking': 'xml'}))

def load_records(phrase, start=1):
    """Function takes a phrase, queries LOC's Voyager z3950 endpoint,
    runs result through the bibframe-socket XQuery transform, and 
    ingests into a running BIBFRAME Datastore instance.

    Args:
        phrase -- String to query z3950
    """
    try:
        voyager_url = build_voyager_url(phrase, start)    
        bf_graph = process_voyager_xml(voyager_url)
        return bf_graph
    except:
        logging.error("{} start={}".format(phrase, start))
    

def load_reporting_samples():
    """Function loads Library of Congress sample sets using "Mark Twain"
    and the "Bible" search phrases."""
    mark_twain = "Mark Twain"
    bible = "Bible"
    start = datetime.utcnow()
    print("Loading {} and {} phrases at {}".format(
        mark_twain, 
        bible, 
        start.isoformat()))
    #load_records(mark_twain)

    load_sample(mark_twain)
    load_sample(bible)
    #load_records(bible, 11)
    end = datetime.utcnow()
    print(
        "Finished loading reporting module samples at {}, total time {}".format(
            end,
            (end-start).seconds / 60.0))

def load_sample(phrase):
    """Function loads an entire query set in 10 record increments 

    Args:
        phrase -- Phrase to search one
    """
    start, end = 1, None
    # Run query and retrieve a single XML record to get total number of records
    result = requests.get(build_voyager_url(phrase, 1))
    if result.status_code > 399:
        message = "Load sample for {} failed trying to retrieve {} code={}".format(
            phrase,
            build_voyager_url(phrase, 1, 1),
            result.status_code)
        logging.error(message)
        raise ValueError(message)
    z3950_xml = etree.XML(result.content)
    numberOfRecords = z3950_xml.find("{http://www.loc.gov/zing/srw/}numberOfRecords")
    num_recs = int(numberOfRecords.text)
    shards = int(num_recs / 10)
    start = datetime.utcnow()
    print("Loading sample {} at {} total shards={}".format(phrase, 
                                                           start.isoformat(), 
                                                           shards))
    for i,shard in enumerate(range(1, shards+1)):
        load_records(phrase, shard)
        print(".", end="")
        if not i%10 and i > 0:
            print(i, end="")
    end = datetime.utcnow()
    print("""Finished loading sample for term "{}" at {}, 
total recs={} time elapsed={} minutes""".format(
        phrase, 
        end.isoformat(), 
        i, 
        (end-start).seconds/60.0))
        
    
    
    
    

def process_holding(element, bf_graph):
    """Function takes a Holding Element and returns the BIBFRAME HeldItem
    RDF graph

    Args:
        element -- Element
        bf_graph -- BIBFRAME Graph of MARC Record
    """
    raw_xml = etree.tostring(element).decode()
    rdf_graph = bibframe.default_graph()
    itemId = element.find("circulations/circulation/itemId")
    if itemId is not None:
        item_url = "http://bibcat.loc.gov/HeldItem/{}".format(itemId.text)
        holding_uri = rdflib.URIRef(item_url)
        rdf_graph.add(
            (holding_uri,
             BF.itemId,
             rdflib.Literal(itemId.text)))
    else:
        holding_uri = rdflib.URIRef(
            "http://bibcat.loc.gov/HeldItem/{}".format(random.randint()))
    instance_query = bf_graph.query(INSTANCE_SPARQL)    
    searcher = bibframe.BIBFRAMESearch(config=CONFIG)
    for row in instance_query.bindings:
        local_uri = row.get('?subject')
        instance_url = searcher.triplestore.__sameAs__(str(local_uri))
        rdf_graph.add(
            (holding_uri,
             BF.holdingFor,
             rdflib.URIRef(instance_url)))
    call_number = element.find("callNumber")
    if call_number is not None:
        rdf_graph.add(
            (holding_uri, 
             BF.shelfMarkLcc, 
             rdflib.Literal(call_number.text)))
        rdf_graph.add(
            (holding_uri, 
             BF.shelfMarkSchema, 
             rdflib.Literal("lcc")))
    local_location = element.find("localLocation")
    if local_location is not None:
        rdf_graph.add(
            (holding_uri,
             BF.subLocation,
             rdflib.Literal(local_location.text)))
    held_item = Resource(CONFIG, bibframe.BIBFRAMESearch(config=CONFIG))
    held_item.__create__(rdf=rdf_graph, index='bibframe')
    return held_item

def process_record(element, quiet=True):
    """Function takes a Holding Element and returns the BIBFRAME HeldItem
    RDF graph

    Args:
        element -- Element
    """
    raw_xml = etree.tostring(element).decode().replace("\n", "").encode()
    bf_graph = xquery_socket(raw_xml)
    ingester = bibframe.Ingester(config=CONFIG, graph=bf_graph)
    ingester.ingest(quiet)
    add_cover_art(element, bf_graph)
    return bf_graph


def process_voyager_xml(url):
    """Function takes a z3950 URL, retrieves XML from LOC's Voyager ILS and 
    iterates through all records.

    Args: 
        url -- z3950 URL
    """
    context = etree.iterparse(urllib.request.urlopen(url), events=('end',))
    bf_graph, counter = None, 0
    for action, element in context:
        tag = str(element.tag)
        if tag.endswith('holding'):
            process_holding(element, bf_graph)
        if tag.startswith('{http://www.loc.gov/MARC21/slim}record'):
            bf_graph = process_record(element)
        
def xquery_socket(raw_xml):
    """Function takes raw_xml and converts to BIBFRAME RDF

    Args:
       raw_xml -- Raw XML 
    """
    xquery_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    xquery_server.connect(('localhost', 8089))
    xquery_server.sendall(raw_xml + b'\n')
    rdf_xml = b''
    while 1:
        data = xquery_server.recv(1024)
        if not data:
            break
        rdf_xml += data
    xquery_server.close()
    bf_graph = bibframe.default_graph()
    bf_graph.parse(data=rdf_xml.decode(), format='xml')
    return bf_graph

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Library of Congress Sample Ingester')
    parser.add_argument('load', help='Load BIBCAT Reporting Samples')
    args = parser.parse_args()
    if args.load:
        load_reporting_samples()
