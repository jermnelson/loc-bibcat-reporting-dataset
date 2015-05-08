"""
This module ingests XML exported from the Library of Congress, converts to 
BIBFRAME entities and stores into a running BIBFRAME Datastore.


"""
__author__ = "Jeremy Nelson"
__license__ = "GPLv3"

import argparse
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
sys.path.append(os.path.join(CURRENT_DIR, "lib/bibframe-datastore"))
import semantic_server.repository.utilities.bibframe as bibframe
from semantic_server.repository.resources.fedora import Resource
from semantic_server.repository.utilities.namespaces import *

# SPARQL Statements
INSTANCE_SPARQL = """PREFIX rdf: <{}>
PREFIX bf: <{}>
SELECT DISTINCT *
WHERE {{
  ?subject rdf:type bf:Instance .
}}""".format(RDF, BF)


# Local config
CONFIG = {'FUSEKI': {'port': 3030, 
                     'host': 'localhost', 
                     'datastore': 'bf'}, 
          'FEDORA': {'port': 8080, 
                     'host': 'localhost'}, 
          'ELASTICSEARCH': {'port': 9200, 
                            'host': 'localhost'}
}

etree.register_namespace("", "http://www.loc.gov/MARC21/slim")

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
    if not field001:
        with open("E:\\2015\\loc-bibcat-reporting-dataset\\bad-rec.xml", "w+") as xml_file:
            xml_file.write(etree.tostring(record).decode())
        raise ValueError()
    else: 
        print("FOUND field001\n", etree.tostring(record).decode())
        
    media_url = LOC_MEDIA_URL.format(field001.text, 100)
    result = requests.get(media_url)
    if result.status_code < 400:
        searcher = bibframe.BIBFRAMESearch(config=CONFIG)
        print("Found {}, bf_graph={}".format(media_url, bf_graph))
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
            binary=result.content, 
            mimetype='image/jpeg')
        return cover_art_url

def load_records(phrase, start=1):
    """Function takes a phrase, queries LOC's Voyager z3950 endpoint,
    runs result through the bibframe-socket XQuery transform, and 
    ingests into a running BIBFRAME Datastore instance.

    Args:
        phrase -- String to query z3950
    """
    voyager_url = urllib.parse.urljoin(
        LOC_Z3950_URL,
        'voyager?' + urllib.parse.urlencode(
            {'operation': 'searchRetrieve',
            'version': '1.1',
            'query': '"{}"'.format(phrase),
            'recordSchema': 'opacxml',
            'startRecord': start,
            'maximumRecords': 10,
            'recordPacking': 'xml'}))
    bf_graph = process_voyager_xml(voyager_url)
    return bf_graph

def load_reporting_samples():
    """Function loads Library of Congress sample sets using "Mark Twain"
    and the "Bible" search phrases."""
    mark_twain = "Mark Twain"
    bible = "Bible"
    start = datetime.utcnow()
    print("Starting loading {} and {} phrases at {}".format(
        mark_twain, 
        bible, 
        start.isoformat()))
    load_records(mark_twain)
    load_records(mark_twain, 11)
    load_records(bible)
    load_records(bible, 11)
    end = datetime.utcnow()
    print(
        "Finished loading reporting module samples at {}, total time {}".format(
            end,
            (end-start).seconds / 60.0))

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
    held_item.__create__(rdf=rdf_graph)
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
    ingester.ingest(False)
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
        if tag.endswith('record'):
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
