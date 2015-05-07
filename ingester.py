"""
This module ingests XML exported from the Library of Congress, converts to 
BIBFRAME entities and stores into a running BIBFRAME Datastore.


"""
__author__ = "Jeremy Nelson"
__license__ = "GPLv3"

import argparse
import os
import requests
import rdflib
import socket
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(os.path.join(CURRENT_DIR, "lib/bibframe-datastore"))
import semantic_server.repository.utilities.bibframe as bibframe

# Local config
CONFIG = {'FUSEKI': {'port': 3030, 
                     'host': 'localhost', 
                     'datastore': 'bf'}, 
          'FEDORA': {'port': 8080, 
                     'host': 'localhost'}, 
          'ELASTICSEARCH': {'port': 9200, 
                            'host': 'localhost'}
}

LOC_Z3950_URL = 'http://z3950.loc.gov:7090/voyager'

XQUERY_SERVER = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


def load_records(phrase, start=1):
    """Function takes a phrase, queries LOC's Voyager z3950 endpoint,
    runs result through the bibframe-socket XQuery transform, and 
    ingests into a running BIBFRAME Datastore instance.

    Args:
        phrase -- String to query z3950
    """
    result = requests.get(
        LOC_Z3950_URL,
        params={ 
            'operation': 'searchRetrieve',
            'version': '1.1',
            'query': '"{}"'.format(phrase),
            'recordSchema': 'opacxml',
            'startRecord': start,
            'maximumRecords': 10,
            'recordPacking': 'xml'})
    if result.status_code > 399:
        raise ValueError(result.text)
    bf_graph = bibframe.default_graph()
    bf_graph.parse(data=rdf_xml, format='xml')
    

def load_reporting_samples():
    """Function loads Library of Congress sample sets using "Mark Twain"
    and the "Bible" search phrases."""
    mark_twain = "Mark Twain"
    bible = "Bible"
    print("Starting loading {} and {} phrases".format(mark_twain, bible))
    load_records(mark_twain)
    load_records(mark_twain, 11)
    load_records(bible)
    load_records(bible, 11)


def process_voyager_xml(voyager_url):
    """Function takes the raw z3950 output from LOC's Voyager ILS and 
    iterates through all records

    Args: 
        voyager_url -- z3950 url
    """
        context = etree.iterparse(urllib.request.urlopen(voyager_url), events=('end',))
    for action, element in context:
        tag = str(element.tag)
        if tag.endswith('holding'):
            process_holding(element)
        if tag.endswith('record'):
            process_record(element)
        
def xquery_socket(raw_xml):
    """Function takes raw_xml and converts to BIBFRAME RDF"""
    XQUERY_SERVER.connect(('localhost', 8089))
    XQUERY_SERVER.sendall(raw_xml.encode() + b'\n')
    rdf_xml = b''
    while 1:
        data = XQUERY_SERVER.recv(1024)
        if not data:
            break
        rdf_xml += data
    XQUERY_SERVER.close()
    bf_graph = bibframe.default_graph()
    bf_graph.parse(rdf_xml, format='xml')
    return bf_graph

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Library of Congress Sample Ingester')
    parser.add_argument('load', help='Action')
    args = parser.parse_args()
    if args.load:
        load_reporting_samples()
     
        
