import argparse
import re
import socket
import os
import json
import csv
from kazoo.client import KazooClient

def parse_args():
    parser = argparse.ArgumentParser('Script is used to generate principals for zookeeper and kafka.')
    parser.add_argument('-z', '--zookeeper', type=str, action='store',
                        nargs='+', help='Zookeeper servers', required=True)
    parser.add_argument('-zp', '--zookeeper-port', type=int, help='Zookeeper port', default=2181)
    parser.add_argument('-d', '--domain', type=str, help='Kerberos domain', required=True)
    parser.add_argument('-ks', '--kafka-service-name', type=str, help='Service name for kafka',
                        required=False, default='kafka')
    parser.add_argument('-zs', '--zookeeper-service-name', type=str, help='Service name for zookeeper',
                        required=False, default='zookeeper')
    parser.add_argument('-dns', '--check-dns', type=bool, help='Check that all of the hosts are resolvable.',
                        required=False, default=True)
    parser.add_argument('-kp', '--kafka-path', type=str, help='Path in zookeeper for kafka.',
                        required=False, default='/')
    parser.add_argument('-o', '--output', type=str, help='Output path for csv.', required=True)
    return parser.parse_args()

args = parse_args()

zookeeper_endpoints = []
# Validate the hostnames. No localhost, 127.0.0.1
for zookeeper_node in args.zookeeper:
    if re.match('^(127\.0\.0\.1|localhost|::1)$', zookeeper_node):
        raise Exception("Zookeeper hostnames cannot be localhost|127.0.0.1|::1")
    zookeeper_endpoint = '{0}:{1}'.format(zookeeper_node, args.zookeeper_port)
    zookeeper_endpoints.append(zookeeper_endpoint)

zookeeper_connection_string = ','.join(zookeeper_endpoints)
zookeeper_broker_root = os.path.join(args.kafka_path, 'brokers/ids')

zk = KazooClient(hosts=zookeeper_connection_string)
zk.start()

children = zk.get_children(zookeeper_broker_root)

broker_nodes=[]

for broker_id in children:
    zookeeper_broker_id_path = os.path.join(zookeeper_broker_root, broker_id)
    print('Reading broker info from {0}'.format(zookeeper_broker_id_path))
    zookeeper_node = zk.get(zookeeper_broker_id_path)
    zookeeper_node_string = zookeeper_node[0].decode('utf-8')
    zookeeper_node_json = json.loads(zookeeper_node_string)
    broker_hostname = zookeeper_node_json['host']

    broker_nodes.append(
            (broker_hostname,
             args.kafka_service_name,
             '{0}/{1}@{2}'.format(args.kafka_service_name, broker_hostname, args.domain)
             )
    )

zk.stop()

with open(args.output, 'w') as csvfile:
    csv_writer = csv.writer(csvfile, delimiter=',')
    csv_writer.writerow(('hostname', 'service_name', 'principal'))
    for broker_data in broker_nodes:
        csv_writer.writerow(broker_data)

print('Discovered {0} broker(s).'.format(len(broker_nodes)))