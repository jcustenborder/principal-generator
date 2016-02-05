import argparse
import random
import string
import csv
import logging
import sys
import os
import time
from subprocess import Popen, PIPE

from ldap3 import *

def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)


def parse_args():
    parser = argparse.ArgumentParser('Script is used to generate principals for zookeeper and kafka.')
    parser.add_argument('-H', '--host', type=str, help='Active Directory Server to connect to', required=True)
    parser.add_argument('-p', '--port', type=int, help='LDAPS port to connect to', required=False, default=636)
    parser.add_argument('-s', '--scheme', type=str, help='Scheme to connect with.', required=False, default='ldaps')
    parser.add_argument('-u', '--username', type=str, help='Username to connect with.', required=True)
    parser.add_argument('-P', '--password', type=str, help='Password to connect with.', required=True)
    parser.add_argument('-r', '--root-dn', type=str, help='Root DN for ldap searches. All items will be created here.', required=True)
    parser.add_argument('-i', '--principals', type=str, help='Input file containing the principals to be created.', required=True)
    parser.add_argument('-e', '--enctype', type=str, help='Type of encryption to use. Supported types are aes128 or aes256', required=False, default='aes256')
    parser.add_argument('-z', '--zookeeper-port', type=str, help='Zookeeper port', required=False, default=2181)
    parser.add_argument('-o', '--output', type=str, help='Output path to write the keytab files to.', required=True)
    return parser.parse_args()

def write_server_properties(path, zookeeper_hosts):
    zookeeper_connect = ','.join(zookeeper_hosts)
    content ="""sasl.kerberos.service.name=kafka
zookeeper.set.acl=true
# When using kerberos zookeeper connect must use the FQDN of the zookeeper hosts.
zookeeper.connect={0}
""".format(zookeeper_connect)
    with open(path, "w") as text_file:
        text_file.write(content)

def write_zookeeper_server_jaas(principal, jaas_path):
    content = """Server {{
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab="/etc/security/keytabs/zookeeper.keytab"
  storeKey=true
  useTicketCache=false
  principal="{0}";
}};""".format(principal)
    with open(jaas_path, "w") as text_file:
        text_file.write(content)

def write_kafka_jaas(principal, jaas_path):
    content = """KafkaServer {{
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="/etc/security/keytabs/kafka.keytab"
    principal="{0}";
}};

// ZooKeeper client authentication
Client {{
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="/etc/security/keytabs/kafka.keytab"
    principal="{0}";
}};""".format(principal)
    with open(jaas_path, "w") as text_file:
        text_file.write(content)

def generate_account_name(prefix, length=14):
    diff = length - len(prefix)

    return '{0}{1}'.format(
        prefix,
        ''.join(random.sample(string.ascii_lowercase, diff))
    )

args = parse_args()
setup_logging()

encryption_type = None
encryption_flag = None

if args.enctype == 'aes128':
    encryption_type = 'aes128-cts-hmac-sha1-96'
    encryption_flag = 0x8
elif args.enctype == 'aes256':
    encryption_type = 'aes256-cts-hmac-sha1-96'
    encryption_flag = 0x10
else:
    raise Exception('enctype must be aes128 or aes256')

ldap_uri = '{0}://{1}:{2}'.format(args.scheme, args.host, args.port)
logging.info('Connecting to {0}'.format(ldap_uri))

conn = Connection(ldap_uri, user=args.username, password=args.password, auto_bind=AUTO_BIND_TLS_BEFORE_BIND, raise_exceptions=True)
conn.start_tls()
conn.bind()

keytabs = {}
zookeeper_hosts=[]



with open(args.principals) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        service_principal = row['principal']
        hostname = row['hostname']
        service_name = row['service_name']
        domain = row['domain']
        user_principal_name = '{0}@{1}'.format(service_principal, domain)
        logging.info('Searching for principal {0}'.format(service_principal))
        ldap_query = '(servicePrincipalName={0})'.format(service_principal)
        account_prefix = None
        if service_name == 'zookeeper':
            zookeeper_hosts.append('{0}:{1}'.format(hostname, args.zookeeper_port))
            account_prefix = 'zoo'
        elif service_name == 'kafka':
            account_prefix = 'kafka'
        else:
            raise Exception('service_name of kafka and zookeeper are supported.')


        if conn.search(args.root_dn, ldap_query):
            logging.info('{0} Exists...'.format(service_principal))
        else:
            logging.info("Principal '{0}' does not exist. Creating...".format(service_principal))
            account_name = generate_account_name(account_prefix)
            new_dn = 'CN={0},{1}'.format(account_name, args.root_dn)
            attributes = {
                'cn': account_name,
                'sAMAccountName': account_name,
                'msDS-SupportedEncryptionTypes': encryption_flag, #enable support for AES128 0x8, and AES256 0x16
                'description': 'Kerberos service principal for {0} on {1}'.format(service_name, hostname),
                'objectClass': ['top', 'person', 'organizationalPerson', 'user'],
                'servicePrincipalName': service_principal,
                'userPrincipalName': user_principal_name,
                'userAccountControl': 514
            }

            logging.info('Creating principal {0} - {1}'.format(account_name, service_principal))

            if conn.add(new_dn, 'user', attributes):
                logging.info('Successfully created {0} - {1}'.format(account_name, service_principal))
            else:
                logging.error('Error encountered while adding account {0} - {1}'.format(account_name, service_principal))

            # From https://github.com/cannatag/ldap3/blob/master/ldap3/extend/microsoft/modifyPassword.py
            new_password = ''.join(random.sample(string.ascii_letters + string.punctuation + string.digits, 32))
            encoded_new_password = ('"%s"' % new_password).encode('utf-16-le')
            password_attributes = {
                'unicodePwd': [(MODIFY_REPLACE, encoded_new_password)],
                'userAccountControl': [(MODIFY_REPLACE, 512 + 65536)]
            }

            logging.info('Setting password for {0}'.format(service_principal))

            if conn.modify(new_dn, password_attributes, None):
                logging.info('Successfully set password for {0}'.format(service_principal))
            else:
                logging.error('Error setting password for {0}'.format(service_principal))

            if not hostname in keytabs:
                keytabs[hostname] = []

            keytabs[hostname].append(
                    (service_name, service_principal, user_principal_name, new_password)
            )

logging.info('Starting ktutil')
ktutil = Popen(['ktutil'], stdin=PIPE)

for hostname, entries in keytabs.items():
    for (service_name, service_principal, user_principal_name, password) in entries:
        host_path = os.path.join(args.output, hostname)
        keytabs_path = os.path.join(host_path, 'etc', 'security', 'keytabs')
        kafka_path = os.path.join(host_path, 'etc', 'kafka')

        if not os.path.isdir(keytabs_path):
            os.makedirs(keytabs_path)
        if not os.path.isdir(kafka_path):
            os.makedirs(kafka_path)

        if service_name == 'zookeeper':
            zookeeper_jaas_path = os.path.join(kafka_path, 'zookeeper.jaas')
            write_zookeeper_server_jaas(user_principal_name, zookeeper_jaas_path)
        elif service_name == 'kafka':
            kafka_jaas_path = os.path.join(kafka_path, 'kafka_server.jaas')
            write_kafka_jaas(user_principal_name, kafka_jaas_path)
            kafka_properties_path = os.path.join(kafka_path, 'server.properties')
            write_server_properties(kafka_properties_path, zookeeper_hosts)

        principal_path = os.path.join(keytabs_path, service_name) + '.keytab'
        if os.path.exists(principal_path):
            logging.debug('Removing existing keytab file. {0}'.format(principal_path))
            os.remove(principal_path)
        logging.info('ktutil clear')
        ktutil.stdin.write('clear\n')
        time.sleep(1)
        addent = 'addent -password -p {0} -k 0 -e {1}\n'.format(user_principal_name, encryption_type)
        logging.info('ktutil {0}'.format(addent))
        ktutil.stdin.write(addent)
        time.sleep(1)
        ktutil.stdin.write('%s\n' % password)
        time.sleep(1)
        wkt = 'wkt {0}\n'.format(principal_path)
        logging.info('ktutil {0}'.format(wkt))
        ktutil.stdin.write('%s\n' % wkt)
        time.sleep(1)

logging.info('ktutil quit')
ktutil.stdin.write('quit\n')
