These utility scripts are used to export the kerberos configuration from a running cluster and create the corresponding service principals in active directory.

## Export

This script connects to zookeeper and extracts the running configuration from zookeeper. This documentation on [Kafka Security](http://docs.confluent.io/2.0.1/kafka/security.html) and this [blog post](http://www.confluent.io/blog/apache-kafka-security-authorization-authentication-encryption) will be helpful.

```
export_principals.py --zookeeper zookeeper --domain EXAMPLE.COM --output principals.csv
```

## Create

This script connects to Active Directory and creates a user account with a random generated name. It will set the service principal based on the advertised hostname of the Kafka broker. For example `kafka/kafka-01.us-west-2.compute.internal@EXAMPLE.COM`. It also generates a snippet of the server.properties file, the kafka and zookeeper, jaas configuration.  

```
KafkaServer {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="/etc/security/keytabs/kafka.keytab"
    principal="kafka/ip-172-31-36-212.us-west-2.compute.internal@EXAMPLE.COM";
};

// ZooKeeper client authentication
Client {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="/etc/security/keytabs/kafka.keytab"
    principal="kafka/ip-172-31-36-212.us-west-2.compute.internal@EXAMPLE.COM";
};
```

```
Server {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab="/etc/security/keytabs/zookeeper.keytab"
  storeKey=true
  useTicketCache=false
  principal="zookeeper/ip-172-31-36-212.us-west-2.compute.internal@EXAMPLE.COM";
};
```

```
sasl.kerberos.service.name=kafka
zookeeper.set.acl=true
# When using kerberos zookeeper connect must use the FQDN of the zookeeper hosts.
zookeeper.connect=ip-172-31-36-214.us-west-2.compute.internal:2181,ip-172-31-36-212.us-west-2.compute.internal:2181,ip-172-31-36-215.us-west-2.compute.internal:2181
```


```
python create_principals.py --scheme ldaps --port 636 --host active directory host --username "CN=Some User,OU=services,DC=example,DC=com" --password password123! --root-dn "OU=services,DC=example,DC=com" --principals principals.csv --output security
```
