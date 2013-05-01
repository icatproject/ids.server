ICAT Data Service (IDS)
=======================

A setup script has been provided to help configure the IDS.


Prerequisites
=============

# 1. Glassfish installed and domain1 running:
<GLASSFISH_HOME>/glassfish/bin/asadmin start-domain domain1

# 2. If you are not using the Derby database ensure the appropriate driver is in 
<GLASSFISH_HOME>/glassfish/domains/domain1/lib/. Glassfish will need to be
restarted after the driver is put there. You will also need write access to a
database/schema.


Installation
============

# 1. Configure ids_glassfish.props. See 'Configuration Files' for more information.


# 2. Configure ids.properties. See 'Configuration Files' for more information.


# 3. Create the connection pool, resources and queues. Also copy the 
#    ids.properties file into place (only if it does not already exist) using:

./ids_setup.py --create


# 4. Deploy the IDS to Glassfish using:

./ids_setup.py --deploy


# 5. Check the status of the deployment using:

./ids_setup.py --status


# 6. If the ICATS you are connecting to are using non standard certificates you
#    will need to add them to the Glassfish trust store:

openssl s_client -showcerts -connect <HOST>:<PORT> </dev/null | sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > <GLASSFISH_HOME>/glassfish/domains/domain1/config/facility.cert
keytool -import -noprompt -alias <ALIAS> -file <GLASSFISH_HOME>/glassfish/domains/domain1/config/facility.cert -keystore <GLASSFISH_HOME>/glassfish/domains/domain1/config/cacerts.jks --storepass changeit


# 7. If you added certificates to the trust store you must restart Glassfish:

asadmin restart-domain

# When you are finished:

# 8. Undeploy the IDS from Glassfish using:

./ids_setup.py --undeploy


# 9. Delete connection pool, resources and queues using:

./ids_setup.py --delete


Configuration Files
===================

The IDS uses 3 configuration files. One for glassfish and two
for the application. Each one is explained in more detail below.


ids_glassfish.props
~~~~~~~~~~~~~~~~~~~

The keys in this file are:
    dbType (optional)
    driver
    connectionProperties
    glassfish
    port (optional)
	requestTimeout

A) dbType (optional, default:derby) - The type of database to use, i.e.:
    derby
    mysql
    oracle
    
B) driver - The database driver to be used, e.g.
    for Derby:
        org.apache.derby.jdbc.ClientDataSource
    for Oracle:
        oracle.jdbc.pool.OracleDataSource
    for MySQL:
        com.mysql.jdbc.jdbc2.optional.MysqlDataSource

C) connectionProperties - The IDS connection properties, e.g.
    for Derby:
        Password=APP:User=APP:serverName=localhost:DatabaseName=dm: \
            connectionAttributes=";"create"'"="'"true
    for Oracle:
        url="'"jdbc:oracle:thin:@//localhost:1521/XE"'": \
        ImplicitCachingEnabled=true:MaxStatements=200:user=dm:password=dmpassword
    for MySQL:
        user=dm:password=dmpassword:databaseName=dm

D) glassfish - The Glassfish home directory, must contain "glassfish/domains"

E) port (optional, default:4848) - The port for glassfish admin calls (normally 4848)

F) requestTimeout - The request timeout specifies in seconds the maximum time Glassfish
   will keep a connection open. Warning: setting this value too low can result in 
   downloads being cut off before they have finished. Set to -1 to disable timeout.


ids.properties
~~~~~~~~~~~~~~

The keys in this file are:
    ICAT_URL
    NUMBER_OF_DAYS_TO_EXPIRE
    MAX_NUMBER_OF_THREADS
    TEMPORARY_STORAGE_PATH
    STORAGE_TYPE
    LOCAL_STORAGE_PATH
    NUMBER_OF_DAYS_TO_KEEP_FILES_IN_CACHE

A) ICAT_URL
   The URL pointing to a running ICAT eg. http://example.com:8080/ICATService/ICAT?wsdl

B) TEMPORARY_STORAGE_PATH
   Location of the ZIP files containing datafiles for each download request.

C) STORAGE_TYPE
   Can either be LOCAL or STORAGED.

D) LOCAL_STORAGE_PATH
   Local location of datafiles once they have been retrieved using the storage plugin. If using 
   STORAGE_TYPE=LOCAL, this is where you store all of your datafiles. 

E) NUMBER_OF_DAYS_TO_KEEP_FILES_IN_CACHE
   How long files will stay in LOCAL_STORAGE_PATH. This has no effect when using STORAGE_TYPE=LOCAL.

F) NUMBER_OF_DAYS_TO_EXPIRE
   How long a download request will remaining in the database. After this time the entry will
   automatically be removed from the database as well as any associated ZIP files.


Log File
========

Messages are logged in <GLASSFISH_HOME>glassfish/domains/domain1/log/server.log
