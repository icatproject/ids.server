#!/usr/bin/env python
"""
Set up the ICAT Data Service (IDS)
"""

from subprocess import call
from os import path
from os import listdir
from os import environ
from tempfile import NamedTemporaryFile
from tempfile import TemporaryFile
from shutil import copyfile
from optparse import OptionParser
from sys import exit
import re
from os import getcwd

# Variables 
APPLICATION_NAME = "IDS-0.0"

GF_PROPS_FILE = "ids_glassfish.props"
GF_REQ_VALUES = ["connectionProperties", "driver", "glassfish", "requestTimeout"]

PROPS_FILES = ["ids.properties"]

SUPPORTED_DATABASES = {"DERBY":'', "MYSQL":'', "ORACLE":''}

# Do NOT change, this value is required by the IDS
CONNECTION_POOL_ID = 'ids'


def get_and_validate_props(file_name, req_values):
    """
    The get_and_validate_props function gets the properties and validate them
    by calling the get_props and check_keys functions.
    """
    props_dict = get_props(file_name)
    check_keys(props_dict, req_values, file_name)
    return props_dict


def get_props(file_name):
    """
    The get_props function checks if the GF_PROPS_FILE file exists and then puts it
    into a Dictionary 
    """ 
    props_dict = {}
    if  not  path.exists(file_name):
        print ("There is no file " + file_name)
        exit(1)
    elif VERBOSE > 1:
        print ("Reading props from " + str(file_name))
    try:
        file_handle = open(file_name, 'r')
        for line in file_handle:
            line = line.strip()
            if line.startswith("#") or line == "":
                continue
            try:
                key, value = line.split("=", 1)
            except ValueError:
                print ("WARNING skipping value in " + str(file_name)
                       + " value:" + line)
            props_dict[key] = value
            if VERBOSE > 2:
                print ("prop " + str(key) + "=" + str(value))
    finally:
        file_handle.close()
    return props_dict    

    
def check_keys(props_dict, required_keys, file_name):
    """
    The check_keys function checks if the properties have all been configured

    """
    for key in required_keys:
        if not props_dict.has_key(key):
            print (key + " must be set in the file " + file_name)
            exit(1)
    return

        
def add_optional_props(props_dict):
    """
    The add_optional_props function checks if optional properties have been
    configured, if not then they're set with the default values.
    """   
    if not props_dict.has_key("domain"):
        props_dict["domain"] = 'domain1'
        if VERBOSE > 2:
            print ("Set domain to " + str(props_dict["domain"]))
    if not props_dict.has_key("port"):
        props_dict["port"] = 4848
        if VERBOSE > 2:
            print ("Set port to " + str(props_dict["port"]))
    if not props_dict.has_key("dbType"):
        props_dict["dbType"] = 'DERBY'
        if VERBOSE > 2:
            print ("Set dbType to " + str(props_dict["dbType"]))
    if not SUPPORTED_DATABASES.has_key(props_dict["dbType"].upper()):
        print ("ERROR " + props_dict["dbType"] + 
               " not supported. Supported databases are: ")
        for key in SUPPORTED_DATABASES.keys():
            print "    " + key
        exit(1)
    return props_dict

 
                
def install_props_file():
    """
    Copy the properties files list in PROPS_FILES
    """
    dest_dir = path.join(GF_PROPS["glassfish"], "glassfish", "domains",
                     GF_PROPS["domain"], "config")
    if not path.exists(dest_dir):
        print "ERROR Cannot find the directory " + dest_dir
        exit(1)
            
    for props in PROPS_FILES:
        destination = path.join(dest_dir, props)
        if path.exists(destination):
            print ("Found existing " + props + " in " + str(dest_dir) 
               + " new file not copied")
        else:
            if not path.exists(props):
                print "ERROR Cannot find " + props + " in the current directory"
                exit(1)
            copyfile(props, destination) 
            if VERBOSE > 0:
                print "copied " + props + " to " + str(destination)
            

def start_derby():
    """
    Ensure the derby database is running
    """
    if VERBOSE > 0:
        print "Ensure the derby database is running"
    command = ASADMIN + " start-database --dbhost 127.0.0.1"
    if VERBOSE > 1:
        print command
    if VERBOSE > 2:
        retcode = call(command, shell=True)
    else:
        retcode = call(command, shell=True, stdout=TemporaryFile()) 
    if retcode > 0:
        print "ERROR starting Derby database"
        exit(1)
#    out_file.close()



def asadmin_cmd(command, errormsg, verbose=0):
    if verbose > 2:
        print ASADMIN + " " + command
    if verbose > 1:
        retcode = call(ASADMIN + " " + command, shell=True)
    else:
        retcode = call(ASADMIN + " " + command, shell=True, stdout=TemporaryFile()) 
    if retcode > 0:
        print errormsg
        exit(1)
    

def create(conf_props):
    """
    Create the database connection pool and resource
    """
    if VERBOSE > 0:
        print "Create the database connection pool and resource"
    install_props_file()
    if conf_props['dbType'].upper() == "DERBY":
        start_derby()

    # Set up required resources 
    asadmin_cmd("create-jdbc-connection-pool --datasourceclassname " + conf_props["driver"] + " --restype javax.sql.DataSource --failconnection=true --steadypoolsize 2 --maxpoolsize 8 --ping --property " + conf_props["connectionProperties"] + " " + CONNECTION_POOL_ID, "Error creating JDBC connection pool", VERBOSE)          
    asadmin_cmd("create-jdbc-resource --connectionpoolid " + CONNECTION_POOL_ID + " " + "jdbc/" + CONNECTION_POOL_ID, "Error creating JDBC resource", VERBOSE)
    asadmin_cmd("create-admin-object --raname jmsra --restype javax.jms.Queue --property Name=InfoRetrievalQueue jms/IDS/InfoRetrievalQueue", "Error creating InfoRetrievalQueue admin object", VERBOSE)
    asadmin_cmd("create-connector-connection-pool --raname jmsra --connectiondefinition javax.jms.QueueConnectionFactory jms/IDS/InfoRetrievalQueueFactoryPool", "Error creating InfoRetrievalQueue connection pool", VERBOSE)
    asadmin_cmd("create-connector-resource --poolname jms/IDS/InfoRetrievalQueueFactoryPool jms/IDS/InfoRetrievalQueueFactory", "Error creating InfoRetrievalQueue", VERBOSE)
    asadmin_cmd("create-admin-object --raname jmsra --restype javax.jms.Queue --property Name=DataRetrievalQueue jms/IDS/DataRetrievalQueue", "Error creating DataRetrievalQueue admin object", VERBOSE)
    asadmin_cmd("create-connector-connection-pool --raname jmsra --connectiondefinition javax.jms.QueueConnectionFactory jms/IDS/DataRetrievalQueueFactoryPool", "Error creating DataRetrievalQueue connection pool", VERBOSE)
    asadmin_cmd("create-connector-resource --poolname jms/IDS/DataRetrievalQueueFactoryPool jms/IDS/DataRetrievalQueueFactory", "Error creating DataRetrievalQueue", VERBOSE)


def delete():
    """
    Delete the database connection pool and resource
    """
    asadmin_cmd("delete-jdbc-resource jdbc/" + CONNECTION_POOL_ID, "Error deleting JDBC resource", VERBOSE)
    asadmin_cmd("delete-jdbc-connection-pool " + CONNECTION_POOL_ID, "Error deleting connection pool", VERBOSE) 
    asadmin_cmd("delete-admin-object jms/IDS/InfoRetrievalQueue", "Error deleting InfoRetrievalQueue admin object", VERBOSE)
    asadmin_cmd("delete-connector-resource jms/IDS/InfoRetrievalQueueFactory", "Error deleting InfoRetrievalQueue", VERBOSE)
    asadmin_cmd("delete-connector-connection-pool --cascade jms/IDS/InfoRetrievalQueueFactoryPool", "Error deleting InfoRetrievalQueue connection pool", VERBOSE)
    asadmin_cmd("delete-admin-object jms/IDS/DataRetrievalQueue", "Error deleting DataRetrievalQueue admin object", VERBOSE)
    asadmin_cmd("delete-connector-resource jms/IDS/DataRetrievalQueueFactory", "Error deleting DataRetrievalQueue", VERBOSE)
    asadmin_cmd("delete-connector-connection-pool --cascade jms/IDS/DataRetrievalQueueFactoryPool", "Error deleting DataRetrievalQueue connection pool", VERBOSE)


def deploy():
    """
    Deploy the IDS application
    """
    if VERBOSE > 0:
        print "Deploying the IDS application"
    asadmin_cmd("deploy " + APPLICATION_NAME + ".war", "ERROR deploying " + APPLICATION_NAME + ".war", VERBOSE)
   
#    if VERBOSE > 0:
#        print "Disable the HTTP request timeout"
#    asadmin_cmd("set server-config.network-config.protocols.protocol.http-listener-1.http.request-timeout-seconds=" + CONF_PROPS_DM["requestTimeout"],
#                "ERROR disabling the HTTP request timeout", VERBOSE)
#    asadmin_cmd("set server-config.network-config.protocols.protocol.http-listener-2.http.request-timeout-seconds=" + CONF_PROPS_DM["requestTimeout"],
#                "ERROR disabling the HTTP request timeout", VERBOSE)


def undeploy():
    """
    Un-deploy the IDS application
    """
    if VERBOSE > 0:
        print "Undeploying the IDS application"
    asadmin_cmd("undeploy " + APPLICATION_NAME, "ERROR undeploying " + APPLICATION_NAME, VERBOSE)
    
#    if VERBOSE > 0:
#        print "Re-enabling the HTTP request timeout"
#    asadmin_cmd("set server-config.network-config.protocols.protocol.http-listener-1.http.request-timeout-seconds=900",
#                "ERROR re-enabling the HTTP request timeout", VERBOSE)
#    asadmin_cmd("set server-config.network-config.protocols.protocol.http-listener-2.http.request-timeout-seconds=900",
#                "ERROR re-enabling the HTTP request timeout", VERBOSE)


def status(conf_props):
    """
    display the status as reported by asadmin
    """
    print "Domains\n"
    asadmin_cmd("list-domains", "Error listing domains", 2)
    print "\nComponents\n"
    asadmin_cmd("list-components", "Error listing components", 2)
    print "\nJDBC connection pools\n"
    asadmin_cmd("list-jdbc-connection-pools", "Error listing JDBC connection pools", 2)
    print "\nJDBC resources\n"
    asadmin_cmd("list-jdbc-resources", "Error listing JDBC resources", 2)
    print "\nAdmin Objects\n"
    asadmin_cmd("list-admin-objects", "Error listing admin objects", 2)
    print "\nConnector Resources\n"
    asadmin_cmd("list-connector-resources", "Error listing connector resources", 2)
    print "\nConnector Connection Pools\n"
    asadmin_cmd("list-connector-connection-pools", "Error listing connector connection pools", 2)
    print "\nJMS Resources\n"
    asadmin_cmd("list-jms-resources", "Error listing JMS resources", 2)
    print "\nHTTP Request Timeout (seconds)\n"
    asadmin_cmd("get server-config.network-config.protocols.protocol.http-listener-1.http.request-timeout-seconds", "Error listing HTTP request timeout", 2)
    asadmin_cmd("get server-config.network-config.protocols.protocol.http-listener-2.http.request-timeout-seconds", "Error listing HTTP request timeout", 2)


PARSER = OptionParser()
PARSER.add_option("--create", dest="create",
                  help="Creates the database connection pool",
                  action="store_true")
PARSER.add_option("--delete", dest="delete",
                  help="Deletes the database connection pool",
                  action="store_true")
PARSER.add_option("--deploy", dest="deploy",
                  help="Deploys the IDS application to Glassfish",
                  action="store_true")
PARSER.add_option("--undeploy", dest="undeploy",
                  help="Undeploys the IDS application from Glassfish",
                  action="store_true")
PARSER.add_option("--status", dest="status",
                  help="Display status information",
                  action="store_true")
PARSER.add_option("-v", "--verbose", action="count", default=0,
                    help="increase output verbosity")

(OPTIONS, ARGS) = PARSER.parse_args()
VERBOSE = OPTIONS.verbose

GF_PROPS = get_and_validate_props(GF_PROPS_FILE, GF_REQ_VALUES)
GF_PROPS = add_optional_props(GF_PROPS)
 
ASADMIN = path.join(GF_PROPS["glassfish"], "bin", "asadmin")
# if windows:
#    ASADMIN = ASADMIN + ".bat"
ASADMIN = ASADMIN + " --port " + GF_PROPS["port"]
 
IJ = path.join(GF_PROPS["glassfish"], "javadb", "bin", "ij")
MYSQL = "mysql"

if OPTIONS.create:
    create(GF_PROPS)
elif OPTIONS.delete:
    delete()
elif OPTIONS.deploy:
    deploy()
elif OPTIONS.undeploy:
    undeploy()
elif OPTIONS.status:
    status(GF_PROPS)
else:
    PARSER.print_help()
    exit(1)
    
print ('All done')
exit(0)
