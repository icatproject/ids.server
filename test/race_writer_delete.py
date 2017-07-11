#! /usr/bin/python3
"""Demonstrate a race condition in IDS.

A delete webservice call may come in while a DsWriter thread is in
process causing it to fail.
"""

from time import sleep
from random import getrandbits
from distutils.version import StrictVersion as Version
import icat
import icat.config
from icat.ids import DataSelection
import logging

logging.basicConfig(level=logging.INFO)
#logging.getLogger('suds.client').setLevel(logging.DEBUG)

config = icat.config.Config(ids="mandatory")
if Version(icat.__version__) < '0.13':
    conf = config.getconfig()
    client = icat.Client(conf.url, **conf.client_kwargs)
else:
    client, conf = config.getconfig()
client.login(conf.auth, conf.credentials)

investigation = client.assertedSearch("Investigation [name='12100409-ST']")[0]
datasetType = client.assertedSearch("DatasetType [name='raw']")[0]
datafileFormat = client.assertedSearch("DatafileFormat [name='raw']")[0]
dataset = client.new("dataset", type=datasetType, investigation=investigation)
dataset.name = "race_writer_delete"
dataset.complete = False
dataset.create()

KiB = 1024
MiB = 1024*KiB

class SizedDataSource(object):
    """A source of random data with predefined size.
    """
    def __init__(self, size):
        self.size = size
    def read(self, n):
        if n < 0 or n > self.size:
            n = self.size
        self.size -= n
        return bytes(getrandbits(8) for _ in range(n))


# Step 1: Upload two files to the dataset.  The first one should be
# fairly large to make sure that writing the archive file in step 4
# takes some time.

dfid1 = client.ids.put(SizedDataSource(200*MiB), "file01.dat", 
                       dataset.id, datafileFormat.id)
dfid2 = client.ids.put(SizedDataSource(2*MiB), "file02.dat", 
                       dataset.id, datafileFormat.id)

# Step 2: Wait a little longer then 60 seconds (assuming this is the
# value of writeDelaySeconds in ids.properties).  Now the DsWriter
# will be started.

sleep(65)

# Step 3: Delete the second file.

try:
    client.ids.delete(DataSelection({'datafileIds': [dfid2]}))
except icat.IDSDataNotOnlineError:
    pass

# The DsWriter started in step 2 fails with a NoSuchFileException
# because one of the file has been deleted from main storage.  A
# corrupted ZIP file has been written to the dataset cache.  However,
# a second WRITE deferred operation triggered by the delete finally
# writes the ZIP file to archive storage successfully.

# Cleanup
sleep(30)
client.deleteData([dataset])
client.delete(dataset)
