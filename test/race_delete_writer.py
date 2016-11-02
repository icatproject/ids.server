#! /usr/bin/python3
"""Test concurrency.

The FSM may want to start a DsWriter thread while a delete webservice
call is still in process.
"""

from time import sleep
from random import getrandbits
import icat
import icat.config
from icat.ids import DataSelection
import logging

logging.basicConfig(level=logging.INFO)
#logging.getLogger('suds.client').setLevel(logging.DEBUG)

conf = icat.config.Config(ids="mandatory").getconfig()
client = icat.Client(conf.url, **conf.client_kwargs)
client.login(conf.auth, conf.credentials)

investigation = client.assertedSearch("Investigation [name='12100409-ST']")[0]
datasetType = client.assertedSearch("DatasetType [name='raw']")[0]
datafileFormat = client.assertedSearch("DatafileFormat [name='raw']")[0]
dataset = client.new("dataset", type=datasetType, investigation=investigation)
dataset.name = "race_delete_writer"
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


# Step 1: Upload many small files.  The number of files should be
# large enough such that deleting them all takes some time so that
# there is at least a chance to hit this time window with trying to
# start the DsWriter.

numfiles = 800
allfiles = DataSelection()

for i in range(numfiles):
    dfid = client.ids.put(SizedDataSource(1*KiB), "file%05d.dat" % i, 
                          dataset.id, datafileFormat.id)
    allfiles.extend({'datafileIds': [dfid]})
# Add another one supposed to survive.
client.ids.put(SizedDataSource(1*KiB), "file%05d.dat" % numfiles, 
               dataset.id, datafileFormat.id)

# Step 2: Wait some time, but need to take into account that also the
# preparation of the delete of so many files takes some time.

sleep(27)

# Step 3: Delete the files.

try:
    client.ids.delete(allfiles)
except icat.IDSDataNotOnlineError:
    pass

# The DsWriter started in step 2 fails with a NoSuchFileException
# because one of the file has been deleted from main storage.  A
# corrupted ZIP file has been written to the dataset cache.  However,
# a second WRITE deferred operation triggered by the delete finally
# writes the ZIP file to archive storage successfully.

# Cleanup
sleep(70)
client.deleteData([dataset])
client.delete(dataset)
