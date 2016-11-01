#! /usr/bin/python3
"""Demonstrate a race condition in IDS.

A DsWriteThenArchiver thread may be started for a dataset while
an upload to the same dataset is still in progress.

Note that timing is critical to reproduce this issue.  That is why the
actual work is done in separate threads with the main thread only
synchronizing and timing.
"""

from threading import Thread
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

# Time to wait after sending the archive request before finishing
# writing file02.dat.
# Variant 1: 70 seconds.  This was used to trigger Issue #47, but is
#            harmless now that this issue is fixed.
# Variant 2: 90 seconds.  This should demonstrate the effect of
#            locking in ids.server now.  E.g. the processing of the
#            ARCHIVE request should get delayed because the upload of
#            file02.dat is still in progress.
sleeptime = 90

# The following object are assumed to exist.  The caller of this
# script needs to have write access to this investigation,
# e.g. permission to create a dataset and to upload datafiles to the
# dataset.
investigation = client.assertedSearch("Investigation [name='12100409-ST']")[0]
datasetType = client.assertedSearch("DatasetType [name='raw']")[0]
datafileFormat = client.assertedSearch("DatafileFormat [name='raw']")[0]


dataset = client.new("dataset", type=datasetType, investigation=investigation)
dataset.name = "test_race_upload_archive"
dataset.complete = False
dataset.create()

step = 0
thread_list = []
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

class SlowDataSource(object):
    """A slow source of random data that keeps on drippling some data
    until the global variable step passes a given stop value.
    """
    def __init__(self, stop, chunksize=128):
        self.stop = stop
        self.chunksize = chunksize
    def read(self, n):
        global step
        if step >= self.stop:
            return bytes()
        else:
            sleep(1)
            if n > self.chunksize:
                n = self.chunksize
            return bytes(getrandbits(8) for _ in range(n))


# Note: I'm not sure whether Suds is thread safe.  Therefore I use a
# separate local client in each thread.  All these clients share the
# same session id.
def upload(n, data):
    lclient = icat.Client(conf.url, **conf.client_kwargs)
    lclient.sessionId = client.sessionId
    fname = "file%02d.dat" % n
    lclient.ids.put(data, fname, dataset.id, datafileFormat.id)
    lclient.sessionId = None


def archive():
    lclient = icat.Client(conf.url, **conf.client_kwargs)
    lclient.sessionId = client.sessionId
    lclient.ids.archive(DataSelection([dataset]))
    lclient.sessionId = None


# Step 1: Upload a file to the dataset.  The file should be fairly
# large to make sure that writing the archive file in step 4 takes
# some time, to allow step 5 to be finished before the
# DsWriteThenArchiver starts to delete the dataset from main storage.

step = 1
ult = Thread(target=upload, args=(1,SizedDataSource(200*MiB)))
thread_list.append(ult)
ult.start()


# Step 2: Start a slow upload of a file to the same dataset.

step = 2
t = Thread(target=upload, args=(2,SlowDataSource(5)))
thread_list.append(t)
t.start()


# Step 3: Wait for the first upload to be finished.  Call archive for
# the dataset.  The first upload will queue a WRITE deferred operation
# in the FiniteStateMachine.  The archive call will change this to
# WRITE_THEN_ARCHIVE.

ult.join()
step = 3
t = Thread(target=archive)
thread_list.append(t)
t.start()


# Step 4: Wait a little longer then 60 seconds (assuming this is the
# value of writeDelaySeconds in ids.properties).  Now the
# DsWriteThenArchiver will be started.

sleep(sleeptime)
step = 4


# Step 5: Finish the slow upload.  This queues another WRITE deferred
# operation.  The thread for the slow upload observes the global
# variable step; setting it to a value >= 5 finishes the upload.

step = 5


# After another 60 seconds the DsWriter will be started.  But since
# the DsWriteThenArchiver already deleted the dataset from
# main storage, the DsWriter will also delete the archive file.
# Booom -> All data is lost.

# Clean up.
for thread in thread_list:
    thread.join()
