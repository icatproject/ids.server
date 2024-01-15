package org.icatproject.ids.integration.twodf;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.NotFoundException;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.icatproject.ids.integration.util.client.TestingClient.Status;

/*
 * Issue #63: Internal error is raised trying to restore a dataset
 * with datafiles not uploaded to IDS
 *
 * ids.server gets confused if datafiles do not exist in the storage,
 * e.g. they are created in ICAT without having location set.
 *
 * Desired behavior: such bogus datafiles should be ignored.
 */

public class BogusDatafileTest extends BaseTest {

    private static long timestamp = System.currentTimeMillis();

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup("twodf.properties");
        icatsetup();
    }

    @Before
    public void createBogusFiles() throws Exception {

        Dataset ds1 = (Dataset) icatWS.get(sessionId, "Dataset", datasetIds.get(0));
        Datafile dfb1 = new Datafile();
        dfb1.setName("dfbogus1_" + timestamp);
        dfb1.setFileSize(42L);
        dfb1.setDataset(ds1);
        dfb1.setId(icatWS.create(sessionId, dfb1));

        Dataset ds2 = (Dataset) icatWS.get(sessionId, "Dataset", datasetIds.get(1));
        Datafile dfb2 = new Datafile();
        dfb2.setName("dfbogus2_" + timestamp);
        dfb2.setFileSize(42L);
        dfb2.setDataset(ds2);
        dfb2.setId(icatWS.create(sessionId, dfb2));

        Dataset ds3 = (Dataset) icatWS.get(sessionId, "Dataset", datasetIds.get(2));
        Datafile dfb3 = new Datafile();
        dfb3.setName("dfbogus3_" + timestamp);
        dfb3.setFileSize(42L);
        dfb3.setDataset(ds3);
        dfb3.setId(icatWS.create(sessionId, dfb3));

        datafileIds.add(dfb1.getId());
        datafileIds.add(dfb2.getId());
        datafileIds.add(dfb3.getId());

    }

    @Test
    public void getEmptyDataset() throws Exception {

        DataSelection selection = new DataSelection().addDataset(datasetIds.get(2));

        try (InputStream stream = testingClient.getData(sessionId, selection, Flag.NONE, 0, null)) {
            checkZipStream(stream, Collections.<Long>emptyList(), 57, 0);
        }

    }

    @Test
    public void getSizeEmptyDataset() throws Exception {

        DataSelection selection = new DataSelection().addDataset(datasetIds.get(2));
        assertEquals(0L, testingClient.getSize(sessionId, selection, 200));

    }

    @Test
    public void getNonEmptyDataset() throws Exception {

        DataSelection selection = new DataSelection().addDataset(datasetIds.get(0));

        testingClient.restore(sessionId, selection, 204);

        waitForIds();
        assertEquals(Status.ONLINE, testingClient.getStatus(sessionId, selection, null));

        try (InputStream stream = testingClient.getData(sessionId, selection, Flag.NONE, 0, null)) {
            checkZipStream(stream, datafileIds.subList(0, 2), 57, 0);
        }

    }

    @Test
    public void getSizeNonEmptyDataset() throws Exception {

        DataSelection selection = new DataSelection().addDataset(datasetIds.get(0));
        assertEquals(104L, testingClient.getSize(sessionId, selection, 200));

    }

    @Test(expected = NotFoundException.class)
    public void getBogusFile() throws Exception {

        DataSelection selection = new DataSelection().addDatafile(datafileIds.get(5));

        testingClient.restore(sessionId, selection, 404);

        waitForIds();
        assertEquals(Status.ONLINE, testingClient.getStatus(sessionId, selection, 404));

        try (InputStream stream = testingClient.getData(sessionId, selection, Flag.NONE, 0, 404)) {
            checkZipStream(stream, Collections.<Long>emptyList(), 57, 0);
        }

    }

    @Test(expected = NotFoundException.class)
    public void getSizeBogusFile() throws Exception {

        DataSelection selection = new DataSelection().addDatafile(datafileIds.get(5));
        testingClient.getSize(sessionId, selection, 404);

    }

    /*
     * Try the full cycle: upload a new file into a dataset having a bogus file, which triggers a write of the
     * datafile to archive storage, archive the dataset, and restore it.  Each step must deal gracefully with
     * the bogus file in the dataset.
     */
    @Test
    public void putWriteArchiveRestore() throws Exception {

        Long dsId = datasetIds.get(0);
        DataSelection selection = new DataSelection().addDataset(dsId);
        Path dirOnFastStorage = getDirOnFastStorage(dsId);

        testingClient.restore(sessionId, selection, 204);
        waitForIds();
        assertTrue(Files.exists(dirOnFastStorage));

        Long dfulId = testingClient.put(sessionId, Files.newInputStream(newFileLocation),
                "uploaded_file_" + timestamp, dsId, supportedDatafileFormat.getId(),
                "A rather splendid datafile", 201);
        Datafile dful = (Datafile) icatWS.get(sessionId, "Datafile", dfulId);
        testingClient.archive(sessionId, selection, 204);
        waitForIds();
        assertFalse(Files.exists(dirOnFastStorage));

        testingClient.restore(sessionId, selection, 204);
        waitForIds();
        assertTrue(Files.exists(dirOnFastStorage));

        File[] filesList = dirOnFastStorage.toFile().listFiles();
        assertEquals(3, filesList.length);
        Set<String> locations = new HashSet<>();
        Datafile df1 = (Datafile) icatWS.get(sessionId, "Datafile", datafileIds.get(0));
        locations.add(getLocationFromDigest(df1.getId(), df1.getLocation()));
        Datafile df2 = (Datafile) icatWS.get(sessionId, "Datafile", datafileIds.get(1));
        locations.add(getLocationFromDigest(df2.getId(), df2.getLocation()));
        locations.add(getLocationFromDigest(dful.getId(), dful.getLocation()));
        for (File file : filesList) {
            String location = setup.getStorageDir().relativize(file.toPath()).toString();
            assertTrue(locations.contains(location));
        }

    }

}
