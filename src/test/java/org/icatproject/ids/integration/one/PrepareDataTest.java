package org.icatproject.ids.integration.one;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.util.List;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.junit.BeforeClass;
import org.junit.Test;

public class PrepareDataTest extends BaseTest {

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup("one.properties");
        icatsetup();
    }

    @Test(expected = BadRequestException.class)
    public void badSessionIdFormatTest() throws Exception {
        testingClient.prepareData("bad sessionId format", new DataSelection().addDataset(datasetIds.get(0)), Flag.NONE,
                400);
    }

    @Test(expected = BadRequestException.class)
    public void noIdsTest() throws Exception {
        testingClient.prepareData("bad sessionId format", new DataSelection(), Flag.NONE, 400);

    }

    @Test(expected = InsufficientPrivilegesException.class)
    public void nonExistingSessionIdTest() throws Exception {
        testingClient.prepareData("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                new DataSelection().addDataset(datasetIds.get(0)), Flag.NONE, 403);

    }

    @Test
    public void correctBehaviourTest() throws Exception {
        String preparedId = testingClient.prepareData(sessionId, new DataSelection().addDatafiles(datafileIds),
                Flag.NONE, 200);
        System.out.println(preparedId);
        assertNotNull(preparedId);
    }

    @Test
    public void prepareDataset() throws Exception {

        String preparedId = testingClient.prepareData(sessionId, new DataSelection().addDataset(datasetIds.get(0)),
                Flag.NONE, 200);

        List<Long> ids = testingClient.getDatafileIds(preparedId, 200);
        assertEquals(2, ids.size());
        assertTrue(ids.contains(datafileIds.get(0)));
        assertTrue(ids.contains(datafileIds.get(1)));

        while (!testingClient.isPrepared(preparedId, 200)) {
            Thread.sleep(1000);
        }

        assertTrue(Files.exists(setup.getPreparedCacheDir().resolve(preparedId)));
    }

    @Test
    public void prepareTwoDatasets() throws Exception {

        String preparedId = testingClient.prepareData(sessionId, new DataSelection().addDataset(datasetIds.get(0))
                .addDataset(datasetIds.get(1)), Flag.NONE, 200);

        List<Long> ids = testingClient.getDatafileIds(preparedId, 200);
        assertEquals(4, ids.size());
        for (Long id : datafileIds) {
            assertTrue(ids.contains(id));
        }

        while (!testingClient.isPrepared(preparedId, 200)) {
            Thread.sleep(1000);
        }

        assertTrue(Files.exists(setup.getPreparedCacheDir().resolve(preparedId)));
    }

    @Test
    public void prepareDatafile() throws Exception {

        String preparedId = testingClient.prepareData(sessionId, new DataSelection().addDatafile(datafileIds.get(0)),
                Flag.NONE, 200);

        List<Long> ids = testingClient.getDatafileIds(preparedId, 200);
        assertEquals(1, ids.size());
        assertTrue(ids.contains(datafileIds.get(0)));

        while (!testingClient.isPrepared(preparedId, 200)) {
            Thread.sleep(1000);
        }

        assertTrue(Files.exists(setup.getPreparedCacheDir().resolve(preparedId)));
    }

    @Test
    public void prepareDatafileAndItsDataset() throws Exception {

        String preparedId = testingClient.prepareData(sessionId, new DataSelection().addDatafile(datafileIds.get(0))
                .addDataset(datasetIds.get(0)), Flag.NONE, 200);

        List<Long> ids = testingClient.getDatafileIds(preparedId, 200);
        assertEquals(2, ids.size());
        assertTrue(ids.contains(datafileIds.get(0)));
        assertTrue(ids.contains(datafileIds.get(1)));

        while (!testingClient.isPrepared(preparedId, 200)) {
            Thread.sleep(1000);
        }

        assertTrue(Files.exists(setup.getPreparedCacheDir().resolve(preparedId)));
    }

}
