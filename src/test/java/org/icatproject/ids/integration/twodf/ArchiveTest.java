package org.icatproject.ids.integration.twodf;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;

public class ArchiveTest extends BaseTest {

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup("twodf.properties");
        icatsetup();
    }

    @Test
    public void restoreThenArchiveDataset() throws Exception {

        Path dirOnFastStorage = getDirOnFastStorage(datasetIds.get(0));

        assertFalse(Files.exists(dirOnFastStorage));

        testingClient.restore(sessionId,
                new DataSelection().addDataset(datasetIds.get(0)), 204);

        waitForIds();

        assertTrue(Files.exists(dirOnFastStorage));

        testingClient.archive(sessionId,
                new DataSelection().addDataset(datasetIds.get(0)), 204);

        waitForIds();
        assertFalse(Files.exists(dirOnFastStorage));

    }

    @Test(expected = BadRequestException.class)
    public void badSessionIdFormatTest() throws Exception {
        testingClient.archive("bad sessionId format",
                new DataSelection().addDatafiles(Arrays.asList(1L, 2L)), 400);
    }

    @Test
    public void noIdsTest() throws Exception {
        testingClient.archive(sessionId, new DataSelection(), 204);
    }

    @Test(expected = InsufficientPrivilegesException.class)
    public void nonExistingSessionIdTest() throws Exception {
        testingClient.archive("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                new DataSelection().addDatafiles(Arrays.asList(1L, 2L)), 403);
    }

}
