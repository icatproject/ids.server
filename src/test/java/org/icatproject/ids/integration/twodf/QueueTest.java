package org.icatproject.ids.integration.twodf;

import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueueTest extends BaseTest {

    private static long timestamp = System.currentTimeMillis();

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup("twodf.properties");
        icatsetup();
    }

    /*
     * Arrange for multiple different operations, requiring conflicting
     * locks on the same dataset to be processed at the same time.  This
     * triggers Bug #82.
     */
    @Test
    public void multiOperationTest() throws Exception {
        Long dsId = datasetIds.get(0);
        Path dirOnFastStorage = getDirOnFastStorage(dsId);
        DataSelection selection = new DataSelection().addDataset(dsId);

        testingClient.restore(sessionId, selection, 204);
        waitForIds();
        assertTrue(Files.exists(dirOnFastStorage));

        testingClient.put(
            sessionId,
            Files.newInputStream(newFileLocation),
            "uploaded_file_" + timestamp,
            dsId,
            supportedDatafileFormat.getId(),
            "A rather splendid datafile",
            201
        );
        testingClient.archive(sessionId, selection, 204);
        waitForIds();
    }
}
