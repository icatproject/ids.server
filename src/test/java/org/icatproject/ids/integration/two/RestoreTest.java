package org.icatproject.ids.integration.two;

import java.nio.file.Path;

import org.junit.BeforeClass;
import org.junit.Test;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;

public class RestoreTest extends BaseTest {

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup("two.properties");
        icatsetup();
    }

    @Test
    public void restoreArchivedDataset() throws Exception {

        Path dirOnFastStorage = getDirOnFastStorage(datasetIds.get(0));

        testingClient.restore(sessionId,
                new DataSelection().addDataset(datasetIds.get(0)), 204);

        waitForIds();

        checkPresent(dirOnFastStorage);

    }

    @Test
    public void restoreTwoArchivedDatasets() throws Exception {
        Path dirOnFastStorage1 = getDirOnFastStorage(datasetIds.get(0));
        Path dirOnFastStorage2 = getDirOnFastStorage(datasetIds.get(1));
        testingClient.restore(sessionId, new DataSelection()
                .addDataset(datasetIds.get(0)).addDataset(datasetIds.get(1)),
                204);

        waitForIds();
        checkPresent(dirOnFastStorage1);
        checkPresent(dirOnFastStorage2);
    }

    @Test
    public void restoreArchivedDatafile() throws Exception {

        Path dirOnFastStorage = getDirOnFastStorage(datasetIds.get(0));

        testingClient.restore(sessionId,
                new DataSelection().addDatafile(datafileIds.get(0)), 204);
        waitForIds();

        checkPresent(dirOnFastStorage);

    }

    @Test
    public void restoreArchivedDatafileAndItsDataset() throws Exception {

        Path dirOnFastStorage = getDirOnFastStorage(datasetIds.get(0));

        testingClient.restore(sessionId, new DataSelection()
                .addDatafile(datafileIds.get(0)).addDataset(datasetIds.get(0)),
                204);

        waitForIds();

        checkPresent(dirOnFastStorage);

    }
}
