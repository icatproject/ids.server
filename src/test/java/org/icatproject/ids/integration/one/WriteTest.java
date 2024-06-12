package org.icatproject.ids.integration.one;

import org.junit.BeforeClass;
import org.junit.Test;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.NotImplementedException;

public class WriteTest extends BaseTest {

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup("one.properties");
        icatsetup();
    }

    @Test(expected = NotImplementedException.class)
    public void writeNotAvailableTest() throws Exception {
        testingClient.write(sessionId,
                new DataSelection().addDataset(datasetIds.get(0)), 501);
    }

}
