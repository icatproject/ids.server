package org.icatproject.ids.integration.one;

import java.util.Arrays;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.icatproject.ids.integration.util.client.NotFoundException;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetStatusForPreparedIdTest extends BaseTest {

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup("one.properties");
        icatsetup();
    }

    @Test(expected = BadRequestException.class)
    public void badPreparedIdFormatTest() throws Exception {
        testingClient.isPrepared("bad preparedId format", 400);
    }

    @Test(expected = NotFoundException.class)
    public void nonExistingPreparedIdTest() throws Exception {
        testingClient.isPrepared("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", 404);
    }

    @Test(expected = NotFoundException.class)
    public void notFoundIdsTest() throws Exception {
        testingClient
                .prepareData(sessionId,
                        new DataSelection().addDatafiles(Arrays.asList(1L, 2L, 3L, 99999L)),
                        Flag.NONE, 404);
    }

    @Test(expected = NotFoundException.class)
    public void notFoundSingleIdTest() throws Exception {
        testingClient.prepareData(sessionId, new DataSelection().addDatafile(99999L), Flag.NONE,
                404);
    }

    @Test(expected = NotFoundException.class)
    public void notFoundDatasetSingleIdTest() throws Exception {
        testingClient
                .prepareData(sessionId, new DataSelection().addDataset(99999L), Flag.NONE, 404);
    }

    @Test(expected = InsufficientPrivilegesException.class)
    public void forbiddenTest() throws Exception {
        testingClient.prepareData(setup.getForbiddenSessionId(),
                new DataSelection().addDatafiles(datafileIds), Flag.NONE, 403);
    }

    @Test
    public void correctBehaviourTest() throws Exception {
        String preparedId = testingClient.prepareData(sessionId,
                new DataSelection().addDatafiles(datafileIds), Flag.NONE, 200);

        while (!testingClient.isPrepared(preparedId, 200)) {
            Thread.sleep(1000);
        }
    }

}
