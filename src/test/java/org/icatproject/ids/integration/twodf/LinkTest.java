package org.icatproject.ids.integration.twodf;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.Test;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataNotOnlineException;

public class LinkTest extends BaseTest {

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup("twodf.properties");
        icatsetup();
    }

    @Test
    public void getLink() throws Exception {

        String username = System.getProperty("user.name");
        try {
            testingClient.getLink(sessionId, datafileIds.get(0), username, 503);
            fail("Should have thrown an exception");
        } catch (DataNotOnlineException e) {
            // All is well
        }

        waitForIds();

        Path link = testingClient.getLink(sessionId, datafileIds.get(0), username, 200);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Files.copy(link, baos);
        assertEquals("df1 test content very compressible very compressible", baos.toString());
    }
}
