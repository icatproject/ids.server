package org.icatproject.ids.integration.one;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;

public class LinkTest extends BaseTest {

    private static Path here;

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup("one.properties");
        icatsetup();
        here = Paths.get("").toAbsolutePath();
    }

    @Test
    public void getLink() throws Exception {
        Path link = here.resolve("alink");
        link = testingClient.getLink(sessionId, datafileIds.get(0), System.getProperty("user.name"), 200);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Files.copy(link, baos);
        assertEquals("df1 test content very compressible very compressible", baos.toString());
        Files.delete(link);
    }
}
