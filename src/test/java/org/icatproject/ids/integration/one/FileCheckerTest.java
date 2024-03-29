package org.icatproject.ids.integration.one;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;

import org.icatproject.Datafile;
import org.icatproject.EntityBaseBean;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;

public class FileCheckerTest extends BaseTest {

    private static Path errorLog;

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup("one.properties");
        icatsetup();
        errorLog = setup.getErrorLog();
    }

    @Test
    public void everythingTest() throws Exception {

        List<Object> os = icatWS.search(sessionId, "Datafile");
        for (Object o : os) {
            icatWS.delete(sessionId, (EntityBaseBean) o);
        }

        Files.deleteIfExists(errorLog);

        Long dfid = 0L;
        for (int i = 0; i < 3; i++) {
            dfid = testingClient.put(sessionId, Files.newInputStream(newFileLocation), "uploaded_file_" + i,
                    datasetIds.get(0), supportedDatafileFormat.getId(), "A rather splendid datafile", 201);
        }

        Datafile df = (Datafile) icatWS.get(sessionId, "Datafile INCLUDE 1", dfid);

        assertFalse(Files.exists(errorLog));

        Long fileSize = df.getFileSize();
        String checksum = df.getChecksum();

        df.setFileSize(fileSize + 1);
        icatWS.update(sessionId, df);
        checkHas("Datafile", dfid, "file size wrong");

        df.setFileSize(fileSize);
        df.setChecksum("Aardvark");
        icatWS.update(sessionId, df);
        Files.deleteIfExists(errorLog);
        checkHas("Datafile", dfid, "checksum wrong");

        df.setChecksum(null);
        icatWS.update(sessionId, df);
        Files.deleteIfExists(errorLog);
        checkHas("Datafile", dfid, "checksum null");

        df.setChecksum(checksum);
        df.setLocation("Zoo");
        icatWS.update(sessionId, df);
        Files.deleteIfExists(errorLog);
        checkHas("Datafile", dfid, "Zoo\" does not contain hash.");

    }

    private void checkHas(String type, Long id, String message) throws IOException, InterruptedException {
        Set<String> lines = new HashSet<String>();
        while (!Files.exists(errorLog)) {
            Thread.sleep(10);
        }
        for (String line : Files.readAllLines(errorLog, Charset.defaultCharset())) {
            int n = line.indexOf(": ") + 2;
            lines.add(line.substring(n));
        }
        assertEquals(1, lines.size());
        String msg = new ArrayList<String>(lines).get(0);
        assertTrue(msg + ":" + type + " " + id, msg.startsWith(type + " " + id));
        assertTrue(msg + ":" + message, msg.endsWith(message));
    }
}
