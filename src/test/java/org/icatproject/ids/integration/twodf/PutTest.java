package org.icatproject.ids.integration.twodf;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.GregorianCalendar;
import javax.xml.datatype.DatatypeFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;

import org.icatproject.Datafile;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;

public class PutTest extends BaseTest {

    private static long timestamp;

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup("twodf.properties");
        icatsetup();
    }

    @Test
    // Works fine for datafile storage
    public void putToUnrestoredDataset() throws Exception {
        testingClient.put(sessionId, Files.newInputStream(newFileLocation), "uploaded_file1_"
                + timestamp, datasetIds.get(0), supportedDatafileFormat.getId(), null, 201);
    }

    @Test
    public void putOneFileTest() throws Exception {
        Path dirOnFastStorage = getDirOnFastStorage(datasetIds.get(0));

        assertFalse(Files.exists(dirOnFastStorage));
        testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 204);

        waitForIds();

        assertTrue(Files.exists(dirOnFastStorage));

        Long dfid = testingClient.put(sessionId, Files.newInputStream(newFileLocation),
                "uploaded_file2_" + timestamp, datasetIds.get(0), supportedDatafileFormat.getId(),
                "A rather splendid datafile", 201);

        waitForIds();

        Datafile df = (Datafile) icatWS.get(sessionId, "Datafile", dfid);
        assertEquals("A rather splendid datafile", df.getDescription());
        assertNull(df.getDoi());
        assertNull(df.getDatafileCreateTime());
        assertNull(df.getDatafileModTime());

        dfid = testingClient.put(sessionId, Files.newInputStream(newFileLocation),
                "uploaded_file3_" + timestamp, datasetIds.get(0), supportedDatafileFormat.getId(),
                "An even better datafile", "7.1.3", new Date(420000), new Date(42000), 201);
        df = (Datafile) icatWS.get(sessionId, "Datafile", dfid);
        assertEquals("An even better datafile", df.getDescription());
        assertEquals("7.1.3", df.getDoi());

        DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        gregorianCalendar.setTime(new Date(420000));
        assertEquals(datatypeFactory.newXMLGregorianCalendar(gregorianCalendar),
                df.getDatafileCreateTime());
        gregorianCalendar.setTime(new Date(42000));
        assertEquals(datatypeFactory.newXMLGregorianCalendar(gregorianCalendar),
                df.getDatafileModTime());

        waitForIds();

        assertTrue(Files.exists(dirOnFastStorage));

        testingClient.archive(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 204);

        waitForIds();

        assertFalse(Files.exists(dirOnFastStorage));

    }

}
