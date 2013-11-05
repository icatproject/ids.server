package org.icatproject.ids.integration.two;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeFactory;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataNotOnlineException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.junit.BeforeClass;
import org.junit.Test;

public class PutTest extends BaseTest {

	private static long timestamp;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
	}

	@Test(expected = DataNotOnlineException.class)
	public void putToUnrestoredDataset() throws Exception {

		testingClient.put(sessionId, Files.newInputStream(newFileLocation), "uploaded_file1_"
				+ timestamp, datasetIds.get(0), supportedDatafileFormat.getId(), null, 404);
	}

	@Test
	public void putOneFileTest() throws Exception {

		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", datasetIds.get(0));
		String uploadedLocation = new File(icatDs.getLocation(), "uploaded_file2_" + timestamp)
				.getPath();
		Path fileOnFastStorage = setup.getStorageDir().resolve(uploadedLocation);
		Path dirOnFastStorage = setup.getStorageDir().resolve(icatDs.getLocation());
		assertFalse(Files.exists(fileOnFastStorage));

		Path zipOnSlowStorage = setup.getStorageArchiveDir().resolve(icatDs.getLocation());

		assertFalse(Files.exists(dirOnFastStorage));
		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 200);

		while (!Files.exists(dirOnFastStorage)) {
			Thread.sleep(1000);
		}

		Files.delete(zipOnSlowStorage); // to check, if the dataset really is going to be written

		Long dfid = testingClient.put(sessionId, Files.newInputStream(newFileLocation),
				"uploaded_file2_" + timestamp, datasetIds.get(0), supportedDatafileFormat.getId(),
				"A rather splendid datafile", 201);

		Datafile df = (Datafile) icat.get(sessionId, "Datafile", dfid);
		assertEquals("A rather splendid datafile", df.getDescription());
		assertNull(df.getDoi());
		assertNull(df.getDatafileCreateTime());
		assertNull(df.getDatafileModTime());

		dfid = testingClient.put(sessionId, Files.newInputStream(newFileLocation),
				"uploaded_file3_" + timestamp, datasetIds.get(0), supportedDatafileFormat.getId(),
				"An even better datafile", "7.1.3", new Date(420000), new Date(42000), 201);
		df = (Datafile) icat.get(sessionId, "Datafile", dfid);
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

		do {
			Thread.sleep(1000);
		} while (!Files.exists(fileOnFastStorage));

		testingClient.archive(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 200);

		while (Files.exists(fileOnFastStorage)) {
			Thread.sleep(1000);
		}

	}
}
