package org.icatproject.ids.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.namespace.QName;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.TreeDeleteVisitor;
import org.icatproject.ids.integration.util.client.DataNotOnlineException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.TestingClient;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PutTest {

	private static Setup setup = null;
	private static ICAT icat;
	TestingClient testingClient;

	private static long timestamp;
	private String sessionId;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup();
		ICATService icatService = new ICATService(setup.getIcatUrl(), new QName(
				"http://icatproject.org", "ICATService"));
		icat = icatService.getICATPort();
	}

	@Before
	public void clearFastStorage() throws Exception {
		Path storageDir = FileSystems.getDefault().getPath(setup.getStorageDir());
		Path storageZipDir = FileSystems.getDefault().getPath(setup.getStorageZipDir());
		TreeDeleteVisitor treeDeleteVisitor = new TreeDeleteVisitor();
		Files.walkFileTree(storageDir, treeDeleteVisitor);
		Files.walkFileTree(storageZipDir, treeDeleteVisitor);
		Files.createDirectories(storageDir);
		Files.createDirectories(storageZipDir);
		testingClient = new TestingClient(setup.getIdsUrl());
		sessionId = setup.getGoodSessionId();
	}

	@Test(expected = DataNotOnlineException.class)
	public void putToUnrestoredDataset() throws Exception {
		testingClient.put(sessionId, new FileInputStream(setup.getNewFileLocation()), "uploaded_file1_"
				+ timestamp, setup.getDatasetIds().get(0), setup.getSupportedDatafileFormat()
				.getId(), null, null, null, null, 404);
	}

	@Test
	public void putOneFileTest() throws Exception {

		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", setup
				.getDatasetIds().get(0));
		// this file will be uploaded
		String uploadedLocation = new File(icatDs.getLocation(), "uploaded_file2_" + timestamp)
				.getPath();
		File fileOnFastStorage = new File(setup.getStorageDir(), uploadedLocation);

		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(new File(setup.getStorageZipDir(), icatDs.getLocation()),
				"files.zip");
		File zipOnSlowStorage = new File(new File(setup.getStorageArchiveDir(),
				icatDs.getLocation()), "files.zip");

		testingClient.restore(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)), 200);

		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());

		zipOnSlowStorage.delete(); // to check, if the dataset really is going to be written

		Long dfid = testingClient.put(sessionId, new FileInputStream(setup.getNewFileLocation()),
				"uploaded_file2_" + timestamp, setup.getDatasetIds().get(0), setup
						.getSupportedDatafileFormat().getId(), "A rather splendid datafile", 201);

		Datafile df = (Datafile) icat.get(sessionId, "Datafile", dfid);
		assertEquals("A rather splendid datafile", df.getDescription());
		assertNull(df.getDoi());
		assertNull(df.getDatafileCreateTime());
		assertNull(df.getDatafileModTime());

		dfid = testingClient.put(sessionId, new FileInputStream(setup.getNewFileLocation()), "uploaded_file3_"
				+ timestamp, setup.getDatasetIds().get(0), setup.getSupportedDatafileFormat()
				.getId(), "An even better datafile", "7.1.3", new Date(420000), new Date(42000),
				201);
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
		} while (!fileOnFastStorage.exists() || !zipOnSlowStorage.exists());

		testingClient.archive(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)), 200);

		while (dirOnFastStorage.listFiles().length > 0 || zipOnFastStorage.exists()) {
			Thread.sleep(1000);
		}

	}
}
