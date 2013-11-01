package org.icatproject.ids.integration.two;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.xml.namespace.QName;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.TreeDeleteVisitor;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.TestingClient;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.icatproject.ids.integration.util.client.TestingClient.Status;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PrepareDataTest {

	private static Setup setup = null;
	private static ICAT icat;
	TestingClient testingClient;
	private String sessionId;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
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

	@Test
	public void prepareArchivedDataset() throws Exception {
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", setup
				.getDatasetIds().get(0));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());
	}

	@Test
	public void prepareTwoArchivedDatasets() throws Exception {

		Dataset icatDs1 = (Dataset) icat.get(sessionId, "Dataset", setup.getDatasetIds().get(0));
		File dirOnFastStorage1 = new File(setup.getStorageDir(), icatDs1.getLocation());
		File zipOnFastStorage1 = new File(setup.getStorageZipDir(), icatDs1.getLocation());
		Dataset icatDs2 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", setup
				.getDatasetIds().get(1));
		File dirOnFastStorage2 = new File(setup.getStorageDir(), icatDs2.getLocation());
		File zipOnFastStorage2 = new File(setup.getStorageZipDir(), icatDs2.getLocation());

		String preparedId = testingClient.prepareData(
				sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)).addDataset(
						setup.getDatasetIds().get(1)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		assertTrue("File " + dirOnFastStorage1.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage1.exists());
		assertTrue("Zip in " + zipOnFastStorage1.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage1.exists());
		assertTrue("File " + dirOnFastStorage2.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage2.exists());
		assertTrue("Zip in " + zipOnFastStorage2.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage2.exists());
	}

	@Test
	public void prepareArchivedDatafile() throws Exception {
		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
				setup.getDatafileIds().get(0));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset()
				.getLocation());

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(setup.getDatafileIds().get(0)), Flag.NONE, 200);

		Status status;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());
	}

	@Test
	public void prepareArchivedDatafileAndItsDataset() throws Exception {

		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
				setup.getDatafileIds().get(0));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset()
				.getLocation());

		String preparedId = testingClient.prepareData(
				sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)).addDatafile(
						setup.getDatafileIds().get(0)), Flag.NONE, 200);

		Status status;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		assertTrue(status == Status.ONLINE || status == Status.INCOMPLETE);

		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());
	}

	@Test(expected = BadRequestException.class)
	public void badSessionIdFormatTest() throws Exception {
		testingClient.prepareData("bad sessionId format",
				new DataSelection().addDataset(setup.getDatasetIds().get(0)), Flag.NONE, 400);
	}

	@Test(expected = BadRequestException.class)
	public void noIdsTest() throws Exception {
		testingClient.prepareData("bad sessionId format", new DataSelection(), Flag.NONE, 400);

	}

	@Test(expected = BadRequestException.class)
	public void nonExistingSessionIdTest() throws Exception {
		testingClient.prepareData("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", new DataSelection(),
				Flag.NONE, 400);

	}

	@Test
	public void correctBehaviourTest() throws Exception {
		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafiles(setup.getDatafileIds()), Flag.NONE, 200);
		assertNotNull(preparedId);
	}

	@Test
	public void prepareRestoredDataset() throws Exception {
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", setup
				.getDatasetIds().get(0));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());

		testingClient.restore(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)), 200);

		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());

		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (status.equals(Status.RESTORING));

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		File preparedFile = new File(setup.getStoragePreparedDir(), preparedId + ".zip");
		assertTrue("File " + preparedFile.getAbsolutePath()
				+ " should have been prepared, but doesn't exist", preparedFile.exists());
	}

	@Test
	public void prepareTwoRestoredDatasets() throws Exception {

		Dataset icatDs1 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", setup
				.getDatasetIds().get(0));
		File dirOnFastStorage1 = new File(setup.getStorageDir(), icatDs1.getLocation());
		File zipOnFastStorage1 = new File(setup.getStorageZipDir(), icatDs1.getLocation());
		Dataset icatDs2 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", setup
				.getDatasetIds().get(1));
		File dirOnFastStorage2 = new File(setup.getStorageDir(), icatDs2.getLocation());
		File zipOnFastStorage2 = new File(setup.getStorageZipDir(), icatDs2.getLocation());

		testingClient.restore(
				sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)).addDataset(
						setup.getDatasetIds().get(1)), 200);

		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage1.exists() || !zipOnFastStorage1.exists()
				|| !dirOnFastStorage2.exists() || !zipOnFastStorage2.exists());

		assertTrue("File " + dirOnFastStorage1.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage1.exists());
		assertTrue("Zip in " + zipOnFastStorage1.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage1.exists());
		assertTrue("File " + dirOnFastStorage2.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage2.exists());
		assertTrue("Zip in " + zipOnFastStorage2.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage2.exists());

		String preparedId = testingClient.prepareData(
				sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)).addDataset(
						setup.getDatasetIds().get(1)), Flag.NONE, 200);

		Status status;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (status.equals(Status.RESTORING));

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		File preparedFile = new File(setup.getStoragePreparedDir(), preparedId + ".zip");
		assertTrue("File " + preparedFile.getAbsolutePath()
				+ " should have been prepared, but doesn't exist", preparedFile.exists());
	}

	@Test
	public void prepareRestoredDatafile() throws Exception {

		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
				setup.getDatafileIds().get(0));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset()
				.getLocation());

		testingClient.restore(sessionId,
				new DataSelection().addDatafile(setup.getDatafileIds().get(0)), 200);

		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());

		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(setup.getDatafileIds().get(0)), Flag.NONE, 200);

		Status status;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (status.equals(Status.RESTORING));

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		File preparedFile = new File(setup.getStoragePreparedDir(), preparedId + ".zip");
		assertTrue("File " + preparedFile.getAbsolutePath()
				+ " should have been prepared, but doesn't exist", preparedFile.exists());
	}

	@Test
	public void prepareRestoredDatafileAndItsDataset() throws Exception {

		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
				setup.getDatafileIds().get(0));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset()
				.getLocation());

		testingClient.restore(
				sessionId,
				new DataSelection().addDatafile(setup.getDatafileIds().get(0)).addDataset(
						setup.getDatasetIds().get(0)), 200);

		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());

		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());

		String preparedId = testingClient.prepareData(
				sessionId,
				new DataSelection().addDatafile(setup.getDatafileIds().get(0)).addDataset(
						setup.getDatasetIds().get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		File preparedFile = new File(setup.getStoragePreparedDir(), preparedId + ".zip");
		assertTrue("File " + preparedFile.getAbsolutePath()
				+ " should have been prepared, but doesn't exist", preparedFile.exists());
	}

}
