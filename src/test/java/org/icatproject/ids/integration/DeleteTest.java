package org.icatproject.ids.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.xml.namespace.QName;

import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.TestingUtils;
import org.icatproject.ids.integration.util.TreeDeleteVisitor;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.NotFoundException;
import org.icatproject.ids.integration.util.client.TestingClient;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeleteTest {

	private static Setup setup = null;
	private static ICAT icat;
	TestingClient testingClient;

	private String sessionId;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup();
		final ICATService icatService = new ICATService(setup.getIcatUrl(), new QName(
				"http://icatproject.org", "ICATService"));
		icat = icatService.getICATPort();
	}

	@Before
	public void clearFastStorage() throws Exception {
		setup = new Setup();
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

	@Test(expected = NotFoundException.class)
	public void deleteFromUnrestoredDataset() throws Exception {
		testingClient.delete(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)), 404);
	}

	@Test
	public void deleteDatafileFromRestoredDatasetTest() throws Exception {
		Dataset icatDs = (Dataset) icat.get(sessionId, "Dataset", setup.getDatasetIds().get(1));

		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(new File(setup.getStorageZipDir(), icatDs.getLocation()),
				"files.zip");
		File zipOnSlowStorage = new File(new File(setup.getStorageArchiveDir(),
				icatDs.getLocation()), "files.zip");

		testingClient.restore(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(1)), 200);
		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());
		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());

		zipOnSlowStorage.delete(); // to check, if the dataset really is going to be written
		testingClient.delete(sessionId,
				new DataSelection().addDatafile(setup.getDatafileIds().get(3)), 200);
		do {
			Thread.sleep(1000);
		} while (!zipOnSlowStorage.exists());
		assertTrue("File " + zipOnSlowStorage.getAbsolutePath()
				+ " should have been created, but doesn't exist", zipOnSlowStorage.exists());
		assertEquals(1, TestingUtils.countZipEntries(zipOnSlowStorage));
	}

	@Test
	public void deleteRestoredDatasetTest() throws Exception {
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", setup
				.getDatasetIds().get(1));

		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(new File(setup.getStorageZipDir(), icatDs.getLocation()),
				"files.zip");
		File zipOnSlowStorage = new File(new File(setup.getStorageArchiveDir(),
				icatDs.getLocation()), "files.zip");

		testingClient.restore(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(1)), 200);
		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());
		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());

		testingClient.delete(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(1)), 200);
		do {
			Thread.sleep(1000);
		} while (zipOnSlowStorage.exists());
		assertTrue("File " + zipOnFastStorage.getAbsolutePath()
				+ " should have been deleted, but still exists", !zipOnFastStorage.exists());
		assertTrue("File " + zipOnSlowStorage.getAbsolutePath()
				+ " should have been deleted, but still exists", !zipOnSlowStorage.exists());
	}

}
