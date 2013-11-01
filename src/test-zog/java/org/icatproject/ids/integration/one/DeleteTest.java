package org.icatproject.ids.integration.one;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.TestingClient;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeleteTest extends BaseTest {


	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("one.properties");
		icatsetup();
	}

	@Test
	public void deleteDatafileTest() throws Exception {
		populateStorage();
		Dataset icatDs = (Dataset) icat.get(sessionId, "Dataset", datasetIds.get(1));

		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(new File(setup.getStorageZipDir(), icatDs.getLocation()),
				"files.zip");
		File zipOnSlowStorage = new File(new File(setup.getStorageArchiveDir(),
				icatDs.getLocation()), "files.zip");

		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(1)), 200);
		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());
		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());

		zipOnSlowStorage.delete(); // to check, if the dataset really is going to be written
		testingClient.delete(sessionId, new DataSelection().addDatafile(datafileIds.get(3)), 200);
		do {
			Thread.sleep(1000);
		} while (!zipOnSlowStorage.exists());
		assertTrue("File " + zipOnSlowStorage.getAbsolutePath()
				+ " should have been created, but doesn't exist", zipOnSlowStorage.exists());
		// assertEquals(1, TestingUtils.countZipEntries(zipOnSlowStorage));
	}

	@Test
	public void deleteDatasetTest() throws Exception {
		populateStorage();
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", datasetIds.get(1));

		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());

		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());

		testingClient.delete(sessionId, new DataSelection().addDataset(datasetIds.get(1)), 200);

		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been deleted, but still exists", !dirOnFastStorage.exists());

	}

}
