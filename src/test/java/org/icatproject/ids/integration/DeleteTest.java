package org.icatproject.ids.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.TestingClient;
import org.icatproject.ids.integration.util.TestingUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.UniformInterfaceException;

public class DeleteTest {

	private static Setup setup = null;
	private static ICAT icat;
	TestingClient testingClient;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup();
		final URL icatUrl = new URL(setup.getIcatUrl());
		final ICATService icatService = new ICATService(icatUrl, new QName(
				"http://icatproject.org", "ICATService"));
		icat = icatService.getICATPort();
	}

	@Before
	public void clearFastStorage() throws Exception {
		File storageDir = new File(setup.getStorageDir());
		File storageZipDir = new File(setup.getStorageZipDir());
		FileUtils.deleteDirectory(storageDir);
		FileUtils.deleteDirectory(storageZipDir);
		storageDir.mkdir();
		storageZipDir.mkdir();
		testingClient = new TestingClient(setup.getIdsUrl());
	}

	@Test
	public void deleteFromUnrestoredDataset() throws Exception {
		int expectedSc = 404;
		try {
			final int DS_NUM_FROM_PROPS = 0;
			testingClient.delete(setup.getGoodSessionId(), null,
					setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void deleteDatafileFromRestoredDatasetTest() throws Exception {
		final int DF_NUM_FROM_PROPS = 3;
		final int DS_NUM_FROM_PROPS = 1;
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(DS_NUM_FROM_PROPS)));

		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(new File(setup.getStorageZipDir(), icatDs.getLocation()),
				"files.zip");
		File zipOnSlowStorage = new File(new File(setup.getStorageArchiveDir(),
				icatDs.getLocation()), "files.zip");

		testingClient.restore(setup.getGoodSessionId(), null,
				setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null);
		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());
		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());

		zipOnSlowStorage.delete(); // to check, if the dataset really is going to be written
		testingClient.delete(setup.getGoodSessionId(), null, null,
				setup.getDatafileIds().get(DF_NUM_FROM_PROPS));
		do {
			Thread.sleep(1000);
		} while (!zipOnSlowStorage.exists());
		assertTrue("File " + zipOnSlowStorage.getAbsolutePath()
				+ " should have been created, but doesn't exist", zipOnSlowStorage.exists());
		assertEquals(1, TestingUtils.countZipEntries(zipOnSlowStorage));
	}

	@Test
	public void deleteRestoredDatasetTest() throws Exception {
		final int DS_NUM_FROM_PROPS = 1;
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(DS_NUM_FROM_PROPS)));

		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(new File(setup.getStorageZipDir(), icatDs.getLocation()),
				"files.zip");
		File zipOnSlowStorage = new File(new File(setup.getStorageArchiveDir(),
				icatDs.getLocation()), "files.zip");

		testingClient.restore(setup.getGoodSessionId(), null,
				setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null);
		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());
		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());

		testingClient.delete(setup.getGoodSessionId(), null,
				setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null);
		do {
			Thread.sleep(1000);
		} while (zipOnSlowStorage.exists());
		assertTrue("File " + zipOnFastStorage.getAbsolutePath()
				+ " should have been deleted, but still exists", !zipOnFastStorage.exists());
		assertTrue("File " + zipOnSlowStorage.getAbsolutePath()
				+ " should have been deleted, but still exists", !zipOnSlowStorage.exists());
	}

}
