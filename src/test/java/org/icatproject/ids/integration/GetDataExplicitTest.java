package org.icatproject.ids.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.integration.util.Response;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.TestingClient;
import org.icatproject.ids.integration.util.TestingUtils;
import org.icatproject.ids.webservice.Status;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.UniformInterfaceException;

public class GetDataExplicitTest {

	private static Setup setup = null;
	private static ICAT icat;
	private TestingClient testingClient;

	// value of the offset in bytes
	final Integer goodOffset = 20;
	final Integer badOffset = 99999999;

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
	public void badPreparedIdFormatTest() throws Exception {
		int expectedSc = 400;
		try {
			testingClient.getDataTest("bad preparedId format", null, null, null, null, null, null,
					null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void badDatafileIdFormatTest() throws Exception {
		int expectedSc = 400;
		try {
			testingClient.getDataTest(setup.getGoodSessionId(), null, null, "notADatafile", null,
					null, null, null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void forbiddenTest() throws Exception {
		int expectedSc = 403;
		try {
			testingClient.getDataTest(setup.getForbiddenSessionId(), null, null,
					setup.getCommaSepDatafileIds(), null, null, null, null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void correctBehaviourTest() throws Exception {
		String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null,
				setup.getCommaSepDatafileIds(), null, null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatusTest(preparedId);
		} while (Status.RESTORING.equals(status));
		assertEquals(Status.ONLINE, status);
		Response response = testingClient.getDataTest(setup.getGoodSessionId(), null, null,
				setup.getCommaSepDatafileIds(), null, null, null, null);
		Map<String, String> map = TestingUtils.filenameMD5Map(response.getResponse());
		TestingUtils.checkMD5Values(map, setup);
	}

	@Test
	public void gettingDatafileRestoresItsDatasetTest() throws Exception {
		final int DF_NUM_FROM_PROPS = 2;
		final int DS_NUM_FROM_PROPS = 1;
		final int UNRESTORED_DS_NUM = 0;
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(DS_NUM_FROM_PROPS)));
		Dataset unrestoredIcatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(UNRESTORED_DS_NUM)));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());
		File unrestoredDirOnFastStorage = new File(setup.getStorageDir(),
				unrestoredIcatDs.getLocation());
		File unrestoredZipOnFastStorage = new File(setup.getStorageZipDir(),
				unrestoredIcatDs.getLocation());
		try {
			testingClient.getDataTest(setup.getGoodSessionId(), null, null, setup.getDatafileIds()
					.get(DF_NUM_FROM_PROPS), null, null, null, null);
		} catch (UniformInterfaceException e) {
			assertEquals(404, e.getResponse().getStatus());
			// Ignore exception. Check if the data will be restored.
		}
		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());

		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());
		assertFalse("File " + unrestoredDirOnFastStorage.getAbsolutePath()
				+ " shouldn't have been restored, but exist", unrestoredDirOnFastStorage.exists());
		assertFalse("Zip in " + unrestoredZipOnFastStorage.getAbsolutePath()
				+ " shouldn't have been restored, but exist", unrestoredZipOnFastStorage.exists());
	}

	@Test
	public void gettingDatafileAndDatasetShouldRestoreBothDatasetsTest() throws Exception {
		final int DF_NUM_FROM_PROPS = 2; // belongs do DS 1
		final int DS_NUM_FROM_PROPS = 0;
		Dataset icatDs0 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(0)));
		Dataset icatDs1 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(1)));
		File dirOnFastStorage0 = new File(setup.getStorageDir(), icatDs0.getLocation());
		File zipOnFastStorage0 = new File(setup.getStorageZipDir(), icatDs0.getLocation());
		File dirOnFastStorage1 = new File(setup.getStorageDir(), icatDs1.getLocation());
		File zipOnFastStorage1 = new File(setup.getStorageZipDir(), icatDs1.getLocation());
		try {
			testingClient.getDataTest(setup.getGoodSessionId(), null,
					setup.getDatasetIds().get(DS_NUM_FROM_PROPS),
					setup.getDatafileIds().get(DF_NUM_FROM_PROPS), null, null, null, null);
		} catch (UniformInterfaceException e) {
			assertEquals(404, e.getResponse().getStatus());
			// Ignore exception. Check if the data will be restored.
		}
		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage0.exists() || !zipOnFastStorage0.exists()
				|| !dirOnFastStorage1.exists() || !zipOnFastStorage1.exists());

		assertTrue("File " + dirOnFastStorage0.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage0.exists());
		assertTrue("Zip in " + zipOnFastStorage0.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage0.exists());
		assertTrue("File " + dirOnFastStorage1.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage1.exists());
		assertTrue("Zip in " + zipOnFastStorage1.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage1.exists());
	}

}
