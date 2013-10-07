package org.icatproject.ids.integration;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.TestingClient;
import org.icatproject.ids.webservice.Status;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.UniformInterfaceException;

public class PrepareDataTest {

	private static Setup setup = null;
	private static ICAT icat;
	TestingClient testingClient;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup();
		final URL icatUrl = new URL(setup.getIcatUrl());
		final ICATService icatService = new ICATService(icatUrl, new QName("http://icatproject.org", "ICATService"));
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
	public void prepareArchivedDataset() throws Exception {
		final int DS_NUM_FROM_PROPS = 0;
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(DS_NUM_FROM_PROPS)));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());

		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null,
				setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null, null, null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId);
		} while (Status.RESTORING.equals(status));

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage.exists());
	}

	@Test
	public void prepareTwoArchivedDatasets() throws Exception {
		final int DS1_NUM_FROM_PROPS = 0;
		final int DS2_NUM_FROM_PROPS = 1;
		Dataset icatDs1 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(DS1_NUM_FROM_PROPS)));
		File dirOnFastStorage1 = new File(setup.getStorageDir(), icatDs1.getLocation());
		File zipOnFastStorage1 = new File(setup.getStorageZipDir(), icatDs1.getLocation());
		Dataset icatDs2 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(DS2_NUM_FROM_PROPS)));
		File dirOnFastStorage2 = new File(setup.getStorageDir(), icatDs2.getLocation());
		File zipOnFastStorage2 = new File(setup.getStorageZipDir(), icatDs2.getLocation());
		String dsIds = setup.getDatasetIds().get(DS1_NUM_FROM_PROPS) + ", "
				+ setup.getDatasetIds().get(DS2_NUM_FROM_PROPS);

		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, dsIds, null, null, null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId);
		} while (Status.RESTORING.equals(status));

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		assertTrue("File " + dirOnFastStorage1.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage1.exists());
		assertTrue("Zip in " + zipOnFastStorage1.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage1.exists());
		assertTrue("File " + dirOnFastStorage2.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage2.exists());
		assertTrue("Zip in " + zipOnFastStorage2.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage2.exists());
	}

	@Test
	public void prepareArchivedDatafile() throws Exception {
		final int DF_NUM_FROM_PROPS = 0;
		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
				Long.parseLong(setup.getDatafileIds().get(DF_NUM_FROM_PROPS)));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset().getLocation());

		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null, setup.getDatafileIds()
				.get(DF_NUM_FROM_PROPS), null, null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId);
		} while (Status.RESTORING.equals(status));

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage.exists());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void prepareArchivedDatafileAndItsDataset() throws Exception {
		final int DF_NUM_FROM_PROPS = 0;
		final int DS_NUM_FROM_PROPS = 0;
		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
				Long.parseLong(setup.getDatafileIds().get(DF_NUM_FROM_PROPS)));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset().getLocation());

		String preparedId = testingClient
				.prepareData(setup.getGoodSessionId(), null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), setup
						.getDatafileIds().get(DF_NUM_FROM_PROPS), null, null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId);
		} while (Status.RESTORING.equals(status));

		assertThat("Status info should be ONLINE or INCOMPLETE, is " + status.name(), status,
				anyOf(equalTo(Status.ONLINE), equalTo(Status.INCOMPLETE)));
		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage.exists());
	}

	@Test
	public void restoreNonExistentDataset() throws Exception {
		int expectedSc = 400;
		try {
			TestingClient client = new TestingClient(setup.getIdsUrl());
			// dataset id -1 shouldn't exist in the DB
			client.prepareData(setup.getGoodSessionId(), null, "-1", null, null, null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void badSessionIdFormatTest() throws Exception {
		int expectedSc = 400;
		try {
			TestingClient client = new TestingClient(setup.getIdsUrl());
			client.prepareData("bad sessionId format", null, null, null, null, null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void badDatafileIdListTest() throws Exception {
		int expectedSc = 400;
		try {
			TestingClient client = new TestingClient(setup.getIdsUrl());
			client.prepareData(setup.getGoodSessionId(), null, null, "1, 2, a", null, null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void badDatasetIdListTest() throws Exception {
		int expectedSc = 400;
		try {
			TestingClient client = new TestingClient(setup.getIdsUrl());
			client.prepareData(setup.getGoodSessionId(), null, "", null, null, null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void tooBigIdTest() throws Exception {
		int expectedSc = 400;
		try {
			TestingClient client = new TestingClient(setup.getIdsUrl());
			client.prepareData(setup.getGoodSessionId(), null, "99999999999999999999", null, null, null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void noIdsTest() throws Exception {
		int expectedSc = 400;
		try {
			TestingClient client = new TestingClient(setup.getIdsUrl());
			client.prepareData(setup.getGoodSessionId(), null, null, null, null, null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void badCompressTest() throws Exception {
		int expectedSc = 400;
		try {
			TestingClient client = new TestingClient(setup.getIdsUrl());
			client.prepareData(setup.getGoodSessionId(), null, null, null, "flase", null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void nonExistingSessionIdTest() throws Exception {
		int expectedSc = 403;
		try {
			TestingClient client = new TestingClient(setup.getIdsUrl());
			client.prepareData("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", null, null, setup.getCommaSepDatafileIds(),
					null, null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void correctBehaviourTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		String preparedId = client.prepareData(setup.getGoodSessionId(), null, null,
				setup.getCommaSepDatafileIds(), null, null);
		assertNotNull(preparedId);
	}

	@Test
	public void prepareRestoredDataset() throws Exception {
		final int DS_NUM_FROM_PROPS = 0;
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(DS_NUM_FROM_PROPS)));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());

		testingClient.restore(setup.getGoodSessionId(), null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null);
		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());

		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage.exists());

		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null,
				setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null, null, null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId);
		} while (status.equals(Status.RESTORING));

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		File preparedFile = new File(setup.getStoragePreparedDir(), preparedId + ".zip");
		assertTrue("File " + preparedFile.getAbsolutePath() + " should have been prepared, but doesn't exist",
				preparedFile.exists());
	}

	@Test
	public void prepareTwoRestoredDatasets() throws Exception {
		final int DS1_NUM_FROM_PROPS = 0;
		final int DS2_NUM_FROM_PROPS = 1;
		Dataset icatDs1 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(DS1_NUM_FROM_PROPS)));
		File dirOnFastStorage1 = new File(setup.getStorageDir(), icatDs1.getLocation());
		File zipOnFastStorage1 = new File(setup.getStorageZipDir(), icatDs1.getLocation());
		Dataset icatDs2 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(DS2_NUM_FROM_PROPS)));
		File dirOnFastStorage2 = new File(setup.getStorageDir(), icatDs2.getLocation());
		File zipOnFastStorage2 = new File(setup.getStorageZipDir(), icatDs2.getLocation());
		String dsIds = setup.getDatasetIds().get(DS1_NUM_FROM_PROPS) + ", "
				+ setup.getDatasetIds().get(DS2_NUM_FROM_PROPS);

		testingClient.restore(setup.getGoodSessionId(), null, dsIds, null);
		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage1.exists() || !zipOnFastStorage1.exists() || !dirOnFastStorage2.exists()
				|| !zipOnFastStorage2.exists());

		assertTrue("File " + dirOnFastStorage1.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage1.exists());
		assertTrue("Zip in " + zipOnFastStorage1.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage1.exists());
		assertTrue("File " + dirOnFastStorage2.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage2.exists());
		assertTrue("Zip in " + zipOnFastStorage2.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage2.exists());

		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, dsIds, null, null, null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId);
		} while (status.equals(Status.RESTORING));

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		File preparedFile = new File(setup.getStoragePreparedDir(), preparedId + ".zip");
		assertTrue("File " + preparedFile.getAbsolutePath() + " should have been prepared, but doesn't exist",
				preparedFile.exists());
	}

	@Test
	public void prepareRestoredDatafile() throws Exception {
		final int DF_NUM_FROM_PROPS = 0;
		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
				Long.parseLong(setup.getDatafileIds().get(DF_NUM_FROM_PROPS)));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset().getLocation());

		testingClient.restore(setup.getGoodSessionId(), null, null, setup.getDatafileIds().get(DF_NUM_FROM_PROPS));
		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());

		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage.exists());

		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null, setup.getDatafileIds()
				.get(DF_NUM_FROM_PROPS), null, null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId);
		} while (status.equals(Status.RESTORING));

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		File preparedFile = new File(setup.getStoragePreparedDir(), preparedId + ".zip");
		assertTrue("File " + preparedFile.getAbsolutePath() + " should have been prepared, but doesn't exist",
				preparedFile.exists());
	}

	@Test
	public void prepareRestoredDatafileAndItsDataset() throws Exception {
		final int DF_NUM_FROM_PROPS = 0;
		final int DS_NUM_FROM_PROPS = 0;
		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
				Long.parseLong(setup.getDatafileIds().get(DF_NUM_FROM_PROPS)));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset().getLocation());
		System.out.println("a");
		testingClient.restore(setup.getGoodSessionId(), null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), setup
				.getDatafileIds().get(DF_NUM_FROM_PROPS));
		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());
		System.out.println("b");
		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage.exists());
		System.out.println("c");
		String preparedId = testingClient
				.prepareData(setup.getGoodSessionId(), null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), setup
						.getDatafileIds().get(DF_NUM_FROM_PROPS), null, null);
		System.out.println("cc");
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId);
		} while (Status.RESTORING.equals(status));
		System.out.println("d");
		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		File preparedFile = new File(setup.getStoragePreparedDir(), preparedId + ".zip");
		assertTrue("File " + preparedFile.getAbsolutePath() + " should have been prepared, but doesn't exist",
				preparedFile.exists());
	}

}
