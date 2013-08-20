package org.icatproject.ids.ids2;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.net.URL;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.Setup;
import org.icatproject.ids.webservice.Status;
import org.icatproject.idsclient.TestingClient;
import org.icatproject.idsclient.exception.TestingClientBadRequestException;
import org.icatproject.idsclient.exception.TestingClientForbiddenException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

//	@Test
//	public void prepareArchivedDataset() throws Exception {
//		final int DS_NUM_FROM_PROPS = 0;
//		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
//				Long.parseLong(setup.getDatasetIds().get(DS_NUM_FROM_PROPS)));
//		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
//		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());
//
//		String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null,
//				setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null, null, null);
//		Status status = null;
//		int retryLimit = 5;
//		do {
//			Thread.sleep(1000);
//			status = testingClient.getStatusTest(preparedId);
//		} while (Status.RESTORING.equals(status) && retryLimit-- > 0);
//
//		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
//		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
//				dirOnFastStorage.exists());
//		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
//				zipOnFastStorage.exists());
//	}
//
//	@Test
//	public void prepareTwoArchivedDatasets() throws Exception {
//		final int DS1_NUM_FROM_PROPS = 0;
//		final int DS2_NUM_FROM_PROPS = 1;
//		Dataset icatDs1 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
//				Long.parseLong(setup.getDatasetIds().get(DS1_NUM_FROM_PROPS)));
//		File dirOnFastStorage1 = new File(setup.getStorageDir(), icatDs1.getLocation());
//		File zipOnFastStorage1 = new File(setup.getStorageZipDir(), icatDs1.getLocation());
//		Dataset icatDs2 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
//				Long.parseLong(setup.getDatasetIds().get(DS2_NUM_FROM_PROPS)));
//		File dirOnFastStorage2 = new File(setup.getStorageDir(), icatDs2.getLocation());
//		File zipOnFastStorage2 = new File(setup.getStorageZipDir(), icatDs2.getLocation());
//		String dsIds = setup.getDatasetIds().get(DS1_NUM_FROM_PROPS) + ", "
//				+ setup.getDatasetIds().get(DS2_NUM_FROM_PROPS);
//
//		String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, dsIds, null, null, null);
//		Status status = null;
//		int retryLimit = 5;
//		do {
//			Thread.sleep(1000);
//			status = testingClient.getStatusTest(preparedId);
//		} while (Status.RESTORING.equals(status) && retryLimit-- > 0);
//
//		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
//		assertTrue("File " + dirOnFastStorage1.getAbsolutePath() + " should have been restored, but doesn't exist",
//				dirOnFastStorage1.exists());
//		assertTrue("Zip in " + zipOnFastStorage1.getAbsolutePath() + " should have been restored, but doesn't exist",
//				zipOnFastStorage1.exists());
//		assertTrue("File " + dirOnFastStorage2.getAbsolutePath() + " should have been restored, but doesn't exist",
//				dirOnFastStorage2.exists());
//		assertTrue("Zip in " + zipOnFastStorage2.getAbsolutePath() + " should have been restored, but doesn't exist",
//				zipOnFastStorage2.exists());
//	}
//
//	@Test
//	public void prepareArchivedDatafile() throws Exception {
//		final int DF_NUM_FROM_PROPS = 0;
//		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
//				Long.parseLong(setup.getDatafileIds().get(DF_NUM_FROM_PROPS)));
//		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
//		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset().getLocation());
//
//		String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null,
//				setup.getDatafileIds().get(DF_NUM_FROM_PROPS), null, null);
//		Status status = null;
//		int retryLimit = 5;
//		do {
//			Thread.sleep(1000);
//			status = testingClient.getStatusTest(preparedId);
//		} while (Status.RESTORING.equals(status) && retryLimit-- > 0);
//
//		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
//		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
//				dirOnFastStorage.exists());
//		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
//				zipOnFastStorage.exists());
//	}
//
//	@Test
//	public void prepareArchivedDatafileAndItsDataset() throws Exception {
//		final int DF_NUM_FROM_PROPS = 0;
//		final int DS_NUM_FROM_PROPS = 0;
//		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
//				Long.parseLong(setup.getDatafileIds().get(DF_NUM_FROM_PROPS)));
//		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
//		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset().getLocation());
//
//		String preparedId = testingClient
//				.prepareDataTest(setup.getGoodSessionId(), null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), setup
//						.getDatafileIds().get(DF_NUM_FROM_PROPS), null, null);
//		Status status = null;
//		int retryLimit = 5;
//		do {
//			Thread.sleep(1000);
//			status = testingClient.getStatusTest(preparedId);
//		} while (Status.RESTORING.equals(status) && retryLimit-- > 0);
//
//		assertThat("Status info should be ONLINE or INCOMPLETE, is " + status.name(), status, anyOf(equalTo(Status.ONLINE), equalTo(Status.INCOMPLETE)));
////		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
//		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
//				dirOnFastStorage.exists());
//		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
//				zipOnFastStorage.exists());
//	}

	@Test(expected = TestingClientBadRequestException.class)
	public void restoreNonExistentDataset() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		// dataset id -1 shouldn't exist in the DB
		client.prepareDataTest(setup.getGoodSessionId(), null, "-1", null, null, null);
	}

	@Test(expected = TestingClientBadRequestException.class)
	public void badSessionIdFormatTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		client.prepareDataTest("bad sessionId format", null, null, null, null, null);
	}

	@Test(expected = TestingClientBadRequestException.class)
	public void badDatafileIdListTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		client.prepareDataTest(setup.getGoodSessionId(), null, null, "1, 2, a", null, null);
	}

	@Test(expected = TestingClientBadRequestException.class)
	public void badDatasetIdListTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		client.prepareDataTest(setup.getGoodSessionId(), null, "", null, null, null);
	}

	@Test(expected = TestingClientBadRequestException.class)
	public void tooBigIdTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		client.prepareDataTest(setup.getGoodSessionId(), null, "99999999999999999999", null, null, null);
	}

	@Test(expected = TestingClientBadRequestException.class)
	public void noIdsTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		client.prepareDataTest(setup.getGoodSessionId(), null, null, null, null, null);
	}

	@Test(expected = TestingClientBadRequestException.class)
	public void badCompressTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		client.prepareDataTest(setup.getGoodSessionId(), null, null, null, "flase", null);
	}

	@Test(expected = TestingClientForbiddenException.class)
	public void nonExistingSessionIdTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		client.prepareDataTest("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", null, null, setup.getCommaSepDatafileIds(),
				null, null);
	}

	@Test
	public void correctBehaviourTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		String preparedId = client.prepareDataTest(setup.getGoodSessionId(), null, null,
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
		
		testingClient.restoreTest(setup.getGoodSessionId(), null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null);
		int retryLimit = 5;
		do {
			Thread.sleep(1000);
		} while ((!dirOnFastStorage.exists() || !zipOnFastStorage.exists()) && retryLimit-- > 0);

		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage.exists());

		String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null,
				setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null, null, null);
		Status status = null;
		retryLimit = 5;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatusTest(preparedId);
		} while (status.equals(Status.RESTORING) && retryLimit-- > 0);

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
		
		testingClient.restoreTest(setup.getGoodSessionId(), null, dsIds, null);
		int retryLimit = 5;
		do {
			Thread.sleep(1000);
		} while ((!dirOnFastStorage1.exists() || !zipOnFastStorage1.exists() || !dirOnFastStorage2.exists() ||
				!zipOnFastStorage2.exists()) && retryLimit-- > 0);

		assertTrue("File " + dirOnFastStorage1.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage1.exists());
		assertTrue("Zip in " + zipOnFastStorage1.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage1.exists());
		assertTrue("File " + dirOnFastStorage2.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage2.exists());
		assertTrue("Zip in " + zipOnFastStorage2.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage2.exists());

		String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, dsIds, null, null, null);
		Status status = null;
		retryLimit = 5;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatusTest(preparedId);
		} while (status.equals(Status.RESTORING) && retryLimit-- > 0);

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
		
		testingClient.restoreTest(setup.getGoodSessionId(), null, null, setup.getDatafileIds().get(DF_NUM_FROM_PROPS));
		int retryLimit = 5;
		do {
			Thread.sleep(1000);
		} while ((!dirOnFastStorage.exists() || !zipOnFastStorage.exists()) && retryLimit-- > 0);

		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage.exists());

		String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null,
				setup.getDatafileIds().get(DF_NUM_FROM_PROPS), null, null);
		Status status = null;
		retryLimit = 5;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatusTest(preparedId);
		} while (status.equals(Status.RESTORING) && retryLimit-- > 0);

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
		
		testingClient.restoreTest(setup.getGoodSessionId(), null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), 
				setup.getDatafileIds().get(DF_NUM_FROM_PROPS));
		int retryLimit = 5;
		do {
			Thread.sleep(1000);
		} while ((!dirOnFastStorage.exists() || !zipOnFastStorage.exists()) && retryLimit-- > 0);

		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage.exists());

		String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), 
				null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), 
				setup.getDatafileIds().get(DF_NUM_FROM_PROPS), null, null);
		Status status = null;
		retryLimit = 5;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatusTest(preparedId);
		} while (Status.RESTORING.equals(status) && retryLimit-- > 0);

		assertEquals("Status info should be ONLINE, is " + status.name(), Status.ONLINE, status);
		File preparedFile = new File(setup.getStoragePreparedDir(), preparedId + ".zip");
		assertTrue("File " + preparedFile.getAbsolutePath() + " should have been prepared, but doesn't exist",
				preparedFile.exists());
	}

}
