package org.icatproject.ids.ids2;

import static org.junit.Assert.*;

import java.io.File;
import java.net.URL;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
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
	}

	@Test
	public void restoreArchivedDataset() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		// choose the second dataset, as it's much smaller (tests run faster)
		final int DS_NUM_FROM_PROPS = 1;
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(DS_NUM_FROM_PROPS)));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());

		String preparedId = client.prepareDataTest(setup.getGoodSessionId(), null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null,
				null, null);
		Status status = null;
		do {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			status = client.getStatusTest(preparedId);
		} while (Status.RESTORING.equals(status));

		assertEquals("Status info should be ONLINE, is " + status.name(), status, Status.ONLINE);
		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage.exists());
	}
	
	@Test
	public void restoreTwoArchivedDatasets() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		Dataset icatDs1 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(0)));
		File dirOnFastStorage1 = new File(setup.getStorageDir(), icatDs1.getLocation());
		File zipOnFastStorage1 = new File(setup.getStorageZipDir(), icatDs1.getLocation());
		Dataset icatDs2 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(1)));
		File dirOnFastStorage2 = new File(setup.getStorageDir(), icatDs2.getLocation());
		File zipOnFastStorage2 = new File(setup.getStorageZipDir(), icatDs2.getLocation());
		String dsIds = setup.getDatasetIds().get(0) + ", " + setup.getDatasetIds().get(1);
		
		String preparedId = client.prepareDataTest(setup.getGoodSessionId(), null, dsIds, null,
				null, null);
		Status status = null;
		do {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			status = client.getStatusTest(preparedId);
		} while (Status.RESTORING.equals(status));

		assertEquals("Status info should be ONLINE, is " + status.name(), status, Status.ONLINE);
		assertTrue("File " + dirOnFastStorage1.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage1.exists());
		assertTrue("Zip in " + zipOnFastStorage1.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage1.exists());
		assertTrue("File " + dirOnFastStorage2.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage2.exists());
		assertTrue("Zip in " + zipOnFastStorage2.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage2.exists());
	}
	
	// TODO check if restoration of files triggers restoration of whole datasets
	// TODO check how the app behaves when a DS and a file from this DS is requested for restoration

	@Test(expected = TestingClientBadRequestException.class)
	public void restoreNonExistentDataset() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		// dataset id -1 shouldn't exist in the DB
		client.prepareDataTest(setup.getGoodSessionId(), null, "-1", null,
				null, null);
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

}
