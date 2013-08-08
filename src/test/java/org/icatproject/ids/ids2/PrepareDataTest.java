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
	private static ICAT icatClient;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup();
		final URL icatUrl = new URL(setup.getIcatUrl());
		final ICATService icatService = new ICATService(icatUrl, new QName("http://icatproject.org", "ICATService"));
		icatClient = icatService.getICATPort();
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
		Dataset icatDs = (Dataset) icatClient.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(0)));
		File fileOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());

		String preparedId = client.prepareDataTest(setup.getGoodSessionId(), null, setup.getDatasetIds().get(0), null,
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
		assertTrue("File " + fileOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				fileOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage.exists());
	}

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
