package org.icatproject.ids.ids2;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.util.Setup;
import org.icatproject.ids.util.TestingClient;
import org.icatproject.idsclient.exception.TestingClientBadRequestException;
import org.icatproject.idsclient.exception.TestingClientForbiddenException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ArchiveTest {

	private static Setup setup = null;
	private static ICAT icat;
	private TestingClient testingClient;

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
	public void restoreThenArchiveDataset() throws Exception {
		final int DS_NUM_FROM_PROPS = 0;
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(DS_NUM_FROM_PROPS)));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(new File(setup.getStorageZipDir(), icatDs.getLocation()), "files.zip");

		testingClient.restoreTest(setup.getGoodSessionId(), null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null);
		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());

		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage.exists());

		testingClient.archiveTest(setup.getGoodSessionId(), null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null);
		while (dirOnFastStorage.listFiles().length > 0 || zipOnFastStorage.exists()) {
			Thread.sleep(1000);
		}
		assertTrue("Directory " + dirOnFastStorage.getAbsolutePath() + " should have been cleaned, but still contains files",
				dirOnFastStorage.listFiles().length == 0);
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been archived, but still exists",
				!zipOnFastStorage.exists());
	}

	@Test(expected = TestingClientBadRequestException.class)
	public void badSessionIdFormatTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		client.archiveTest("bad sessionId format", null, null, "1,2");
	}

	@Test(expected = TestingClientBadRequestException.class)
	public void badDatafileIdFormatTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		client.archiveTest(setup.getGoodSessionId(), null, null, "1,2,a");
	}

	@Test(expected = TestingClientBadRequestException.class)
	public void badDatasetIdFormatTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		client.archiveTest(setup.getGoodSessionId(), null, "", null);
	}

	@Test(expected = TestingClientBadRequestException.class)
	public void noIdsTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		client.archiveTest(setup.getGoodSessionId(), null, null, null);
	}

	@Test(expected = TestingClientForbiddenException.class)
	public void nonExistingSessionIdTest() throws Exception {
		TestingClient client = new TestingClient(setup.getIdsUrl());
		client.archiveTest("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", null, "1,2", null);
	}

}
