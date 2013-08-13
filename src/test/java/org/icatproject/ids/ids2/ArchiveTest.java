package org.icatproject.ids.ids2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ArchiveTest {
	
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
	public void restoreThenArchiveDataset() throws Exception {
		final int DS_NUM_FROM_PROPS = 0;
		TestingClient client = new TestingClient(setup.getIdsUrl());
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(DS_NUM_FROM_PROPS)));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());

		String preparedId = client.prepareDataTest(setup.getGoodSessionId(), null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null,
				null, null);
		Status status = null;
		int retryLimit = 5;
		do {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			status = client.getStatusTest(preparedId);
		} while (Status.RESTORING.equals(status) && retryLimit-- > 0);

		assertEquals("Status info should be ONLINE, is " + status.name(), status, Status.ONLINE);
		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist",
				zipOnFastStorage.exists());
		
		client.archiveTest(setup.getGoodSessionId(), null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null);
		retryLimit = 10;
		while ((dirOnFastStorage.exists() || zipOnFastStorage.exists()) && retryLimit-- > 0) {
			Thread.sleep(1000);
		}
		assertTrue("File " + dirOnFastStorage.getAbsolutePath() + " should have been archived, but still exists",
				!dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been archived, but still exists",
				!zipOnFastStorage.exists());
	}

}
