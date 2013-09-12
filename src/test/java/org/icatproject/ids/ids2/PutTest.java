package org.icatproject.ids.ids2;

import java.io.File;
import java.net.URL;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.util.Setup;
import org.icatproject.ids.util.TestingClient;
import org.icatproject.idsclient.exception.TestingClientNotFoundException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class PutTest {

	private static Setup setup = null;
	private static ICAT icat;
	TestingClient testingClient;

	private static long timestamp;

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
		timestamp = System.currentTimeMillis();
	}
	
	@Test(expected = TestingClientNotFoundException.class)
	public void putToUnrestoredDataset() throws Exception {
		final int DS_NUM_FROM_PROPS = 0;
		File fileOnUsersDisk = new File(setup.getNewFileLocation()); // this file will be uploaded
		testingClient.putTest(setup.getGoodSessionId(), "uploaded_file1_" + timestamp, "xml", setup.getDatasetIds()
				.get(DS_NUM_FROM_PROPS), null, null, null, null, fileOnUsersDisk);
	}

	@Test
	public void putOneFileTest() throws Exception {
		final int DS_NUM_FROM_PROPS = 0;		
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(DS_NUM_FROM_PROPS)));
		File fileOnUsersDisk = new File(setup.getNewFileLocation()); // this file will be uploaded
		String uploadedLocation = new File(icatDs.getLocation(), "uploaded_file2_"+timestamp).getPath();
		File fileOnFastStorage = new File(setup.getStorageDir(), uploadedLocation);
		
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
		
		testingClient.putTest(setup.getGoodSessionId(), "uploaded_file2_"+timestamp, "xml", setup.getDatasetIds()
				.get(DS_NUM_FROM_PROPS), null, null, null, null, fileOnUsersDisk);
		do {
			Thread.sleep(1000);
		} while (!fileOnFastStorage.exists());
		assertTrue("File " + fileOnFastStorage.getAbsolutePath() + " should have been created, but doesn't exist",
				fileOnFastStorage.exists());
		
		testingClient.archiveTest(setup.getGoodSessionId(), null, setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null);
		while (dirOnFastStorage.listFiles().length > 0 || zipOnFastStorage.exists()) {
			Thread.sleep(1000);
		}
		assertTrue("Directory " + dirOnFastStorage.getAbsolutePath() + " should have been cleaned, but still contains files",
				dirOnFastStorage.listFiles().length == 0);
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been archived, but still exists",
				!zipOnFastStorage.exists());
	}
}
