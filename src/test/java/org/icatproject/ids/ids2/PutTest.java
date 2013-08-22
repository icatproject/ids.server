package org.icatproject.ids.ids2;

import java.io.File;
import java.net.URL;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
import org.icatproject.Datafile;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.Setup;
import org.icatproject.idsclient.TestingClient;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PutTest {
	
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
	public void putOneFileTest() throws Exception {
		File fileOnSlowStorage = new File(setup.getUserLocalDir(), "test_file.txt");
		testingClient.putTest(setup.getGoodSessionId(), "my_file_name.txt", fileOnSlowStorage);
		
	}

}
