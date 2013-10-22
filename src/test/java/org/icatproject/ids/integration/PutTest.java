package org.icatproject.ids.integration;

import java.io.File;
import java.io.FileInputStream;
import java.net.HttpURLConnection;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;

import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.TreeDeleteVisitor;
import org.icatproject.ids.integration.util.client.DataNotOnlineException;
import org.icatproject.ids.integration.util.client.TestingClient;
import org.icatproject.ids.integration.util.client.TestingClient.Method;
import org.icatproject.ids.integration.util.client.TestingClient.ParmPos;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PutTest {

	private static Setup setup = null;
	private static ICAT icat;
	TestingClient testingClient;

	private static long timestamp;
	private Map<String, String> parameters;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup();
		ICATService icatService = new ICATService(setup.getIcatUrl(), new QName(
				"http://icatproject.org", "ICATService"));
		icat = icatService.getICATPort();
	}

	@Before
	public void clearFastStorage() throws Exception {
		Path storageDir = FileSystems.getDefault().getPath(setup.getStorageDir());
		Path storageZipDir = FileSystems.getDefault().getPath(setup.getStorageZipDir());
		TreeDeleteVisitor treeDeleteVisitor = new TreeDeleteVisitor();
		Files.walkFileTree(storageDir, treeDeleteVisitor);
		Files.walkFileTree(storageZipDir, treeDeleteVisitor);
		Files.createDirectories(storageDir);
		Files.createDirectories(storageZipDir);
		testingClient = new TestingClient(setup.getIdsUrl());
		parameters = new HashMap<>();
	}

	@Test(expected = DataNotOnlineException.class)
	public void putToUnrestoredDataset() throws Exception {
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("name", "uploaded_file1_" + timestamp);
		parameters.put("datafileFormatId", setup.getSupportedDatafileFormat().getId().toString());
		parameters.put("datasetId", Long.toString(setup.getDatasetIds().get(0)));
		testingClient.process("put", parameters, Method.PUT, ParmPos.URL,
				new FileInputStream(setup.getNewFileLocation()), 404);
	}

	@Test
	public void putOneFileTest() throws Exception {

		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", setup
				.getDatasetIds().get(0));
		// this file will be uploaded
		String uploadedLocation = new File(icatDs.getLocation(), "uploaded_file2_" + timestamp)
				.getPath();
		File fileOnFastStorage = new File(setup.getStorageDir(), uploadedLocation);

		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(new File(setup.getStorageZipDir(), icatDs.getLocation()),
				"files.zip");
		File zipOnSlowStorage = new File(new File(setup.getStorageArchiveDir(),
				icatDs.getLocation()), "files.zip");

		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("datasetIds", Long.toString(setup.getDatasetIds().get(0)));
		HttpURLConnection response = testingClient.process("restore", parameters, Method.POST,
				ParmPos.BODY, null, 200);
		TestingClient.print(response);

		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());

		zipOnSlowStorage.delete(); // to check, if the dataset really is going to be written

		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("name", "uploaded_file2_" + timestamp);
		parameters.put("datafileFormatId", setup.getSupportedDatafileFormat().getId().toString());
		parameters.put("datasetId", Long.toString(setup.getDatasetIds().get(0)));
		response = testingClient.process("put", parameters, Method.PUT, ParmPos.URL,
				new FileInputStream(setup.getNewFileLocation()), 201);
		TestingClient.print(response);

		do {
			Thread.sleep(1000);
		} while (!fileOnFastStorage.exists() || !zipOnSlowStorage.exists());

		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("datasetId", Long.toString(setup.getDatasetIds().get(0)));
		response = testingClient.process("archive", parameters, Method.POST, ParmPos.BODY, null,
				200);
		TestingClient.print(response);

		while (dirOnFastStorage.listFiles().length > 0 || zipOnFastStorage.exists()) {
			Thread.sleep(1000);
		}
	}
}
