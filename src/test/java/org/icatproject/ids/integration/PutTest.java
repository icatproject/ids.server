package org.icatproject.ids.integration;

import static org.junit.Assert.assertEquals;

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
import org.icatproject.ids.integration.util.TestingClient;
import org.icatproject.ids.integration.util.TestingClient.Method;
import org.icatproject.ids.integration.util.TestingClient.ParmPos;
import org.icatproject.ids.integration.util.TreeDeleteVisitor;
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

	@Test
	public void putToUnrestoredDataset() throws Exception {
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("name", "uploaded_file1_" + timestamp);
		parameters.put("datafileFormatId", setup.getSupportedDatafileFormat().getId().toString());
		parameters.put("datasetId", setup.getDatasetIds().get(0));
		HttpURLConnection response = testingClient.process("put", parameters, Method.PUT,
				ParmPos.URL, new FileInputStream(setup.getNewFileLocation()));
		TestingClient.print(response);
		assertEquals(404, response.getResponseCode());
	}

	@Test
	public void putOneFileTest() throws Exception {

		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(0)));
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
		parameters.put("datasetIds", setup.getDatasetIds().get(0));
		HttpURLConnection response = testingClient.process("restore", parameters, Method.POST,
				ParmPos.BODY, null);
		TestingClient.print(response);
		assertEquals(200, response.getResponseCode());

		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());

		zipOnSlowStorage.delete(); // to check, if the dataset really is going to be written

		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("name", "uploaded_file2_" + timestamp);
		parameters.put("datafileFormatId", setup.getSupportedDatafileFormat().getId().toString());
		parameters.put("datasetId", setup.getDatasetIds().get(0));
		response = testingClient.process("put", parameters, Method.PUT, ParmPos.URL,
				new FileInputStream(setup.getNewFileLocation()));
		TestingClient.print(response);
		assertEquals(201, response.getResponseCode());
		do {
			Thread.sleep(1000);
		} while (!fileOnFastStorage.exists() || !zipOnSlowStorage.exists());

		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("datasetId", setup.getDatasetIds().get(0));
		response = testingClient.process("archive", parameters, Method.POST, ParmPos.BODY, null);
		TestingClient.print(response);
		assertEquals(200, response.getResponseCode());

		while (dirOnFastStorage.listFiles().length > 0 || zipOnFastStorage.exists()) {
			Thread.sleep(1000);
		}
	}
}
