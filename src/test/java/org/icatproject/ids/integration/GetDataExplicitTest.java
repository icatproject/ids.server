package org.icatproject.ids.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
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
import org.icatproject.ids.integration.util.TestingUtils;
import org.icatproject.ids.integration.util.TreeDeleteVisitor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetDataExplicitTest {

	private static Setup setup = null;
	private static ICAT icat;
	private TestingClient testingClient;
	private Map<String, String> parameters;

	// value of the offset in bytes
	final Integer goodOffset = 20;
	final Integer badOffset = 99999999;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup();
		final ICATService icatService = new ICATService(setup.getIcatUrl(), new QName(
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
	public void badPreparedIdFormatTest() throws Exception {
		parameters.put("preparedId", "bad preparedId format");
		HttpURLConnection response = testingClient.process("getData", parameters, Method.GET,
				ParmPos.URL, null);
		assertEquals(400, response.getResponseCode());
	}

	@Test
	public void badDatafileIdFormatTest() throws Exception {
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("datafileIds", "notADatafile");
		HttpURLConnection response = testingClient.process("getData", parameters, Method.GET,
				ParmPos.URL, null);
		assertEquals(400, response.getResponseCode());
	}

	@Test
	public void forbiddenTest() throws Exception {
		parameters.put("sessionId", setup.getForbiddenSessionId());
		parameters.put("datafileIds", setup.getCommaSepDatafileIds());
		HttpURLConnection response = testingClient.process("getData", parameters, Method.GET,
				ParmPos.URL, null);
		assertEquals(403, response.getResponseCode());
	}

	@Test
	public void correctBehaviourTest() throws Exception {
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("datafileIds", setup.getCommaSepDatafileIds());
		HttpURLConnection response = testingClient.process("getData", parameters, Method.GET,
				ParmPos.URL, null);
		assertEquals(202, response.getResponseCode());

		do {
			Thread.sleep(1000);
			response = testingClient.process("getData", parameters, Method.GET, ParmPos.URL, null);
		} while (response.getResponseCode() == 202);

		Map<String, String> map = TestingUtils.filenameMD5Map(TestingClient.getResult(response
				.getInputStream()));
		TestingUtils.checkMD5Values(map, setup);
	}

	@Test
	public void gettingDatafileRestoresItsDatasetTest() throws Exception {

		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(1)));
		Dataset unrestoredIcatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(0)));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());
		File unrestoredDirOnFastStorage = new File(setup.getStorageDir(),
				unrestoredIcatDs.getLocation());
		File unrestoredZipOnFastStorage = new File(setup.getStorageZipDir(),
				unrestoredIcatDs.getLocation());

		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("datafileIds", setup.getDatafileIds().get(2));
		HttpURLConnection response = testingClient.process("getData", parameters, Method.GET,
				ParmPos.URL, null);
		TestingClient.print(response);
		assertEquals(202, response.getResponseCode());

		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());

		assertFalse("File " + unrestoredDirOnFastStorage.getAbsolutePath()
				+ " shouldn't have been restored, but exist", unrestoredDirOnFastStorage.exists());
		assertFalse("Zip in " + unrestoredZipOnFastStorage.getAbsolutePath()
				+ " shouldn't have been restored, but exist", unrestoredZipOnFastStorage.exists());

		response = testingClient.process("getData", parameters, Method.GET, ParmPos.URL, null);

		assertEquals(200, response.getResponseCode());

	}

	@Test
	public void gettingDatafileAndDatasetShouldRestoreBothDatasetsTest() throws Exception {
		Dataset icatDs0 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(0)));
		Dataset icatDs1 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
				Long.parseLong(setup.getDatasetIds().get(1)));
		File dirOnFastStorage0 = new File(setup.getStorageDir(), icatDs0.getLocation());
		File zipOnFastStorage0 = new File(setup.getStorageZipDir(), icatDs0.getLocation());
		File dirOnFastStorage1 = new File(setup.getStorageDir(), icatDs1.getLocation());
		File zipOnFastStorage1 = new File(setup.getStorageZipDir(), icatDs1.getLocation());

		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("datasetIds", setup.getDatasetIds().get(0));
		parameters.put("datafileIds", setup.getDatafileIds().get(2));
		HttpURLConnection response = testingClient.process("getData", parameters, Method.GET,
				ParmPos.URL, null);
		TestingClient.print(response);
		assertEquals(202, response.getResponseCode());

		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage0.exists() || !zipOnFastStorage0.exists()
				|| !dirOnFastStorage1.exists() || !zipOnFastStorage1.exists());

		response = testingClient.process("getData", parameters, Method.GET, ParmPos.URL, null);

		assertEquals(200, response.getResponseCode());

	}

}
