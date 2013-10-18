package org.icatproject.ids.integration;

import static org.junit.Assert.assertEquals;

import java.net.HttpURLConnection;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;

import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.TestingClient;
import org.icatproject.ids.integration.util.TestingClient.Method;
import org.icatproject.ids.integration.util.TestingClient.ParmPos;
import org.icatproject.ids.integration.util.TreeDeleteVisitor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class GetStatusExplicitTest {

	private static Setup setup = null;
	@SuppressWarnings("unused")
	private static ICAT icat;
	private TestingClient testingClient;
	private Map<String, String> parameters;

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
	public void badPreparedId() throws Exception {
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("preparedId", "99999999");
		HttpURLConnection response = testingClient.process("getStatus", parameters, Method.GET,
				ParmPos.URL, null);
		assertEquals(400, response.getResponseCode());
	}

	@Test
	public void notFoundPreparedId() throws Exception {
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("preparedId", "88888888-4444-4444-4444-cccccccccccc");
		HttpURLConnection response = testingClient.process("getStatus", parameters, Method.GET,
				ParmPos.URL, null);
		assertEquals(404, response.getResponseCode());
	}

	@Test
	public void notFoundDatafileIdsTest() throws Exception {
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("datafileIds", "1,2,3,9999999");
		HttpURLConnection response = testingClient.process("getStatus", parameters, Method.GET,
				ParmPos.URL, null);
		assertEquals(404, response.getResponseCode());
	}

	@Test
	public void forbiddenTest() throws Exception {
		parameters.put("sessionId", setup.getForbiddenSessionId());
		parameters.put("datafileIds", setup.getCommaSepDatafileIds());
		HttpURLConnection response = testingClient.process("getStatus", parameters, Method.GET,
				ParmPos.URL, null);

		assertEquals(403, response.getResponseCode());
	}

	@Test
	public void correctBehaviourTest() throws Exception {
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("datafileIds", setup.getCommaSepDatafileIds());
		HttpURLConnection response = testingClient.process("prepareData", parameters, Method.POST,
				ParmPos.BODY, null);
		String preparedId = TestingClient.getResult(response.getInputStream()).toString().trim();
		assertEquals(200, response.getResponseCode());
		
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("preparedId", preparedId);
		String status;
		do {
			Thread.sleep(1000);
			response = testingClient
					.process("getStatus", parameters, Method.GET, ParmPos.URL, null);
			 status =  TestingClient.getResult(response.getInputStream()).toString().trim();
			 System.out.println("*" + status + "*");
		} while (! status.equals("ONLINE"));
		
		// status = testingClient.getStatus(setup.getGoodSessionId(), null, null,
		// setup.getCommaSepDatafileIds());
		// assertEquals(Status.ONLINE, status);
	}

	@Ignore
	@Test
	public void restoringDatafileRestoresItsDatasetTest() throws Exception {
		// String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null, setup
		// .getDatafileIds().get(0), null, null);
		// Status status = null;
		// do {
		// Thread.sleep(1000);
		// status = testingClient.getStatus(preparedId);
		// } while (Status.RESTORING.equals(status));
		// assertEquals(Status.ONLINE, status);
		// status = testingClient.getStatus(setup.getGoodSessionId(), null,
		// setup.getDatasetIds().get(0), null);
		// assertEquals(Status.ONLINE, status);
		// status = testingClient.getStatus(setup.getGoodSessionId(), null,
		// setup.getDatasetIds().get(1), null);
		// assertEquals(Status.ARCHIVED, status);
	}

}
