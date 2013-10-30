package org.icatproject.ids.integration.two;

import static org.junit.Assert.assertEquals;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.TreeDeleteVisitor;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.icatproject.ids.integration.util.client.NotFoundException;
import org.icatproject.ids.integration.util.client.TestingClient;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.icatproject.ids.integration.util.client.TestingClient.Method;
import org.icatproject.ids.integration.util.client.TestingClient.ParmPos;
import org.icatproject.ids.integration.util.client.TestingClient.Status;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetStatusExplicitTest {

	private static Setup setup = null;

	private TestingClient testingClient;
	private Map<String, String> parameters;
	private String sessionId;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
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
		sessionId = setup.getGoodSessionId();
	}

	@Test
	public void ping() throws Exception {
		testingClient.ping(200);
	}

	@Test(expected = BadRequestException.class)
	public void badPreparedId() throws Exception {
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("preparedId", "99999999");
		testingClient.process("getStatus", parameters, Method.GET, ParmPos.URL, null, null, 400);
	}

	@Test(expected = NotFoundException.class)
	public void notFoundPreparedId() throws Exception {
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("preparedId", "88888888-4444-4444-4444-cccccccccccc");
		testingClient.process("getStatus", parameters, Method.GET, ParmPos.URL, null, null, 404);
	}

	@Test(expected = NotFoundException.class)
	public void notFoundDatafileIdsTest() throws Exception {
		testingClient.getStatus(sessionId,
				new DataSelection().addDatasets(Arrays.asList(1L, 2L, 3L, 9999999L)), 404);
	}

	@Test(expected = InsufficientPrivilegesException.class)
	public void forbiddenTest() throws Exception {
		parameters.put("sessionId", setup.getForbiddenSessionId());
		parameters.put("datafileIds",
				setup.getDatafileIds().toString().replace("[", "").replace("]", "")
						.replace(" ", ""));
		testingClient.process("getStatus", parameters, Method.GET, ParmPos.URL, null, null, 403);
	}

	@Test
	public void correctBehaviourTest() throws Exception {
		Status status;

		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(sessionId,
					new DataSelection().addDatafiles(setup.getDatafileIds()), 200);
			System.out.println("*" + status + "*");
		} while (status != Status.ONLINE);

	}

	@Test
	public void restoringDatafileRestoresItsDatasetTest() throws Exception {
		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(setup.getDatafileIds().get(0)), Flag.NONE, 200);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));
		assertEquals(Status.ONLINE, status);
		status = testingClient.getStatus(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)), 200);
		assertEquals(Status.ONLINE, status);
		status = testingClient.getStatus(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(1)), 200);
		assertEquals(Status.ARCHIVED, status);
	}

}
