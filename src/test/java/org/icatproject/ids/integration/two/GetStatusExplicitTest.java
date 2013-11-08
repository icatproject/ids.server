package org.icatproject.ids.integration.two;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.icatproject.ids.integration.util.client.NotFoundException;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.icatproject.ids.integration.util.client.TestingClient.Method;
import org.icatproject.ids.integration.util.client.TestingClient.ParmPos;
import org.icatproject.ids.integration.util.client.TestingClient.Status;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetStatusExplicitTest extends BaseTest {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
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
		testingClient.isPrepared("88888888-4444-4444-4444-cccccccccccc", 404);
	}

	@Test(expected = NotFoundException.class)
	public void notFoundDatafileIdsTest() throws Exception {
		testingClient.getStatus(sessionId,
				new DataSelection().addDatasets(Arrays.asList(1L, 2L, 3L, 9999999L)), 404);
	}

	@Test(expected = InsufficientPrivilegesException.class)
	public void forbiddenTest() throws Exception {
		parameters.put("sessionId", setup.getForbiddenSessionId());
		parameters.put("datafileIds", datafileIds.toString().replace("[", "").replace("]", "")
				.replace(" ", ""));
		testingClient.process("getStatus", parameters, Method.GET, ParmPos.URL, null, null, 403);
	}

	@Test
	public void correctBehaviourTest() throws Exception {
		Status status = testingClient.getStatus(sessionId,
				new DataSelection().addDatafiles(datafileIds), 200);
		assertEquals(status, Status.ARCHIVED);
		waitForIds();
		assertEquals(status, Status.ARCHIVED);
	}

	@Test
	public void restoringDatafileRestoresItsDatasetTest() throws Exception {
		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		Status status = testingClient.getStatus(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)), 200);
		assertEquals(Status.ONLINE, status);
		status = testingClient.getStatus(sessionId,
				new DataSelection().addDataset(datasetIds.get(1)), 200);
		assertEquals(Status.ARCHIVED, status);
	}

}
