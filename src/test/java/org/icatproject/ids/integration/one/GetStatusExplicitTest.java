package org.icatproject.ids.integration.one;

import java.util.Arrays;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.icatproject.ids.integration.util.client.NotFoundException;
import org.icatproject.ids.integration.util.client.TestingClient.Method;
import org.icatproject.ids.integration.util.client.TestingClient.ParmPos;
import org.icatproject.ids.integration.util.client.TestingClient.Status;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetStatusExplicitTest extends BaseTest {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("one.properties");
		icatsetup();
	}

	@Test
	public void ping() throws Exception {
		testingClient.ping(200);
	}

	@Test(expected = BadRequestException.class)
	public void badPreparedId() throws Exception {
		testingClient.isPrepared("99999999", 400);
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

		Status status;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(sessionId,
					new DataSelection().addDatafiles(datafileIds), 200);
			System.out.println("*" + status + "*");
		} while (status != Status.ONLINE);

	}

}
