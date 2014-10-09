package org.icatproject.ids.integration.two;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.junit.BeforeClass;
import org.junit.Test;

public class MiscTest extends BaseTest {

	final int goodOffset = 20;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
	}

	@Test
	public void apiVersionTest() throws Exception {
		assertTrue(testingClient.getApiVersion(200).startsWith("1.2."));
	}

	@Test
	public void isReadOnlyTest() throws Exception {
		assertFalse(testingClient.isReadOnly(200));
	}

	@Test
	public void isTwoLevelTest() throws Exception {
		assertTrue(testingClient.isTwoLevel(200));
	}

	@Test(expected = InsufficientPrivilegesException.class)
	public void unprivSessionIdTest() throws Exception {
		testingClient.getServiceStatus(setup.getForbiddenSessionId(), 403);
	}

	@Test
	public void correctBehaviourNoOffsetTest() throws Exception {
		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.NONE, 200);

		assertFalse(testingClient.getServiceStatus(sessionId, 200).getOpItems().isEmpty());
		assertFalse(testingClient.getServiceStatus(sessionId, 200).getPrepItems().isEmpty());

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		assertTrue(testingClient.getServiceStatus(sessionId, 200).getOpItems().isEmpty());

		waitForIds();
		assertTrue(testingClient.getServiceStatus(sessionId, 200).getOpItems().isEmpty());
		assertTrue(testingClient.getServiceStatus(sessionId, 200).getPrepItems().isEmpty());

		try (InputStream stream = testingClient.getData(preparedId, null, 0, 200)) {
			checkStream(stream, datafileIds.get(0));
		}

	}
}