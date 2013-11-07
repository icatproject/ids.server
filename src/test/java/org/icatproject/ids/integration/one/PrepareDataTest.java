package org.icatproject.ids.integration.one;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.icatproject.ids.integration.util.client.TestingClient.Status;
import org.junit.BeforeClass;
import org.junit.Test;

public class PrepareDataTest extends BaseTest {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("one.properties");
		icatsetup();
	}

	@Test(expected = BadRequestException.class)
	public void badSessionIdFormatTest() throws Exception {
		testingClient.prepareData("bad sessionId format",
				new DataSelection().addDataset(datasetIds.get(0)), Flag.NONE, 400);
	}

	@Test(expected = BadRequestException.class)
	public void noIdsTest() throws Exception {
		testingClient.prepareData("bad sessionId format", new DataSelection(), Flag.NONE, 400);

	}

	@Test(expected = InsufficientPrivilegesException.class)
	public void nonExistingSessionIdTest() throws Exception {
		testingClient.prepareData("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
				new DataSelection().addDataset(datasetIds.get(0)), Flag.NONE, 403);

	}

	@Test
	public void correctBehaviourTest() throws Exception {
		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafiles(datafileIds), Flag.NONE, 200);
		assertNotNull(preparedId);
	}

	@Test
	public void prepareDataset() throws Exception {

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(500);
			status = testingClient.getStatus(preparedId, 200);
		} while (status.equals(Status.RESTORING));

		assertEquals(Status.ONLINE, status);
		assertTrue(Files.exists(setup.getPreparedCacheDir().resolve(preparedId)));
	}

	@Test
	public void prepareTwoDatasets() throws Exception {

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDataset(datasetIds.get(1)),
				Flag.NONE, 200);

		Status status;
		do {
			Thread.sleep(500);
			status = testingClient.getStatus(preparedId, 200);
		} while (status.equals(Status.RESTORING));

		assertEquals(Status.ONLINE, status);
		assertTrue(Files.exists(setup.getPreparedCacheDir().resolve(preparedId)));
	}

	@Test
	public void prepareDatafile() throws Exception {

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.NONE, 200);

		Status status;
		do {
			Thread.sleep(500);
			status = testingClient.getStatus(preparedId, 200);
		} while (status.equals(Status.RESTORING));

		assertEquals(Status.ONLINE, status);
		assertTrue(Files.exists(setup.getPreparedCacheDir().resolve(preparedId)));
	}

	@Test
	public void prepareDatafileAndItsDataset() throws Exception {

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)).addDataset(datasetIds.get(0)),
				Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(500);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		assertEquals(Status.ONLINE, status);
		assertTrue(Files.exists(setup.getPreparedCacheDir().resolve(preparedId)));
	}

}
