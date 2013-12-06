package org.icatproject.ids.integration.one;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Path;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.NotFoundException;
import org.icatproject.ids.integration.util.client.TestingClient.Status;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeleteTest extends BaseTest {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("one.properties");
		icatsetup();
	}

	@Test
	public void deleteDatafileTest() throws Exception {
		DataSelection dsel = new DataSelection().addDatafile(datafileIds.get(3));
		assertEquals(Status.ONLINE, testingClient.getStatus(sessionId, dsel, 200));
		testingClient.delete(sessionId, new DataSelection().addDatafile(datafileIds.get(3)), 200);
		try {
			testingClient.getStatus(sessionId, dsel, 404);
			fail();
		} catch (NotFoundException e) {
			// pass
		}
	}

	@Test
	public void deleteDatasetTest() throws Exception {
		Path dirOnFastStorage = getDirOnFastStorage(datasetIds.get(1));
		DataSelection dsel = new DataSelection().addDataset(datasetIds.get(1));
		assertEquals(Status.ONLINE, testingClient.getStatus(sessionId, dsel, 200));
		assertTrue(Files.exists(dirOnFastStorage));

		System.out.println(dirOnFastStorage);

		testingClient.delete(sessionId, dsel, 200);
		assertEquals(Status.ONLINE, testingClient.getStatus(sessionId, dsel, 200));
		assertFalse(Files.exists(dirOnFastStorage));

	}
}
