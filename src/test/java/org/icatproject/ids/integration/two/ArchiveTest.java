package org.icatproject.ids.integration.two;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.icatproject.Dataset;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.icatproject.ids.integration.util.client.TestingClient.Method;
import org.icatproject.ids.integration.util.client.TestingClient.ParmPos;
import org.junit.BeforeClass;
import org.junit.Test;

public class ArchiveTest extends BaseTest {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
	}

	@Test
	public void restoreThenArchiveDataset() throws Exception {
		Dataset icatDs = (Dataset) icat.get(sessionId, "Dataset", datasetIds.get(0));
		Path dirOnFastStorage = setup.getStorageDir().resolve(icatDs.getLocation());
		Path zipOnFastStorage = setup.getDatasetCacheDir().resolve(icatDs.getLocation());

		assertFalse(Files.exists(dirOnFastStorage));
		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 200);
		while (!Files.exists(dirOnFastStorage)) {
			Thread.sleep(1000);
		}

		assertTrue(Files.exists(zipOnFastStorage));
		testingClient.archive(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 200);
		while (Files.exists(zipOnFastStorage) || Files.exists(dirOnFastStorage)) {
			Thread.sleep(1000);
		}

	}

	@Test(expected = BadRequestException.class)
	public void badSessionIdFormatTest() throws Exception {
		testingClient.archive("bad sessionId format",
				new DataSelection().addDatafiles(Arrays.asList(1L, 2L)), 400);
	}

	@Test(expected = BadRequestException.class)
	public void badDatafileIdFormatTest() throws Exception {
		parameters.put("sessionId", sessionId);
		parameters.put("datafileIds", "1,2,a");
		testingClient.process("restore", parameters, Method.POST, ParmPos.BODY, null, null, 400);
	}

	@Test(expected = BadRequestException.class)
	public void badDatasetIdFormatTest() throws Exception {

		parameters.put("sessionId", sessionId);
		parameters.put("datafileIds", "");
		testingClient.process("restore", parameters, Method.POST, ParmPos.BODY, null, null, 400);
	}

	@Test(expected = BadRequestException.class)
	public void noIdsTest() throws Exception {
		testingClient.archive(sessionId, new DataSelection(), 400);
	}

	@Test(expected = InsufficientPrivilegesException.class)
	public void nonExistingSessionIdTest() throws Exception {
		testingClient.archive("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
				new DataSelection().addDatafiles(Arrays.asList(1L, 2L)), 403);
	}

}
