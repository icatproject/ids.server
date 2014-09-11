package org.icatproject.ids.integration.two;

import static org.junit.Assert.assertNotNull;

import java.nio.file.Path;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.junit.BeforeClass;
import org.junit.Test;

public class PrepareDataTest extends BaseTest {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
	}

	@Test
	public void prepareArchivedDataset() throws Exception {
		Path dirOnFastStorage = getDirOnFastStorage(datasetIds.get(0));
		Path datasetCacheFile = getDatasetCacheFile(datasetIds.get(0));

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)), Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);
	}

	@Test
	public void prepareTwoArchivedDatasets() throws Exception {

		Path dirOnFastStorage1 = getDirOnFastStorage(datasetIds.get(0));
		Path datasetCacheFile1 = getDatasetCacheFile(datasetIds.get(0));
		Path dirOnFastStorage2 = getDirOnFastStorage(datasetIds.get(1));
		Path datasetCacheFile2 = getDatasetCacheFile(datasetIds.get(1));

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDataset(datasetIds.get(1)),
				Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		checkPresent(dirOnFastStorage1);
		checkPresent(datasetCacheFile1);
		checkPresent(dirOnFastStorage2);
		checkPresent(datasetCacheFile2);
	}

	@Test
	public void prepareArchivedDatafile() throws Exception {
		Path dirOnFastStorage = getDirOnFastStorage(datasetIds.get(0));
		Path datasetCacheFile = getDatasetCacheFile(datasetIds.get(0));

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);
	}

	@Test
	public void prepareArchivedDatafileAndItsDataset() throws Exception {
		Path dirOnFastStorage = getDirOnFastStorage(datasetIds.get(0));
		Path datasetCacheFile = getDatasetCacheFile(datasetIds.get(0));

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDatafile(datafileIds.get(0)),
				Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);
	}

	@Test(expected = BadRequestException.class)
	public void badSessionIdFormatTest() throws Exception {
		testingClient.prepareData("bad sessionId format",
				new DataSelection().addDataset(datasetIds.get(0)), Flag.NONE, 400);
	}

	@Test
	public void noIdsTest() throws Exception {
		testingClient.prepareData(sessionId, new DataSelection(), Flag.NONE, 200);
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
	public void prepareRestoredDataset() throws Exception {
		Path dirOnFastStorage = getDirOnFastStorage(datasetIds.get(0));
		Path datasetCacheFile = getDatasetCacheFile(datasetIds.get(0));

		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 204);

		waitForIds();

		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)), Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);
		checkPresent(setup.getPreparedCacheDir().resolve(preparedId));
	}

	@Test
	public void prepareTwoRestoredDatasets() throws Exception {

		Path dirOnFastStorage1 = getDirOnFastStorage(datasetIds.get(0));
		Path datasetCacheFile1 = getDatasetCacheFile(datasetIds.get(0));
		Path dirOnFastStorage2 = getDirOnFastStorage(datasetIds.get(1));
		Path datasetCacheFile2 = getDatasetCacheFile(datasetIds.get(1));

		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0))
				.addDataset(datasetIds.get(1)), 204);

		waitForIds();

		checkPresent(dirOnFastStorage1);
		checkPresent(datasetCacheFile1);
		checkPresent(dirOnFastStorage2);
		checkPresent(datasetCacheFile2);

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDataset(datasetIds.get(1)),
				Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		Path preparedFile = setup.getPreparedCacheDir().resolve(preparedId);
		checkPresent(preparedFile);
	}

	@Test
	public void prepareRestoredDatafile() throws Exception {

		Path dirOnFastStorage = getDirOnFastStorage(datasetIds.get(0));
		Path datasetCacheFile = getDatasetCacheFile(datasetIds.get(0));

		testingClient.restore(sessionId, new DataSelection().addDatafile(datafileIds.get(0)), 204);

		waitForIds();

		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		Path preparedFile = setup.getPreparedCacheDir().resolve(preparedId);
		checkPresent(preparedFile);
	}

	@Test
	public void prepareRestoredDatafileAndItsDataset() throws Exception {

		Path dirOnFastStorage = getDirOnFastStorage(datasetIds.get(0));
		Path datasetCacheFile = getDatasetCacheFile(datasetIds.get(0));

		testingClient.restore(sessionId, new DataSelection().addDatafile(datafileIds.get(0))
				.addDataset(datasetIds.get(0)), 204);
		waitForIds();
		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)).addDataset(datasetIds.get(0)),
				Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		checkPresent(setup.getPreparedCacheDir().resolve(preparedId));

	}

}
