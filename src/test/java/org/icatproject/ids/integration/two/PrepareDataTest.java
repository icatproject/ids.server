package org.icatproject.ids.integration.two;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Path;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.icatproject.ids.integration.util.client.TestingClient.Status;
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
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", datasetIds.get(0));
		Path dirOnFastStorage = setup.getStorageDir().resolve(icatDs.getLocation());
		Path datasetCacheFile = setup.getDatasetCacheDir().resolve(icatDs.getLocation());
		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		assertEquals(Status.ONLINE, status);
		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);
	}

	@Test
	public void prepareTwoArchivedDatasets() throws Exception {

		Dataset icatDs1 = (Dataset) icat.get(sessionId, "Dataset", datasetIds.get(0));
		Path dirOnFastStorage1 = setup.getStorageDir().resolve(icatDs1.getLocation());
		Path datasetCacheFile1 = setup.getDatasetCacheDir().resolve(icatDs1.getLocation());
		Dataset icatDs2 = (Dataset) icat
				.get(setup.getGoodSessionId(), "Dataset", datasetIds.get(1));
		Path dirOnFastStorage2 = setup.getStorageDir().resolve(icatDs2.getLocation());
		Path datasetCacheFile2 = setup.getDatasetCacheDir().resolve(icatDs2.getLocation());

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDataset(datasetIds.get(1)),
				Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		assertEquals(Status.ONLINE, status);
		checkPresent(dirOnFastStorage1);
		checkPresent(datasetCacheFile1);
		checkPresent(dirOnFastStorage2);
		checkPresent(datasetCacheFile2);
	}

	@Test
	public void prepareArchivedDatafile() throws Exception {
		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
				datafileIds.get(0));
		Path dirOnFastStorage = setup.getStorageDir().resolve(icatDf.getDataset().getLocation());
		Path datasetCacheFile = setup.getDatasetCacheDir().resolve(
				icatDf.getDataset().getLocation());

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.NONE, 200);

		Status status;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		assertEquals(Status.ONLINE, status);
		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);
	}

	@Test
	public void prepareArchivedDatafileAndItsDataset() throws Exception {

		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
				datafileIds.get(0));
		Path dirOnFastStorage = setup.getStorageDir().resolve(icatDf.getDataset().getLocation());
		Path datasetCacheFile = setup.getDatasetCacheDir().resolve(
				icatDf.getDataset().getLocation());

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDatafile(datafileIds.get(0)),
				Flag.NONE, 200);

		Status status;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		assertEquals(Status.ONLINE, status);
		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);
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

	@Test(expected = BadRequestException.class)
	public void nonExistingSessionIdTest() throws Exception {
		testingClient.prepareData("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", new DataSelection(),
				Flag.NONE, 400);

	}

	@Test
	public void correctBehaviourTest() throws Exception {
		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafiles(datafileIds), Flag.NONE, 200);
		assertNotNull(preparedId);
	}

	@Test
	public void prepareRestoredDataset() throws Exception {
		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", datasetIds.get(0));
		Path dirOnFastStorage = setup.getStorageDir().resolve(icatDs.getLocation());
		Path datasetCacheFile = setup.getDatasetCacheDir().resolve(icatDs.getLocation());

		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 200);

		waitForIds();

		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (status.equals(Status.RESTORING));

		assertEquals(Status.ONLINE, status);
		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);
		checkPresent(setup.getPreparedCacheDir().resolve(preparedId));
	}

	@Test
	public void prepareTwoRestoredDatasets() throws Exception {

		Dataset icatDs1 = (Dataset) icat.get(sessionId, "Dataset", datasetIds.get(0));
		Path dirOnFastStorage1 = setup.getStorageDir().resolve(icatDs1.getLocation());
		Path datasetCacheFile1 = setup.getDatasetCacheDir().resolve(icatDs1.getLocation());
		Dataset icatDs2 = (Dataset) icat
				.get(setup.getGoodSessionId(), "Dataset", datasetIds.get(1));
		Path dirOnFastStorage2 = setup.getStorageDir().resolve(icatDs2.getLocation());
		Path datasetCacheFile2 = setup.getDatasetCacheDir().resolve(icatDs2.getLocation());
		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0))
				.addDataset(datasetIds.get(1)), 200);

		waitForIds();

		checkPresent(dirOnFastStorage1);
		checkPresent(datasetCacheFile1);
		checkPresent(dirOnFastStorage2);
		checkPresent(datasetCacheFile2);

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDataset(datasetIds.get(1)),
				Flag.NONE, 200);

		Status status;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (status.equals(Status.RESTORING));

		assertEquals(Status.ONLINE, status);
		Path preparedFile = setup.getPreparedCacheDir().resolve(preparedId);
		checkPresent(preparedFile);
	}

	@Test
	public void prepareRestoredDatafile() throws Exception {

		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
				datafileIds.get(0));
		Path dirOnFastStorage = setup.getStorageDir().resolve(icatDf.getDataset().getLocation());
		Path datasetCacheFile = setup.getDatasetCacheDir().resolve(
				icatDf.getDataset().getLocation());

		testingClient.restore(sessionId, new DataSelection().addDatafile(datafileIds.get(0)), 200);

		waitForIds();

		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.NONE, 200);

		Status status;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (status.equals(Status.RESTORING));

		assertEquals(Status.ONLINE, status);
		Path preparedFile = setup.getPreparedCacheDir().resolve(preparedId);
		checkPresent(preparedFile);
	}

	@Test
	public void prepareRestoredDatafileAndItsDataset() throws Exception {

		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
				datafileIds.get(0));
		Path dirOnFastStorage = setup.getStorageDir().resolve(icatDf.getDataset().getLocation());
		Path datasetCacheFile = setup.getDatasetCacheDir().resolve(
				icatDf.getDataset().getLocation());

		testingClient.restore(sessionId, new DataSelection().addDatafile(datafileIds.get(0))
				.addDataset(datasetIds.get(0)), 200);
		waitForIds();
		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)).addDataset(datasetIds.get(0)),
				Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		waitForIds();
		Path preparedFile = setup.getPreparedCacheDir().resolve(preparedId);
		checkPresent(preparedFile);
		assertEquals(Status.ONLINE, status);
	}

}
