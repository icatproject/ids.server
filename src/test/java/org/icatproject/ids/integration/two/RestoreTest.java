package org.icatproject.ids.integration.two;

import java.nio.file.Path;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.junit.BeforeClass;
import org.junit.Test;

public class RestoreTest extends BaseTest {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
	}

	@Test
	public void restoreArchivedDataset() throws Exception {

		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", datasetIds.get(0));
		Path dirOnFastStorage = setup.getStorageDir().resolve(icatDs.getLocation());
		Path datasetCacheFile = setup.getDatasetCacheDir().resolve(icatDs.getLocation());

		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 200);

		waitForIds();

		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);
	}

	@Test
	public void restoreTwoArchivedDatasets() throws Exception {

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
	}

	@Test
	public void restoreArchivedDatafile() throws Exception {

		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
				datafileIds.get(0));
		Path dirOnFastStorage = setup.getStorageDir().resolve(icatDf.getDataset().getLocation());
		Path datasetCacheFile = setup.getDatasetCacheDir().resolve(
				icatDf.getDataset().getLocation());

		testingClient.restore(sessionId, new DataSelection().addDatafile(datafileIds.get(0)), 200);
		waitForIds();

		checkPresent(dirOnFastStorage);
		checkPresent(datasetCacheFile);
	}

	@Test
	public void restoreArchivedDatafileAndItsDataset() throws Exception {

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
	}
}
