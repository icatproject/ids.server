//package org.icatproject.ids.integration.two;
//
//import static org.junit.Assert.assertTrue;
//
//import java.io.File;
//
//import org.icatproject.Datafile;
//import org.icatproject.Dataset;
//import org.icatproject.ids.integration.BaseTest;
//import org.icatproject.ids.integration.util.Setup;
//import org.icatproject.ids.integration.util.client.DataSelection;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//public class RestoreTest extends BaseTest {
//
//	@BeforeClass
//	public static void setup() throws Exception {
//		setup = new Setup("two.properties");
//		icatsetup();
//	}
//
//	@Test
//	public void restoreArchivedDataset() throws Exception {
//
//		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", datasetIds.get(0));
//		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
//		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());
//
//		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 200);
//
//		do {
//			Thread.sleep(1000);
//		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());
//
//		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
//		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());
//	}
//
//	@Test
//	public void restoreTwoArchivedDatasets() throws Exception {
//
//		Dataset icatDs1 = (Dataset) icat
//				.get(setup.getGoodSessionId(), "Dataset", datasetIds.get(0));
//		File dirOnFastStorage1 = new File(setup.getStorageDir(), icatDs1.getLocation());
//		File zipOnFastStorage1 = new File(setup.getStorageZipDir(), icatDs1.getLocation());
//		Dataset icatDs2 = (Dataset) icat
//				.get(setup.getGoodSessionId(), "Dataset", datasetIds.get(1));
//		File dirOnFastStorage2 = new File(setup.getStorageDir(), icatDs2.getLocation());
//		File zipOnFastStorage2 = new File(setup.getStorageZipDir(), icatDs2.getLocation());
//
//		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0))
//				.addDataset(datasetIds.get(1)), 200);
//
//		do {
//			Thread.sleep(1000);
//		} while (!dirOnFastStorage1.exists() || !zipOnFastStorage1.exists()
//				|| !dirOnFastStorage2.exists() || !zipOnFastStorage2.exists());
//
//		assertTrue("File " + dirOnFastStorage1.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", dirOnFastStorage1.exists());
//		assertTrue("Zip in " + zipOnFastStorage1.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", zipOnFastStorage1.exists());
//		assertTrue("File " + dirOnFastStorage2.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", dirOnFastStorage2.exists());
//		assertTrue("Zip in " + zipOnFastStorage2.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", zipOnFastStorage2.exists());
//	}
//
//	@Test
//	public void restoreArchivedDatafile() throws Exception {
//
//		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
//				datafileIds.get(0));
//		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
//		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset()
//				.getLocation());
//
//		testingClient.restore(sessionId, new DataSelection().addDatafile(datafileIds.get(0)), 200);
//
//		do {
//			Thread.sleep(1000);
//		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());
//
//		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
//		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());
//	}
//
//	@Test
//	public void restoreArchivedDatafileAndItsDataset() throws Exception {
//
//		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
//				datafileIds.get(0));
//		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
//		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset()
//				.getLocation());
//
//		testingClient.restore(sessionId, new DataSelection().addDatafile(datafileIds.get(0))
//				.addDataset(datasetIds.get(0)), 200);
//
//		do {
//			Thread.sleep(1000);
//		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());
//
//		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
//		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());
//	}
// }
