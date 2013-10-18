//package org.icatproject.ids.integration;
//
//import static org.junit.Assert.assertTrue;
//
//import java.io.File;
//import java.net.URL;
//import java.nio.file.FileSystems;
//import java.nio.file.Files;
//import java.nio.file.Path;
//
//import javax.xml.namespace.QName;
//
//import org.icatproject.Datafile;
//import org.icatproject.Dataset;
//import org.icatproject.ICAT;
//import org.icatproject.ICATService;
//import org.icatproject.ids.integration.util.Setup;
//import org.icatproject.ids.integration.util.TestingClient;
//import org.icatproject.ids.integration.util.TreeDeleteVisitor;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//public class RestoreTest {
//
//	private static Setup setup = null;
//	private static ICAT icat;
//	TestingClient testingClient;
//
//	@BeforeClass
//	public static void setup() throws Exception {
//		setup = new Setup();
//		final URL icatUrl = new URL(setup.getIcatUrl());
//		final ICATService icatService = new ICATService(icatUrl, new QName(
//				"http://icatproject.org", "ICATService"));
//		icat = icatService.getICATPort();
//	}
//
//	@Before
//	public void clearFastStorage() throws Exception {
//		Path storageDir = FileSystems.getDefault().getPath(setup.getStorageDir());
//		Path storageZipDir = FileSystems.getDefault().getPath(setup.getStorageZipDir());
//		TreeDeleteVisitor treeDeleteVisitor = new TreeDeleteVisitor();
//		Files.walkFileTree(storageDir, treeDeleteVisitor);
//		Files.walkFileTree(storageZipDir, treeDeleteVisitor);
//		Files.createDirectories(storageDir);
//		Files.createDirectories(storageZipDir);
//		testingClient = new TestingClient(setup.getIdsUrl());
//	}
//
//	@Test
//	public void restoreArchivedDataset() throws Exception {
//		final int DS_NUM_FROM_PROPS = 0;
//		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
//				Long.parseLong(setup.getDatasetIds().get(DS_NUM_FROM_PROPS)));
//		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
//		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());
//
//		testingClient.restore(setup.getGoodSessionId(), null,
//				setup.getDatasetIds().get(DS_NUM_FROM_PROPS), null);
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
//		final int DS1_NUM_FROM_PROPS = 0;
//		final int DS2_NUM_FROM_PROPS = 1;
//		Dataset icatDs1 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
//				Long.parseLong(setup.getDatasetIds().get(DS1_NUM_FROM_PROPS)));
//		File dirOnFastStorage1 = new File(setup.getStorageDir(), icatDs1.getLocation());
//		File zipOnFastStorage1 = new File(setup.getStorageZipDir(), icatDs1.getLocation());
//		Dataset icatDs2 = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
//				Long.parseLong(setup.getDatasetIds().get(DS2_NUM_FROM_PROPS)));
//		File dirOnFastStorage2 = new File(setup.getStorageDir(), icatDs2.getLocation());
//		File zipOnFastStorage2 = new File(setup.getStorageZipDir(), icatDs2.getLocation());
//		String dsIds = setup.getDatasetIds().get(DS1_NUM_FROM_PROPS) + ", "
//				+ setup.getDatasetIds().get(DS2_NUM_FROM_PROPS);
//
//		testingClient.restore(setup.getGoodSessionId(), null, dsIds, null);
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
//		final int DF_NUM_FROM_PROPS = 0;
//		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
//				Long.parseLong(setup.getDatafileIds().get(DF_NUM_FROM_PROPS)));
//		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
//		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset()
//				.getLocation());
//
//		testingClient.restore(setup.getGoodSessionId(), null, null,
//				setup.getDatafileIds().get(DF_NUM_FROM_PROPS));
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
//		final int DF_NUM_FROM_PROPS = 0;
//		final int DS_NUM_FROM_PROPS = 0;
//		Datafile icatDf = (Datafile) icat.get(setup.getGoodSessionId(), "Datafile INCLUDE Dataset",
//				Long.parseLong(setup.getDatafileIds().get(DF_NUM_FROM_PROPS)));
//		File dirOnFastStorage = new File(setup.getStorageDir(), icatDf.getDataset().getLocation());
//		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDf.getDataset()
//				.getLocation());
//
//		testingClient.restore(setup.getGoodSessionId(), null,
//				setup.getDatasetIds().get(DS_NUM_FROM_PROPS),
//				setup.getDatafileIds().get(DF_NUM_FROM_PROPS));
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
