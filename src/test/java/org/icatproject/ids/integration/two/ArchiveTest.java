//package org.icatproject.ids.integration.two;
//
//import static org.junit.Assert.assertTrue;
//
//import java.io.File;
//import java.nio.file.Path;
//import java.util.Arrays;
//
//import org.icatproject.Dataset;
//import org.icatproject.ids.integration.BaseTest;
//import org.icatproject.ids.integration.util.Setup;
//import org.icatproject.ids.integration.util.client.BadRequestException;
//import org.icatproject.ids.integration.util.client.DataSelection;
//import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
//import org.icatproject.ids.integration.util.client.TestingClient.Method;
//import org.icatproject.ids.integration.util.client.TestingClient.ParmPos;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//public class ArchiveTest extends BaseTest {
//
//	@BeforeClass
//	public static void setup() throws Exception {
//		setup = new Setup("two.properties");
//		icatsetup();
//	}
//
//	@Test
//	public void restoreThenArchiveDataset() throws Exception {
//		Dataset icatDs = (Dataset) icat.get(sessionId, "Dataset", datasetIds.get(0));
//		Path dirOnFastStorage = setup.getStorageDir().resolve(icatDs.getLocation());
//		Path zipOnFastStorage = setup.getStorageZipDir().resolve(icatDs.getLocation());
//
//		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 200);
//		do {
//			Thread.sleep(1000);
//		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());
//		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
//		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());
//
//		testingClient.archive(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 200);
//		while (dirOnFastStorage.listFiles().length > 0 || zipOnFastStorage.exists()) {
//			Thread.sleep(1000);
//		}
//		assertTrue("Directory " + dirOnFastStorage.getAbsolutePath()
//				+ " should have been cleaned, but still contains files",
//				dirOnFastStorage.listFiles().length == 0);
//		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
//				+ " should have been archived, but still exists", !zipOnFastStorage.exists());
//	}
//
//	@Test(expected = BadRequestException.class)
//	public void badSessionIdFormatTest() throws Exception {
//		testingClient.archive("bad sessionId format",
//				new DataSelection().addDatafiles(Arrays.asList(1L, 2L)), 400);
//	}
//
//	@Test(expected = BadRequestException.class)
//	public void badDatafileIdFormatTest() throws Exception {
//		parameters.put("sessionId", sessionId);
//		parameters.put("datafileIds", "1,2,a");
//		testingClient.process("restore", parameters, Method.POST, ParmPos.BODY, null, null, 400);
//	}
//
//	@Test(expected = BadRequestException.class)
//	public void badDatasetIdFormatTest() throws Exception {
//
//		parameters.put("sessionId", sessionId);
//		parameters.put("datafileIds", "");
//		testingClient.process("restore", parameters, Method.POST, ParmPos.BODY, null, null, 400);
//	}
//
//	@Test(expected = BadRequestException.class)
//	public void noIdsTest() throws Exception {
//		testingClient.archive(sessionId, new DataSelection(), 400);
//	}
//
//	@Test(expected = InsufficientPrivilegesException.class)
//	public void nonExistingSessionIdTest() throws Exception {
//		testingClient.archive("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
//				new DataSelection().addDatafiles(Arrays.asList(1L, 2L)), 403);
//	}
//
// }
