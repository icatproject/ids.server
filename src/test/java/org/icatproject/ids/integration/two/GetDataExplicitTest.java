//package org.icatproject.ids.integration.two;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.fail;
//
//import java.io.File;
//import java.net.HttpURLConnection;
//
//import org.icatproject.Dataset;
//import org.icatproject.ids.integration.BaseTest;
//import org.icatproject.ids.integration.util.Setup;
//import org.icatproject.ids.integration.util.client.BadRequestException;
//import org.icatproject.ids.integration.util.client.DataNotOnlineException;
//import org.icatproject.ids.integration.util.client.DataSelection;
//import org.icatproject.ids.integration.util.client.IdsException;
//import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
//import org.icatproject.ids.integration.util.client.TestingClient.Flag;
//import org.icatproject.ids.integration.util.client.TestingClient.Method;
//import org.icatproject.ids.integration.util.client.TestingClient.ParmPos;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//public class GetDataExplicitTest extends BaseTest {
//
//	
//	// value of the offset in bytes
//	final Integer goodOffset = 20;
//	final Integer badOffset = 99999999;
//	
//	@BeforeClass
//	public static void setup() throws Exception {
//		setup = new Setup("two.properties");
//		icatsetup();
//	}
//
//	
//
//	@Test(expected = BadRequestException.class)
//	public void badPreparedIdFormatTest() throws Exception {
//		parameters.put("preparedId", "bad preparedId format");
//		testingClient.process("getData", parameters, Method.GET, ParmPos.URL, null, null, 400);
//	}
//
//	@Test(expected = BadRequestException.class)
//	public void badDatafileIdFormatTest() throws Exception {
//		parameters.put("sessionId", setup.getGoodSessionId());
//		parameters.put("datafileIds", "notADatafile");
//		testingClient.process("getData", parameters, Method.GET, ParmPos.URL, null, null, 400);
//	}
//
//	@Test(expected = InsufficientPrivilegesException.class)
//	public void forbiddenTest() throws Exception {
//		parameters.put("sessionId", setup.getForbiddenSessionId());
//		parameters.put("datafileIds", datafileIds.toString().replace("[", "").replace("]", "")
//				.replace(" ", ""));
//		testingClient.process("getData", parameters, Method.GET, ParmPos.URL, null, null, 403);
//	}
//
//	@Test
//	public void correctBehaviourTest() throws Exception {
//		parameters.put("sessionId", sessionId);
//		parameters.put("datafileIds", datafileIds.toString().replace("[", "").replace("]", "")
//				.replace(" ", ""));
//
//		HttpURLConnection response;
//		try {
//			response = testingClient.process("getData", parameters, Method.GET, ParmPos.URL, null,
//					null, 404);
//			fail("Should have thrown exception");
//		} catch (IdsException e) {
//			assertEquals(DataNotOnlineException.class, e.getClass());
//		}
//
//		while (true) {
//			Thread.sleep(1000);
//			try {
//				response = testingClient.process("getData", parameters, Method.GET, ParmPos.URL,
//						null, null, null);
//				break;
//			} catch (IdsException e) {
//				assertEquals(DataNotOnlineException.class, e.getClass());
//			}
//		}
//
//		// TODO Map<String, String> map = TestingUtils.filenameMD5Map(response.getInputStream());
//		// TestingUtils.checkMD5Values(map, setup);
//		fail();
//	}
//
//	@Test
//	public void gettingDatafileRestoresItsDatasetTest() throws Exception {
//
//		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset", datasetIds.get(1));
//		Dataset unrestoredIcatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
//				datasetIds.get(0));
//		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
//		File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());
//		File unrestoredDirOnFastStorage = new File(setup.getStorageDir(),
//				unrestoredIcatDs.getLocation());
//		File unrestoredZipOnFastStorage = new File(setup.getStorageZipDir(),
//				unrestoredIcatDs.getLocation());
//
//		try {
//			testingClient.getData(sessionId, new DataSelection().addDatafile(datafileIds.get(2)),
//					Flag.NONE, null, 0, 404);
//			fail("Should have thrown an exception");
//		} catch (DataNotOnlineException e) {
//			// All is well
//		}
//
//		do {
//			Thread.sleep(1000);
//		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());
//
//		assertFalse("File " + unrestoredDirOnFastStorage.getAbsolutePath()
//				+ " shouldn't have been restored, but exist", unrestoredDirOnFastStorage.exists());
//		assertFalse("Zip in " + unrestoredZipOnFastStorage.getAbsolutePath()
//				+ " shouldn't have been restored, but exist", unrestoredZipOnFastStorage.exists());
//
//		testingClient.getData(sessionId, new DataSelection().addDatafile(datafileIds.get(2)),
//				Flag.NONE, null, 0, 200);
//
//	}
//
//	@Test
//	public void gettingDatafileAndDatasetShouldRestoreBothDatasetsTest() throws Exception {
//		Dataset icatDs0 = (Dataset) icat
//				.get(setup.getGoodSessionId(), "Dataset", datasetIds.get(0));
//		Dataset icatDs1 = (Dataset) icat
//				.get(setup.getGoodSessionId(), "Dataset", datasetIds.get(1));
//		File dirOnFastStorage0 = new File(setup.getStorageDir(), icatDs0.getLocation());
//		File zipOnFastStorage0 = new File(setup.getStorageZipDir(), icatDs0.getLocation());
//		File dirOnFastStorage1 = new File(setup.getStorageDir(), icatDs1.getLocation());
//		File zipOnFastStorage1 = new File(setup.getStorageZipDir(), icatDs1.getLocation());
//
//		try {
//			testingClient.getData(sessionId, new DataSelection().addDatafile(datafileIds.get(2))
//					.addDataset(datasetIds.get(0)), Flag.NONE, null, 0, 404);
//			fail("Should throw exception");
//		} catch (DataNotOnlineException e) {
//			// All is well
//		}
//
//		do {
//			Thread.sleep(1000);
//		} while (!dirOnFastStorage0.exists() || !zipOnFastStorage0.exists()
//				|| !dirOnFastStorage1.exists() || !zipOnFastStorage1.exists());
//		testingClient.getData(sessionId, new DataSelection().addDatafile(datafileIds.get(2))
//				.addDataset(datasetIds.get(0)), Flag.NONE, null, 0, 200);
//
//	}
//
// }
