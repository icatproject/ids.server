//package org.icatproject.ids.integration;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//
//import java.io.File;
//import java.nio.file.FileSystems;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.HashMap;
//import java.util.Map;
//
//import javax.xml.namespace.QName;
//
//import org.icatproject.Dataset;
//import org.icatproject.ICAT;
//import org.icatproject.ICATService;
//import org.icatproject.ids.integration.util.Form;
//import org.icatproject.ids.integration.util.Setup;
//import org.icatproject.ids.integration.util.TestingClient;
//import org.icatproject.ids.integration.util.TreeDeleteVisitor;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//public class ArchiveTest {
//
//	private static Setup setup = null;
//	private static ICAT icat;
//	private TestingClient testingClient;
//
//	@BeforeClass
//	public static void setup() throws Exception {
//		setup = new Setup();
//		final ICATService icatService = new ICATService(setup.getIcatUrl(), new QName(
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
//	public void restoreThenArchiveDataset() throws Exception {
//		Dataset icatDs = (Dataset) icat.get(setup.getGoodSessionId(), "Dataset",
//				Long.parseLong(setup.getDatasetIds().get(0)));
//		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
//		File zipOnFastStorage = new File(new File(setup.getStorageZipDir(), icatDs.getLocation()),
//				"files.zip");
//
//		Map<String, String> form = new HashMap<>();
//		form.add("sessionId", sessionId);
//		
//			form.add("datasetIds", datasetIds);
//		if (datafileIds != null)
//			form.add("datafileIds", datafileIds);
//		result = testingClient.restore(setup.getGoodSessionId(), null,
//				setup.getDatasetIds().get(0), null);
//		do {
//			Thread.sleep(1000);
//		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());
//
//		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
//		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
//				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());
//
//		testingClient.archive(setup.getGoodSessionId(), null,
//				setup.getDatasetIds().get(0), null);
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
//	@Test
//	public void badSessionIdFormatTest() throws Exception {
//		int expectedSc = 400;
//		try {
//			TestingClient client = new TestingClient(setup.getIdsUrl());
//			client.archive("bad sessionId format", null, null, "1,2");
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void badDatafileIdFormatTest() throws Exception {
//		int expectedSc = 400;
//		try {
//			TestingClient client = new TestingClient(setup.getIdsUrl());
//			client.archive(setup.getGoodSessionId(), null, null, "1,2,a");
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void badDatasetIdFormatTest() throws Exception {
//		int expectedSc = 400;
//		try {
//			TestingClient client = new TestingClient(setup.getIdsUrl());
//			client.archive(setup.getGoodSessionId(), null, "", null);
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void noIdsTest() throws Exception {
//		int expectedSc = 400;
//		try {
//			TestingClient client = new TestingClient(setup.getIdsUrl());
//			client.archive(setup.getGoodSessionId(), null, null, null);
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void nonExistingSessionIdTest() throws Exception {
//		int expectedSc = 403;
//		try {
//			TestingClient client = new TestingClient(setup.getIdsUrl());
//			client.archive("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", null, "1,2", null);
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//}
