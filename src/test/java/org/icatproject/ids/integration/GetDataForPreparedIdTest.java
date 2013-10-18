//package org.icatproject.ids.integration;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.fail;
//
//import java.net.URL;
//import java.nio.file.FileSystems;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.Map;
//
//import javax.xml.namespace.QName;
//
//import org.icatproject.ICAT;
//import org.icatproject.ICATService;
//import org.icatproject.ids.integration.util.Response;
//import org.icatproject.ids.integration.util.Setup;
//import org.icatproject.ids.integration.util.TestingClient;
//import org.icatproject.ids.integration.util.TestingUtils;
//import org.icatproject.ids.integration.util.TreeDeleteVisitor;
//import org.icatproject.ids.webservice.Status;
//import org.icatproject.ids.webservice.exceptions.ForbiddenException;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import com.sun.jersey.api.client.UniformInterfaceException;
//
///*
// * Test the getData method for the IDS. This is a bit more involved than the
// * tests performed on the other methods as it requires verifying that the files
// * downloaded are correct as well as the web service responses.
// * 
// * TODO: move offsets into test.properties?
// */
//public class GetDataForPreparedIdTest {
//
//	private static Setup setup = null;
//	@SuppressWarnings("unused")
//	private static ICAT icat;
//	private TestingClient testingClient;
//
//	// value of the offset in bytes
//	final Integer goodOffset = 20;
//	final Integer badOffset = 99999999;
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
//	public void badPreparedIdFormatTest() throws Exception {
//		int expectedSc = 400;
//		try {
//			testingClient.getData("bad preparedId format", null, null);
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void badFileNameFormatTest() throws Exception {
//		int expectedSc = 400;
//		try {
//			String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null,
//					setup.getDatafileIds().get(0), null, null);
//
//			Status status = null;
//			do {
//				Thread.sleep(1000);
//				status = testingClient.getStatus(preparedId);
//			} while (Status.RESTORING.equals(status));
//
//			testingClient.getData(preparedId, "this/is/a/bad/file/name", null);
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void badOffsetFormatTest() throws Exception {
//		int expectedSc = 400;
//		try {
//			String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null,
//					setup.getDatafileIds().get(0), null, null);
//
//			Status status = null;
//			do {
//				Thread.sleep(1000);
//				status = testingClient.getStatus(preparedId);
//			} while (Status.RESTORING.equals(status));
//
//			testingClient.getData(preparedId, null, -10L);
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void nonExistantPreparedIdTest() throws Exception {
//		int expectedSc = 404;
//		try {
//			testingClient.getData("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", null, null);
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void forbiddenTest() throws Exception {
//		int expectedSc = 403;
//		try {
//			String preparedId = testingClient.prepareData(setup.getForbiddenSessionId(), null,
//					null, setup.getDatafileIds().get(0), null, null);
//
//			try {
//				Status status = null;
//				do {
//					Thread.sleep(1000);
//					status = testingClient.getStatus(preparedId);
//				} while (Status.RESTORING.equals(status));
//			} catch (ForbiddenException e) {
//				// ignore because testing to see if getData throws a ForbiddenException as well
//			}
//
//			testingClient.getData(preparedId, null, null);
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void offsetTooBigTest() throws Exception {
//		int expectedSc = 400;
//		try {
//			String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null,
//					setup.getDatafileIds().get(0), null, null);
//
//			Status status = null;
//			do {
//				Thread.sleep(1000);
//				status = testingClient.getStatus(preparedId);
//			} while (Status.RESTORING.equals(status));
//
//			testingClient.getData(preparedId, null, badOffset.longValue());
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void correctBehaviourNoOffsetTest() throws Exception {
//		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null, setup
//				.getDatafileIds().get(0), null, null);
//
//		Status status = null;
//		do {
//			Thread.sleep(1000);
//			status = testingClient.getStatus(preparedId);
//		} while (Status.RESTORING.equals(status));
//
//		Response response = testingClient.getData(preparedId, null, null);
//		Map<String, String> map = TestingUtils.filenameMD5Map(response.getResponse());
//		TestingUtils.checkMD5Values(map, setup);
//	}
//
//	@Test
//	public void correctBehaviourNoOffsetMultipleDatafilesTest() throws Exception {
//		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null,
//				setup.getCommaSepDatafileIds(), null, null);
//
//		Status status = null;
//		do {
//			Thread.sleep(1000);
//			status = testingClient.getStatus(preparedId);
//		} while (Status.RESTORING.equals(status));
//
//		Response response = testingClient.getData(preparedId, null, null);
//		Map<String, String> map = TestingUtils.filenameMD5Map(response.getResponse());
//		TestingUtils.checkMD5Values(map, setup);
//	}
//
//	@Test
//	public void correctBehaviourNoOffsetWithDatasetTest() throws Exception {
//		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, setup
//				.getDatasetIds().get(0), null, null, null);
//
//		Status status = null;
//		do {
//			Thread.sleep(1000);
//			status = testingClient.getStatus(preparedId);
//		} while (Status.RESTORING.equals(status));
//
//		Response response = testingClient.getData(preparedId, null, null);
//		Map<String, String> map = TestingUtils.filenameMD5Map(response.getResponse());
//		TestingUtils.checkMD5Values(map, setup);
//	}
//
//	@Test
//	public void correctBehaviourNoOffsetWithDatasetAndDatafileTest() throws Exception {
//
//		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, setup
//				.getDatasetIds().get(0), setup.getCommaSepDatafileIds(), null, null);
//
//		Status status = null;
//		do {
//			System.out.println("preparing...");
//			Thread.sleep(1000);
//			status = testingClient.getStatus(preparedId);
//		} while (Status.RESTORING.equals(status));
//
//		Response response = testingClient.getData(preparedId, null, null);
//		Map<String, String> map = TestingUtils.filenameMD5Map(response.getResponse());
//		TestingUtils.checkMD5Values(map, setup);
//	}
//
//	@Test
//	public void correctBehaviourWithOffsetTest() throws Exception {
//		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null, setup
//				.getDatafileIds().get(0), null, null);
//
//		Status status = null;
//		do {
//			Thread.sleep(1000);
//			status = testingClient.getStatus(preparedId);
//		} while (Status.RESTORING.equals(status));
//
//		// request the zip file twice, with and without an offset
//		Response zip = testingClient.getData(preparedId, null, null);
//		Response zipoffset = testingClient.getData(preparedId, null, goodOffset.longValue());
//
//		// check that the full zip file is valid
//		Map<String, String> map = TestingUtils.filenameMD5Map(zip.getResponse());
//		TestingUtils.checkMD5Values(map, setup);
//
//		// compare the two zip files byte by byte taking into account the offset
//		byte[] a = zip.getResponse().toByteArray();
//		byte[] b = zipoffset.getResponse().toByteArray();
//		for (int i = 0; i < b.length; i++) {
//			Assert.assertEquals("Byte offset: " + i, (byte) b[i], (byte) a[i + goodOffset]);
//		}
//	}
//
//	@Test
//	public void correctBehaviourFilenameWithExtensionTest() throws Exception {
//		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null, setup
//				.getDatafileIds().get(0), null, null);
//
//		Status status = null;
//		do {
//			Thread.sleep(1000);
//			status = testingClient.getStatus(preparedId);
//		} while (Status.RESTORING.equals(status));
//
//		Response response = testingClient
//				.getData(preparedId, "testfilenamewithextension.zip", null);
//		Assert.assertEquals("Downloaded filename does not match requested filename",
//				response.getFilename(), "testfilenamewithextension.zip");
//	}
//
//	@Test
//	public void correctBehaviourFilenameWithoutExtensionTest() throws Exception {
//		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null, setup
//				.getDatafileIds().get(0), null, null);
//
//		Status status = null;
//		do {
//			Thread.sleep(1000);
//			status = testingClient.getStatus(preparedId);
//		} while (Status.RESTORING.equals(status));
//
//		Response response = testingClient.getData(preparedId, "testfilenamewithoutextension", null);
//		Assert.assertEquals("Downloaded filename does not match requested filename",
//				response.getFilename(), "testfilenamewithoutextension.zip");
//	}
//}