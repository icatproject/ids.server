//package org.icatproject.ids.integration;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.fail;
//
//import java.net.URL;
//import java.nio.file.FileSystems;
//import java.nio.file.Files;
//import java.nio.file.Path;
//
//import javax.xml.namespace.QName;
//
//import org.icatproject.ICAT;
//import org.icatproject.ICATService;
//import org.icatproject.ids.integration.util.Setup;
//import org.icatproject.ids.integration.util.TestingClient;
//import org.icatproject.ids.integration.util.TreeDeleteVisitor;
//import org.icatproject.ids.webservice.Status;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import com.sun.jersey.api.client.UniformInterfaceException;
//
//public class GetStatusForPreparedIdTest {
//
//	private static Setup setup = null;
//	@SuppressWarnings("unused")
//	private static ICAT icat;
//	private TestingClient testingClient;
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
//			testingClient.getStatus("bad preparedId format");
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void nonExistingPreparedIdTest() throws Exception {
//		int expectedSc = 404;
//		try {
//			testingClient.getStatus("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void notFoundIdsTest() throws Exception {
//		int expectedSc = 404;
//		try {
//			String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null,
//					"1,2,3,99999", null, null);
//			Status status = null;
//			do {
//				Thread.sleep(1000);
//				status = testingClient.getStatus(preparedId);
//			} while (Status.RESTORING.equals(status));
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void notFoundSingleIdTest() throws Exception {
//		int expectedSc = 404;
//		try {
//			String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null,
//					"99999", null, null);
//			Status status = null;
//			do {
//				Thread.sleep(1000);
//				status = testingClient.getStatus(preparedId);
//			} while (Status.RESTORING.equals(status));
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void notFoundDatasetSingleIdTest() throws Exception {
//		int expectedSc = 404;
//		try {
//			String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, "99999",
//					null, null, null);
//			Status status = null;
//			do {
//				Thread.sleep(1000);
//				status = testingClient.getStatus(preparedId);
//			} while (Status.RESTORING.equals(status));
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
//					null, setup.getCommaSepDatafileIds(), null, null);
//			Status status = null;
//			do {
//				Thread.sleep(1000);
//				status = testingClient.getStatus(preparedId);
//			} while (Status.RESTORING.equals(status));
//			fail("Expected SC " + expectedSc);
//		} catch (UniformInterfaceException e) {
//			assertEquals(expectedSc, e.getResponse().getStatus());
//		}
//	}
//
//	@Test
//	public void correctBehaviourTest() throws Exception {
//		String preparedId = testingClient.prepareData(setup.getGoodSessionId(), null, null,
//				setup.getCommaSepDatafileIds(), null, null);
//		Status status = null;
//		do {
//			Thread.sleep(1000);
//			status = testingClient.getStatus(preparedId);
//		} while (Status.RESTORING.equals(status));
//		assertEquals(Status.ONLINE, status);
//	}
//
// }
