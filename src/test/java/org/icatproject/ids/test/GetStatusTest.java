package org.icatproject.ids.test;

import java.io.File;
import java.net.URL;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.test.util.Setup;
import org.icatproject.ids.test.util.TestingClient;
import org.icatproject.ids.webservice.Status;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.UniformInterfaceException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GetStatusTest {

	private static Setup setup = null;
	@SuppressWarnings("unused")
	private static ICAT icat;
	private TestingClient testingClient;

	// value of the offset in bytes
	final Integer goodOffset = 20;
	final Integer badOffset = 99999999;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup();
		final URL icatUrl = new URL(setup.getIcatUrl());
		final ICATService icatService = new ICATService(icatUrl, new QName("http://icatproject.org", "ICATService"));
		icat = icatService.getICATPort();
	}

	@Before
	public void clearFastStorage() throws Exception {
		File storageDir = new File(setup.getStorageDir());
		File storageZipDir = new File(setup.getStorageZipDir());
		FileUtils.deleteDirectory(storageDir);
		FileUtils.deleteDirectory(storageZipDir);
		storageDir.mkdir();
		storageZipDir.mkdir();
		testingClient = new TestingClient(setup.getIdsUrl());
	}

	@Test
	public void badPreparedIdFormatTest() throws Exception {
		int expectedSc = 400;
		try {
			testingClient.getStatusTest("bad preparedId format");
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void nonExistingPreparedIdTest() throws Exception {
		int expectedSc = 404;
		try {
			testingClient.getStatusTest("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void notFoundIdsTest() throws Exception {
		int expectedSc = 404;
		try {
			String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null, "1,2,3,99999",
					null, null);
			Status status = null;
			do {
				Thread.sleep(1000);
				status = testingClient.getStatusTest(preparedId);
			} while (Status.RESTORING.equals(status));
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void notFoundSingleIdTest() throws Exception {
		int expectedSc = 404;
		try {
			String preparedId = testingClient
					.prepareDataTest(setup.getGoodSessionId(), null, null, "99999", null, null);
			Status status = null;
			do {
				Thread.sleep(1000);
				status = testingClient.getStatusTest(preparedId);
			} while (Status.RESTORING.equals(status));
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void notFoundDatasetSingleIdTest() throws Exception {
		int expectedSc = 404;
		try {
			String preparedId = testingClient
					.prepareDataTest(setup.getGoodSessionId(), null, "99999", null, null, null);
			Status status = null;
			do {
				Thread.sleep(1000);
				status = testingClient.getStatusTest(preparedId);
			} while (Status.RESTORING.equals(status));
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void forbiddenTest() throws Exception {
		int expectedSc = 403;
		try {
			String preparedId = testingClient.prepareDataTest(setup.getForbiddenSessionId(), null, null,
					setup.getCommaSepDatafileIds(), null, null);
			Status status = null;
			do {
				Thread.sleep(1000);
				status = testingClient.getStatusTest(preparedId);
			} while (Status.RESTORING.equals(status));
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void correctBehaviourTest() throws Exception {
		String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null,
				setup.getCommaSepDatafileIds(), null, null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatusTest(preparedId);
		} while (Status.RESTORING.equals(status));
		assertEquals(Status.ONLINE, status);
	}

}
