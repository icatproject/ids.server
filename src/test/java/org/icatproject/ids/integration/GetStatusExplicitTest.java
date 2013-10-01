package org.icatproject.ids.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.TestingClient;
import org.icatproject.ids.webservice.Status;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.UniformInterfaceException;

public class GetStatusExplicitTest {

	private static Setup setup = null;
	@SuppressWarnings("unused")
	private static ICAT icat;
	private TestingClient testingClient;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup();
		final URL icatUrl = new URL(setup.getIcatUrl());
		final ICATService icatService = new ICATService(icatUrl, new QName(
				"http://icatproject.org", "ICATService"));
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
	public void notFoundDatasetIdTest() throws Exception {
		int expectedSc = 404;
		try {
			testingClient.getStatusTest(setup.getGoodSessionId(), null, "99999999", null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void notFoundDatafileIdsTest() throws Exception {
		int expectedSc = 404;
		try {
			testingClient.getStatusTest(setup.getGoodSessionId(), null, null, "1,2,3,9999999");
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
	}

	@Test
	public void forbiddenTest() throws Exception {
		int expectedSc = 403;
		try {
			testingClient.getStatusTest(setup.getForbiddenSessionId(), null, null,
					setup.getCommaSepDatafileIds());
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
		status = testingClient.getStatusTest(setup.getGoodSessionId(), null, null,
				setup.getCommaSepDatafileIds());
		assertEquals(Status.ONLINE, status);
	}

	@Test
	public void restoringDatafileRestoresItsDatasetTest() throws Exception {
		String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null,
				setup.getDatafileIds().get(0), null, null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatusTest(preparedId);
		} while (Status.RESTORING.equals(status));
		assertEquals(Status.ONLINE, status);
		status = testingClient.getStatusTest(setup.getGoodSessionId(), null, setup.getDatasetIds()
				.get(0), null);
		assertEquals(Status.ONLINE, status);
		status = testingClient.getStatusTest(setup.getGoodSessionId(), null, setup.getDatasetIds()
				.get(1), null);
		assertEquals(Status.ARCHIVED, status);
	}

}
