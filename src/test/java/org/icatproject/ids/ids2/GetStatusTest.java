package org.icatproject.ids.ids2;

import java.io.File;
import java.net.URL;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.util.Setup;
import org.icatproject.ids.util.TestingClient;
import org.icatproject.ids.webservice.Status;
import org.icatproject.idsclient.exception.TestingClientBadRequestException;
import org.icatproject.idsclient.exception.TestingClientForbiddenException;
import org.icatproject.idsclient.exception.TestingClientNotFoundException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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

	@Test(expected = TestingClientBadRequestException.class)
	public void badPreparedIdFormatTest() throws Exception {
		testingClient.getStatusTest("bad preparedId format");
	}

	@Test(expected = TestingClientNotFoundException.class)
	public void nonExistingPreparedIdTest() throws Exception {
		testingClient.getStatusTest("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
	}

	@Test(expected = TestingClientNotFoundException.class)
	public void notFoundIdsTest() throws Exception {
		String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null, "1,2,3,99999", null,
				null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatusTest(preparedId);
		} while (Status.RESTORING.equals(status));
	}

	@Test(expected = TestingClientNotFoundException.class)
	public void notFoundSingleIdTest() throws Exception {
		String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null, "99999", null, null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatusTest(preparedId);
		} while (Status.RESTORING.equals(status));
	}

	@Test(expected = TestingClientNotFoundException.class)
	public void notFoundDatasetSingleIdTest() throws Exception {
		String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, "99999", null, null, null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatusTest(preparedId);
		} while (Status.RESTORING.equals(status));
	}

	@Test(expected = TestingClientForbiddenException.class)
	public void forbiddenTest() throws Exception {
		String preparedId = testingClient.prepareDataTest(setup.getForbiddenSessionId(), null, null,
				setup.getCommaSepDatafileIds(), null, null);
		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatusTest(preparedId);
		} while (Status.RESTORING.equals(status));
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
