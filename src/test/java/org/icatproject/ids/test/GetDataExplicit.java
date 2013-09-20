package org.icatproject.ids.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URL;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.test.util.Response;
import org.icatproject.ids.test.util.Setup;
import org.icatproject.ids.test.util.TestingClient;
import org.icatproject.ids.test.util.TestingUtils;
import org.icatproject.ids.webservice.Status;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.UniformInterfaceException;

public class GetDataExplicit {
	
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
			testingClient.getDataTest("bad preparedId format", null, null, null, null, null, null, null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
    }
	
	@Test
    public void badDatafileIdFormatTest() throws Exception {
    	int expectedSc = 400;
        try {
			testingClient.getDataTest(setup.getGoodSessionId(), null, null, "notADatafile", null, null, null, null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
    }
	
	@Test
	public void forbiddenTest() throws Exception {
		int expectedSc = 403;
		try {
			testingClient.getDataTest(setup.getForbiddenSessionId(), null, null, setup.getCommaSepDatafileIds(), null, null, null, null);
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
		Response response = testingClient.getDataTest(setup.getGoodSessionId(), null, null, setup.getCommaSepDatafileIds(), null, null, null, null);
		Map<String, String> map = TestingUtils.filenameMD5Map(response.getResponse());
        TestingUtils.checkMD5Values(map, setup);
	}

}
