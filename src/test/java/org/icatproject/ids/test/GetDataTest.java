package org.icatproject.ids.test;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.test.util.Response;
import org.icatproject.ids.test.util.Setup;
import org.icatproject.ids.test.util.TestingClient;
import org.icatproject.ids.webservice.Status;
import org.icatproject.ids.webservice.exceptions.ForbiddenException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.UniformInterfaceException;

/*
 * Test the getData method for the IDS. This is a bit more involved than the
 * tests performed on the other methods as it requires verifying that the files
 * downloaded are correct as well as the web service responses.
 * 
 * TODO: move offsets into test.properties?
 */
public class GetDataTest {
    
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
			testingClient.getDataTest("bad preparedId format", null, null);
			fail("Expected SC " + expectedSc);
		} catch (UniformInterfaceException e) {
			assertEquals(expectedSc, e.getResponse().getStatus());
		}
    }

    @Test
    public void badFileNameFormatTest() throws Exception {
    	int expectedSc = 400;
    	try {
	        String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null, setup.getDatafileIds().get(0),
	                null, null);
	
	        Status status = null;
	        do {
	        	Thread.sleep(1000);
	            status = testingClient.getStatusTest(preparedId);
	        } while (Status.RESTORING.equals(status));
	
	        testingClient.getDataTest(preparedId, "this/is/a/bad/file/name", null);
	        fail("Expected SC " + expectedSc);
    	} catch (UniformInterfaceException e) {
    		assertEquals(expectedSc, e.getResponse().getStatus());
    	}
    }

    @Test
    public void badOffsetFormatTest() throws Exception {
    	int expectedSc = 400;
    	try{
	        String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null, setup.getDatafileIds().get(0),
	                null, null);
	
	        Status status = null;
	        do {
	        	Thread.sleep(1000);
	            status = testingClient.getStatusTest(preparedId);
	        } while (Status.RESTORING.equals(status));
	
	        testingClient.getDataTest(preparedId, null, -10L);
	        fail("Expected SC " + expectedSc);
    	} catch (UniformInterfaceException e) {
    		assertEquals(expectedSc, e.getResponse().getStatus());
    	}
    }

    @Test
    public void nonExistantPreparedIdTest() throws Exception {
    	int expectedSc = 404;
    	try {
    		testingClient.getDataTest("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", null, null);
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
	                setup.getDatafileIds().get(0), null, null);
	
	        try {
	            Status status = null;
	            do {
	            	Thread.sleep(1000);
	                status = testingClient.getStatusTest(preparedId);
	            } while (Status.RESTORING.equals(status));
	        } catch (ForbiddenException e) {
	            // ignore because testing to see if getData throws a ForbiddenException as well
	        }
	
	        testingClient.getDataTest(preparedId, null, null);
	        fail("Expected SC " + expectedSc);
        } catch (UniformInterfaceException e) {
        	assertEquals(expectedSc, e.getResponse().getStatus());
        }
    }
    
    @Test
    public void offsetTooBigTest() throws Exception {
    	int expectedSc = 400;
    	try {
	        String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null, setup.getDatafileIds().get(0),
	                null, null);
	
	        Status status = null;
	        do {
	        	Thread.sleep(1000);
	            status = testingClient.getStatusTest(preparedId);
	        } while (Status.RESTORING.equals(status));
	
	        testingClient.getDataTest(preparedId, null, badOffset.longValue());
	        fail("Expected SC " + expectedSc);
    	} catch (UniformInterfaceException e) {
    		assertEquals(expectedSc, e.getResponse().getStatus());
    	}
    }

    @Test
    public void correctBehaviourNoOffsetTest() throws Exception {
        String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null, setup.getDatafileIds().get(0),
                null, null);

        Status status = null;
        do {
        	Thread.sleep(1000);
            status = testingClient.getStatusTest(preparedId);
        } while (Status.RESTORING.equals(status));

        Response response = testingClient.getDataTest(preparedId, null, null);
        Map<String, String> map = filenameMD5Map(response.getResponse());
        checkMD5Values(map);
    }

    @Test
    public void correctBehaviourNoOffsetMultipleDatafilesTest() throws Exception {
        String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null, setup.getCommaSepDatafileIds(),
                null, null);

        Status status = null;
        do {
        	Thread.sleep(1000);
            status = testingClient.getStatusTest(preparedId);
        } while (Status.RESTORING.equals(status));

        Response response = testingClient.getDataTest(preparedId, null, null);
        Map<String, String> map = filenameMD5Map(response.getResponse());
        checkMD5Values(map);
    }

    @Test
    public void correctBehaviourNoOffsetWithDatasetTest() throws Exception {
        String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, setup.getDatasetIds().get(0), null,
                null, null);

        Status status = null;
        do {
        	Thread.sleep(1000);
            status = testingClient.getStatusTest(preparedId);
        } while (Status.RESTORING.equals(status));

        Response response = testingClient.getDataTest(preparedId, null, null);
        Map<String, String> map = filenameMD5Map(response.getResponse());
        checkMD5Values(map);
    }

    @Test
    public void correctBehaviourNoOffsetWithDatasetAndDatafileTest() throws Exception {

        String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, setup.getDatasetIds().get(0),
                setup.getCommaSepDatafileIds(), null, null);

        Status status = null;
        do {
        	System.out.println("preparing...");
        	Thread.sleep(1000);
            status = testingClient.getStatusTest(preparedId);
        } while (Status.RESTORING.equals(status));

        Response response = testingClient.getDataTest(preparedId, null, null);
        Map<String, String> map = filenameMD5Map(response.getResponse());
        checkMD5Values(map);
    }

    @Test
    public void correctBehaviourWithOffsetTest() throws Exception {
        String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null, setup.getDatafileIds().get(0),
                null, null);

        Status status = null;
        do {
        	Thread.sleep(1000);
            status = testingClient.getStatusTest(preparedId);
        } while (Status.RESTORING.equals(status));

        // request the zip file twice, with and without an offset
        Response zip = testingClient.getDataTest(preparedId, null, null);
        Response zipoffset = testingClient.getDataTest(preparedId, null, goodOffset.longValue());

        // check that the full zip file is valid
        Map<String, String> map = filenameMD5Map(zip.getResponse());
        checkMD5Values(map);

        // compare the two zip files byte by byte taking into account the offset
        byte[] a = zip.getResponse().toByteArray();
        byte[] b = zipoffset.getResponse().toByteArray();
        for (int i = 0; i < b.length; i++) {
            Assert.assertEquals("Byte offset: " + i, (byte) b[i], (byte) a[i + goodOffset]);
        }
    }
    
    @Test
    public void correctBehaviourFilenameWithExtensionTest() throws Exception {
        String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null, setup.getDatafileIds().get(0),
                null, null);

        Status status = null;
        do {
        	Thread.sleep(1000);
            status = testingClient.getStatusTest(preparedId);
        } while (Status.RESTORING.equals(status));

        Response response = testingClient.getDataTest(preparedId, "testfilenamewithextension.zip", null);
        Assert.assertEquals("Downloaded filename does not match requested filename",
                response.getFilename(), "testfilenamewithextension.zip");
    }

    @Test
    public void correctBehaviourFilenameWithoutExtensionTest() throws Exception {
        String preparedId = testingClient.prepareDataTest(setup.getGoodSessionId(), null, null, setup.getDatafileIds().get(0),
                null, null);

        Status status = null;
        do {
        	Thread.sleep(1000);
            status = testingClient.getStatusTest(preparedId);
        } while (Status.RESTORING.equals(status));

        Response response = testingClient.getDataTest(preparedId, "testfilenamewithoutextension", null);
        Assert.assertEquals("Downloaded filename does not match requested filename",
                response.getFilename(), "testfilenamewithoutextension.zip");
    }

    /*
     * Takes in a outputstream of a zip file. Creates a mapping between the filenames and their MD5
     * sums.
     */
    private Map<String, String> filenameMD5Map(ByteArrayOutputStream file) throws IOException,
            NoSuchAlgorithmException {
        Map<String, String> filenameMD5map = new HashMap<String, String>();
        ZipInputStream zis = null;
        ByteArrayOutputStream os = null;
        ZipEntry entry = null;
        try {
            zis = new ZipInputStream(new ByteArrayInputStream(file.toByteArray()));
            while ((entry = zis.getNextEntry()) != null) {
                os = new ByteArrayOutputStream();
                int len;
                byte[] buffer = new byte[1024];
                while ((len = zis.read(buffer)) != -1) { // zis.read will read up to last byte of the entry
                    os.write(buffer, 0, len);
                }
                
                MessageDigest m = MessageDigest.getInstance("MD5");
                m.reset();
                m.update(os.toByteArray());
                String md5sum = new BigInteger(1, m.digest()).toString(16);
                while(md5sum.length() < 32 ){
                    md5sum = "0" + md5sum;
                }
                filenameMD5map.put(entry.getName(), md5sum);
            }
        } finally {
            if (zis != null) {
                zis.close();
            }
            if (os != null) {
                os.close();
            }
        }
        return filenameMD5map;
    }

    private void checkMD5Values(Map<String, String> map) {
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> pairs = (Map.Entry<String, String>) it.next();
            String correctMD5 = setup.getFilenameMD5().get(pairs.getKey());
            if (correctMD5 == null) {
                Assert.fail("Cannot find MD5 sum for filename '" + pairs.getKey() + "'");
            }
            
            Assert.assertEquals("Stored MD5 sum for " + pairs.getKey()
                    + " does not match downloaded file", correctMD5, pairs.getValue());
            it.remove();
        }
    }
}