package org.icatproject.ids.ids2;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.namespace.QName;

import org.apache.commons.io.FileUtils;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.Setup;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids.webservice.exceptions.InternalServerErrorException;
import org.icatproject.ids2.ported.thread.ProcessQueue;
import org.icatproject.idsclient.Status;
import org.icatproject.idsclient.TestingClient;
import org.icatproject.idsclient.exceptions.IDSException;
import org.junit.BeforeClass;
import org.junit.Test;

public class PrepareDataTest {
	
	private static Setup setup = null;
	private static ICAT icatClient;

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup();
        
    	try {
			final URL icatUrl = new URL(setup.getIcatUrl());
			final ICATService icatService = new ICATService(icatUrl, new QName(
					"http://icatproject.org", "ICATService"));
			icatClient = icatService.getICATPort();
		} catch (Exception e) {
			throw new InternalServerErrorException("Could not initialize ICAT client");
		}
    }
    
    @Test
    public void restoreArchivedDataset() throws Exception {
        TestingClient client = new TestingClient(setup.getIdsUrl());
        Dataset icatDs = (Dataset) icatClient.get(setup.getGoodSessionId(), "Dataset", Long.parseLong(setup.getDatasetIds().get(0)));
        File fileOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
        File zipOnFastStorage = new File(setup.getStorageZipDir(), icatDs.getLocation());
        FileUtils.deleteDirectory(fileOnFastStorage);
        FileUtils.deleteDirectory(zipOnFastStorage);
        
        String preparedId = client.prepareDataTest(setup.getGoodSessionId(), null, setup.getDatasetIds().get(0), null, null, null);
        Status status = null;
        do {
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            status = client.getStatus(preparedId);
        } while (Status.RESTORING.equals(status));
        
        assertEquals("Status info should be ONLINE, is " + status.name(), status, Status.ONLINE);
        assertTrue("File " + fileOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist", fileOnFastStorage.exists());
        assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath() + " should have been restored, but doesn't exist", zipOnFastStorage.exists());
    }

}
