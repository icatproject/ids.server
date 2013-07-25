package org.icatproject.ids;

import java.io.IOException;
import java.net.MalformedURLException;

import junit.framework.Assert;

import org.icatproject.IcatException_Exception;
import org.icatproject.idsclient.Status;
import org.icatproject.idsclient.TestingClient;
import org.icatproject.idsclient.exceptions.BadRequestException;
import org.icatproject.idsclient.exceptions.ForbiddenException;
import org.icatproject.idsclient.exceptions.IDSException;
import org.icatproject.idsclient.exceptions.NotFoundException;
import org.junit.BeforeClass;
import org.junit.Test;


/*
 * Test the getStatus method for the IDS.
 */
public class GetStatusTest {

    private static Setup setup = null;

    @BeforeClass
    public static void setup() throws MalformedURLException, IcatException_Exception {
        setup = new Setup();
    }

    @Test(expected = BadRequestException.class)
    public void badPreparedIdFormatTest() throws IOException, IDSException {
        TestingClient client = new TestingClient(setup.getidsURL());
        client.getStatusTest("bad preparedId format");
    }

    @Test(expected = NotFoundException.class)
    public void nonExistingPreparedIdTest() throws IOException, IDSException {
        TestingClient client = new TestingClient(setup.getidsURL());
        client.getStatus("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    }

    @Test(expected = NotFoundException.class)
    public void notFoundIdsTest() throws IOException, IDSException {
        TestingClient client = new TestingClient(setup.getidsURL());
        String preparedId = client.prepareDataTest(setup.getGoodSessionId(), null, null, "1,2,3,99999", null, null);
        Status status = null;
        do {
            status = client.getStatus(preparedId);
        } while (Status.RESTORING.equals(status));
    }
    
    @Test(expected = NotFoundException.class)
    public void notFoundSingleIdTest() throws IOException, IDSException {
        TestingClient client = new TestingClient(setup.getidsURL());
        String preparedId = client.prepareDataTest(setup.getGoodSessionId(), null, null, "99999", null, null);
        Status status = null;
        do {
            status = client.getStatus(preparedId);
        } while (Status.RESTORING.equals(status));
    }
    
    @Test(expected = NotFoundException.class)
    public void notFoundDatasetSingleIdTest() throws IOException, IDSException {
        TestingClient client = new TestingClient(setup.getidsURL());
        String preparedId = client.prepareDataTest(setup.getGoodSessionId(), null, "99999", null, null, null);
        Status status = null;
        do {
            status = client.getStatus(preparedId);
        } while (Status.RESTORING.equals(status));
    }

    @Test(expected = ForbiddenException.class)
    public void forbiddenTest() throws IOException, IDSException {
        TestingClient client = new TestingClient(setup.getidsURL());
        String preparedId = client.prepareDataTest(setup.getForbiddenSessionId(), null, null, setup.getCommaSepDatafileIds(),
                null, null);
        Status status = null;
        do {
            status = client.getStatus(preparedId);
        } while (Status.RESTORING.equals(status));
    }

    @Test
    public void correctBehaviourTest() throws IOException, IDSException {
        TestingClient client = new TestingClient(setup.getidsURL());
        String preparedId = client.prepareDataTest(setup.getGoodSessionId(), null, null, setup.getCommaSepDatafileIds(), null, null);
        Status status = null;
        do {
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.out.println("sleep interrupted");
				
			}
            status = client.getStatus(preparedId);
        } while (Status.RESTORING.equals(status));
        Assert.assertEquals(Status.ONLINE, status);
    }
}
