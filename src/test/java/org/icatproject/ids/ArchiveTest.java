package org.icatproject.ids;

import java.io.IOException;
import java.net.MalformedURLException;

import org.icatproject.IcatException_Exception;
import org.icatproject.idsclient.TestingClient;
import org.icatproject.idsclient.exceptions.BadRequestException;
import org.icatproject.idsclient.exceptions.ForbiddenException;
import org.icatproject.idsclient.exceptions.IDSException;
import org.icatproject.idsclient.exceptions.NotImplementedException;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * Test the archive method for the IDS. NOTE: these tests are specific to
 * the IDS and may not be valid for the correct implementation of the IDS
 * specification.
 */
public class ArchiveTest {

    private static Setup setup = null;

    @BeforeClass
    public static void setup() throws MalformedURLException, IcatException_Exception {
        setup = new Setup();
    }

    @Test(expected = NotImplementedException.class)
    public void notImplementedInvestigationIdsTest() throws IOException, IDSException {
        TestingClient client = new TestingClient(setup.getidsURL());
        client.archiveTest(setup.getGoodSessionId(), "1,2", null, null);
    }

    @Test(expected = BadRequestException.class)
    public void badSessionIdFormatTest() throws IOException, IDSException {
        TestingClient client = new TestingClient(setup.getidsURL());
        client.archiveTest("bad sessionId format", null, null, "1,2");
    }

    @Test(expected = BadRequestException.class)
    public void badDatafileIdFormatTest() throws IOException, IDSException {
        TestingClient client = new TestingClient(setup.getidsURL());
        client.archiveTest(setup.getGoodSessionId(), null, null, "1,2,a");
    }

    @Test(expected = BadRequestException.class)
    public void badDatasetIdFormatTest() throws IOException, IDSException {
        TestingClient client = new TestingClient(setup.getidsURL());
        client.archiveTest(setup.getGoodSessionId(), null, "", null);
    }

    @Test(expected = BadRequestException.class)
    public void noIdsTest() throws IOException, IDSException {
        TestingClient client = new TestingClient(setup.getidsURL());       
        client.archiveTest(setup.getGoodSessionId(), null, null, null);
    }

    @Test(expected = ForbiddenException.class)
    public void nonExistingSessionIdTest() throws IOException, IDSException {
        TestingClient client = new TestingClient(setup.getidsURL());
        client.archiveTest("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", null, "1,2", null);
    }
}