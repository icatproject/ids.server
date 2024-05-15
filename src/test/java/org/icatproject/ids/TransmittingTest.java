package org.icatproject.ids;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;
import org.icatproject.icat.client.IcatException;
import org.icatproject.ids.enums.StorageUnit;
import org.icatproject.ids.finiteStateMachine.FiniteStateMachine;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.requestHandlers.ArchiveHandler;
import org.icatproject.ids.requestHandlers.DeleteHandler;
import org.icatproject.ids.requestHandlers.GetDataFileIdsHandler;
import org.icatproject.ids.requestHandlers.GetDataHandler;
import org.icatproject.ids.requestHandlers.GetIcatUrlHandler;
import org.icatproject.ids.requestHandlers.GetServiceStatusHandler;
import org.icatproject.ids.requestHandlers.GetStatusHandler;
import org.icatproject.ids.requestHandlers.IsPreparedHandler;
import org.icatproject.ids.requestHandlers.IsReadOnlyHandler;
import org.icatproject.ids.requestHandlers.IsTwoLevelHandler;
import org.icatproject.ids.requestHandlers.PrepareDataHandler;
import org.icatproject.ids.requestHandlers.PutHandler;
import org.icatproject.ids.requestHandlers.ResetHandler;
import org.icatproject.ids.requestHandlers.RestoreHandler;
import org.icatproject.ids.requestHandlers.WriteHandler;
import org.icatproject.ids.requestHandlers.getSizeHandlers.GetSizeHandler;
import org.icatproject.ids.requestHandlers.getSizeHandlers.GetSizeHandlerForFastProcessing;
import org.icatproject.ids.services.IcatReader;
import org.icatproject.ids.services.LockManager;
import org.icatproject.ids.services.PropertyHandler;
import org.icatproject.ids.services.ServiceProvider;
import org.icatproject.ids.services.Transmitter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


/**
 * This test was created when the building of the transmission body was made more comon and generic. 
 * It should ensure the right structure of the containing json.
 */
@RunWith(MockitoJUnitRunner.class)
public class TransmittingTest {

    @Mock
    private PropertyHandler mockedPropertyHandler;
    @Mock
    private Transmitter mockedTransmitter;
    @Mock
    private FiniteStateMachine mockedFsm;
    @Mock
    private LockManager mockedLockManager;
    @Mock
    private IcatReader mockedReader;
    @Mock ArchiveStorageInterface mockedArchiveStorage;
    @Mock
    private ICAT mockedIcat;

    private String ip = "192.168.17.1";
    private String sessionId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    private String preparedId = sessionId;
    private String investigationIds = "1, 2, 3";
    private String datasetIds = "4, 5, 6";
    private String datafileIds = "7, 8, 9";
    private String defaultTransmissionBodyForPreparedId = "{\"preparedId\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\"}";
    private String defaultTransmissionBodyForSessionId = "{\"userName\":\"TestUser\",\"investigationIds\":[1,2,3],\"datasetIds\":[4,5,6],\"datafileIds\":[7,8,9]}";

    private void setup()
            throws URISyntaxException, IcatException_Exception, IcatException {

        when(mockedPropertyHandler.getIcatService()).thenReturn(mockedIcat);
        when(mockedPropertyHandler.getCacheDir()).thenReturn(Paths.get(""));
        when(mockedPropertyHandler.getStorageUnit()).thenReturn(StorageUnit.DATASET);
        when(mockedPropertyHandler.getArchiveStorage()).thenReturn(mockedArchiveStorage);
        when(mockedPropertyHandler.getReadOnly()).thenReturn(false);
        when(mockedIcat.getUserName("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")).thenReturn("TestUser");

        ServiceProvider.createInstance(mockedPropertyHandler, mockedTransmitter, mockedFsm, mockedLockManager, mockedReader);
    }

    @Test
    public void testTransmitterBodyForArchiveRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new ArchiveHandler(ip, sessionId, investigationIds, datasetIds, datafileIds);
        String body = handler.provideTransmissionBody();
        assertEquals(defaultTransmissionBodyForSessionId, body);
    }

    @Test
    public void testTransmitterBodyForGetSizeRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new GetSizeHandler(ip, preparedId, null, null, null, null);
        String body = handler.provideTransmissionBody();
        assertEquals(defaultTransmissionBodyForPreparedId, body);

        handler = new GetSizeHandler(ip, null, sessionId, investigationIds, datasetIds, datafileIds);
        body = handler.provideTransmissionBody();
        assertEquals(defaultTransmissionBodyForSessionId, body);
    }

    @Test
    public void testTransmitterBodyForGetSizeFastRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new GetSizeHandlerForFastProcessing(ip, sessionId, investigationIds, datasetIds, datafileIds);
        String body = handler.provideTransmissionBody();
        assertEquals(defaultTransmissionBodyForSessionId, body);

    }

    @Test
    public void testTransmitterBodyForDeleteRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new DeleteHandler(ip, sessionId, investigationIds, datasetIds, datafileIds);
        String body = handler.provideTransmissionBody();
        assertEquals(defaultTransmissionBodyForSessionId, body);
    }

    @Test
    public void testTransmitterBodyForGetDataFileIdsRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new GetDataFileIdsHandler(ip, preparedId, null, null, null, null);
        String body = handler.provideTransmissionBody();
        assertEquals(defaultTransmissionBodyForPreparedId, body);

        handler = new GetDataFileIdsHandler("192.168.17.1", null,  sessionId, investigationIds, datasetIds, datafileIds);
        body = handler.provideTransmissionBody();
        assertEquals(defaultTransmissionBodyForSessionId, body);
    }

    @Test
    public void testTransmitterBodyForGetDataRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new GetDataHandler(ip, preparedId, null, null, null, null, false, false, "", "");
        String body = handler.provideTransmissionBody();
        assertEquals("{\"preparedId\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\",\"transferId\":-1}", body);

        handler = new GetDataHandler(ip, null,  sessionId, investigationIds, datasetIds, datafileIds, false, false, "", "");
        body = handler.provideTransmissionBody();
        assertEquals("{\"userName\":\"TestUser\",\"investigationIds\":[1,2,3],\"datasetIds\":[4,5,6],\"datafileIds\":[7,8,9],\"transferId\":-1}", body);
    }

    @Test
    public void testTransmitterBodyForGetIcatUrlRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new GetIcatUrlHandler(ip);
        String body = handler.provideTransmissionBody();
        assertEquals("{}", body);
    }

    @Test
    public void testTransmitterBodyForGetServiceStatusRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new GetServiceStatusHandler(ip, sessionId);
        String body = handler.provideTransmissionBody();
        assertEquals("{\"userName\":\"TestUser\"}", body);
    }

    @Test
    public void testTransmitterBodyForGetStatusRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new GetStatusHandler(ip, preparedId, null, null, null, null);
        String body = handler.provideTransmissionBody();
        assertEquals(defaultTransmissionBodyForPreparedId, body);

        handler = new GetStatusHandler(ip, null, sessionId, investigationIds, datasetIds, datafileIds);
        body = handler.provideTransmissionBody();
        assertEquals(defaultTransmissionBodyForSessionId, body);
    }

    @Test
    public void testTransmitterBodyForIsPreparedRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new IsPreparedHandler(ip, preparedId);
        String body = handler.provideTransmissionBody();
        assertEquals("{\"preparedId\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\"}", body);
    }

    @Test
    public void testTransmitterBodyForIsReadOnlyRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new IsReadOnlyHandler(ip);
        String body = handler.provideTransmissionBody();
        assertEquals("{}", body);
    }

    @Test
    public void testTransmitterBodyForIsTwoLevelRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new IsTwoLevelHandler(ip);
        String body = handler.provideTransmissionBody();
        assertEquals("{}", body);
    }

    @Test
    public void testTransmitterBodyForPrepareDataRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new PrepareDataHandler(ip, sessionId, investigationIds, datasetIds, datafileIds, false, false);
        String body = handler.provideTransmissionBody();
        assertEquals("{\"userName\":\"TestUser\",\"investigationIds\":[1,2,3],\"datasetIds\":[4,5,6],\"datafileIds\":[7,8,9],\"preparedId\":\"\"}", body);

    }

    @Test
    public void testTransmitterBodyForPutRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new PutHandler(ip, sessionId, new ByteArrayInputStream( "".getBytes() ), "someName", datafileIds , datasetIds, "someDescription", "someDOI", "simeCreateTimeString", "someModTimeString", false, false );
        String body = handler.provideTransmissionBody();
        assertEquals("{\"userName\":\"TestUser\",\"datafileId\":-1}", body);
    }

    @Test
    public void testTransmitterBodyForResetRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new ResetHandler(ip, preparedId, null, null, null, null);
        String body = handler.provideTransmissionBody();
        assertEquals(defaultTransmissionBodyForPreparedId, body);

        handler = new ResetHandler(ip, null, sessionId, investigationIds, datasetIds, datafileIds);
        body = handler.provideTransmissionBody();
        assertEquals(defaultTransmissionBodyForSessionId, body);
    }

    @Test
    public void testTransmitterBodyForRestoreRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new RestoreHandler(ip, sessionId, investigationIds, datasetIds, datafileIds);
        String body = handler.provideTransmissionBody();
        assertEquals(defaultTransmissionBodyForSessionId, body);
    }

    @Test
    public void testTransmitterBodyForWriteRequest_shouldBeOk() throws Exception {
        
        setup();

        var handler = new WriteHandler(ip, sessionId, investigationIds, datasetIds, datafileIds);
        String body = handler.provideTransmissionBody();
        assertEquals(defaultTransmissionBodyForSessionId, body);
    }
}