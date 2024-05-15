package org.icatproject.ids;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;
import org.icatproject.icat.client.IcatException;
import org.icatproject.ids.enums.StorageUnit;
import org.icatproject.ids.finiteStateMachine.FiniteStateMachine;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.requestHandlers.ArchiveHandler;
import org.icatproject.ids.services.IcatReader;
import org.icatproject.ids.services.LockManager;
import org.icatproject.ids.services.PropertyHandler;
import org.icatproject.ids.services.ServiceProvider;
import org.icatproject.ids.services.Transmitter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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

        var handler = new ArchiveHandler("192.168.17.1", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "1, 2, 3", "4, 5, 6", "7, 8, 9");
        String body = handler.provideTransmissionBody();
        assertEquals("{\"userName\":\"TestUser\",\"investigationIds\":[1,2,3],\"datasetIds\":[4,5,6],\"datafileIds\":[7,8,9]}", body);
    }
}