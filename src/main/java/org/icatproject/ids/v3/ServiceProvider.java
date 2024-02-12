package org.icatproject.ids.v3;

import org.icatproject.ICAT;
import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.IcatReader;
import org.icatproject.ids.LockManager;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.Transmitter;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.plugin.MainStorageInterface;

public class ServiceProvider {

    private static ServiceProvider instance = null;

    private Transmitter transmitter;
    private FiniteStateMachine fsm;
    private LockManager lockManager;
    private IcatReader icatReader;
    private ICAT icat;

    private ServiceProvider(Transmitter transmitter, FiniteStateMachine fsm, LockManager lockManager, IcatReader reader) {
        this.transmitter = transmitter;
        this.fsm = fsm;
        this.lockManager = lockManager;
        this.icatReader = reader;
    }

    public static void createInstance(Transmitter transmitter, FiniteStateMachine fsm, LockManager lockManager, IcatReader reader) {

        if(instance != null) return;

        instance = new ServiceProvider(transmitter, fsm, lockManager, reader);
    }

    public static ServiceProvider getInstance() throws InternalException {
        if(instance == null) {
            throw new InternalException("ServiceProvider is not yet instantiated, please call createInstance at first.");
        }
        return instance;
    }

    public Transmitter getTransmitter() {
        return transmitter;
    }

    public FiniteStateMachine getFsm() {
        return fsm;
    }

    public LockManager getLockManager() {
        return lockManager;
    }

    public IcatReader getIcatReader() {
        return icatReader;
    }

    public PropertyHandler getPropertyHandler() {
        return PropertyHandler.getInstance();
    }

    public MainStorageInterface getMainStorage() {
        return this.getPropertyHandler().getMainStorage();
    }

    public ICAT getIcat() {
        return this.getPropertyHandler().getIcatService();
    }


}