package org.icatproject.ids.v3;

import java.util.Set;

import org.icatproject.ICAT;
import org.icatproject.ids.IcatReader;
import org.icatproject.ids.LockManager;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.Transmitter;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.v3.FiniteStateMachine.FiniteStateMachine;
import org.icatproject.ids.v3.enums.CallType;

/**
 * This class serves the developer with multiple services.
 * Maybe it is just for the redesign for version 3 and will later be replaced with dependency injection, when it will be more clear where which service is used.
 */
public class ServiceProvider {

    private static ServiceProvider instance = null;

    private Transmitter transmitter;
    private FiniteStateMachine fsm;
    private LockManager lockManager;
    private IcatReader icatReader;

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

    public static ServiceProvider getInstance() {
        if(instance == null) {
            throw new RuntimeException("ServiceProvider is not yet instantiated, please call createInstance at first.");
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

    public Set<CallType> getLogSet() {
        return PropertyHandler.getInstance().getLogSet();
    }


}