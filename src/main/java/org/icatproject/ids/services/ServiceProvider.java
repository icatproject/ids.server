package org.icatproject.ids.services;

import java.util.Set;

import org.icatproject.ICAT;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.finiteStateMachine.FiniteStateMachine;
import org.icatproject.ids.plugin.MainStorageInterface;

/**
 * This class provides multiple services to the developer Maybe it is just for
 * the redesign for version 3 and will later be replaced with dependency
 * injection, when it will be more clear where which service is used.
 */
public class ServiceProvider {

    private static ServiceProvider instance = null;

    private Transmitter transmitter;
    private FiniteStateMachine fsm;
    private LockManager lockManager;
    private IcatReader icatReader;
    private PropertyHandler propertyHandler;

    private ServiceProvider(PropertyHandler propertyHandler,
            Transmitter transmitter, FiniteStateMachine fsm,
            LockManager lockManager, IcatReader reader) {
        this.transmitter = transmitter;
        this.fsm = fsm;
        this.lockManager = lockManager;
        this.icatReader = reader;
        this.propertyHandler = propertyHandler;
    }

    /**
     * At first, the ServiceProvider has to be created. Do not call
     * getInstance() before you have called createInstande()
     *
     * @param transmitter
     * @param fsm
     * @param lockManager
     * @param reader
     */
    public static void createInstance(Transmitter transmitter,
            FiniteStateMachine fsm, LockManager lockManager,
            IcatReader reader) {

        createInstance(PropertyHandler.getInstance(), transmitter, fsm,
                lockManager, reader);
    }

    public static void createInstance(PropertyHandler propertyHandler,
            Transmitter transmitter, FiniteStateMachine fsm,
            LockManager lockManager, IcatReader reader) {

        if (instance != null)
            return;

        instance = new ServiceProvider(propertyHandler, transmitter, fsm,
                lockManager, reader);
    }

    public static ServiceProvider getInstance() {
        if (instance == null) {
            throw new RuntimeException(
                    "ServiceProvider is not yet instantiated, please call createInstance at first.");
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
        return this.propertyHandler;
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
