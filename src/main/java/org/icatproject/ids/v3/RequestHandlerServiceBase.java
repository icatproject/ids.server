package org.icatproject.ids.v3;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.Executors;

import javax.xml.datatype.DatatypeFactory;

import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.IcatReader;
import org.icatproject.ids.LockManager;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.Transmitter;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.handlers.GetDataHandler;
import org.icatproject.ids.v3.handlers.RequestHandlerBase;
import org.icatproject.ids.v3.models.ValueContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestHandlerServiceBase {

    private HashMap<RequestType, RequestHandlerBase> handlers;
    private PropertyHandler propertyHandler;
    protected final static Logger logger = LoggerFactory.getLogger(RequestHandlerBase.class);
    private static Boolean inited = false;
    protected Path preparedDir;
    private static String key;
    private ArchiveStorageInterface archiveStorage;
    private boolean twoLevel;
    private Path datasetDir;
    private Path markerDir;
    private UnfinishedWorkServiceBase unfinishedWorkService;


    public RequestHandlerServiceBase() {

        this.propertyHandler = PropertyHandler.getInstance();
        this.unfinishedWorkService = new UnfinishedWorkServiceBase();

        this.handlers = new HashMap<RequestType, RequestHandlerBase>();
        this.registerHandler(RequestType.GETDATA, new GetDataHandler());      
    }

    private void registerHandler(RequestType requestType, RequestHandlerBase requestHandler) {

        //use only the handlers that supports the configured StorageUnit
        if( requestHandler.supportsStorageUnit(this.propertyHandler.getStorageUnit()) )
            this.handlers.put(requestType, requestHandler);
    }

    public ValueContainer handle(RequestType requestType, HashMap<String, ValueContainer> parameters) throws InternalException, BadRequestException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException {

        if(this.handlers.containsKey(requestType))
            return this.handlers.get(requestType).handle(parameters);
        else
            throw new InternalException("No handler found for RequestType " + requestType + " and StorageUnit " + this.propertyHandler.getStorageUnit() + " in RequestHandlerService. Do you forgot to register?");
    }

    public void init(Transmitter transmitter, LockManager lockManager , FiniteStateMachine fsm, IcatReader reader) {
        try {
            synchronized (inited) {
                logger.info("creating RequestHandlerService");
                ServiceProvider.createInstance(transmitter, fsm, lockManager, reader);
                propertyHandler = ServiceProvider.getInstance().getPropertyHandler();
                // zipMapper = propertyHandler.getZipMapper();
                // mainStorage = propertyHandler.getMainStorage();
                archiveStorage = propertyHandler.getArchiveStorage();
                twoLevel = archiveStorage != null;
                // datatypeFactory = DatatypeFactory.newInstance();
                preparedDir = propertyHandler.getCacheDir().resolve("prepared");
                Files.createDirectories(preparedDir);

                // rootUserNames = propertyHandler.getRootUserNames();
                // readOnly = propertyHandler.getReadOnly();
                // enableWrite = propertyHandler.getEnableWrite();

                // icat = propertyHandler.getIcatService();

                if (!inited) {
                    key = propertyHandler.getKey();
                    logger.info("Key is " + (key == null ? "not set" : "set"));
                }

                if (twoLevel) {
                    // storageUnit = propertyHandler.getStorageUnit();
                    datasetDir = propertyHandler.getCacheDir().resolve("dataset");
                    markerDir = propertyHandler.getCacheDir().resolve("marker");
                    if (!inited) {
                        Files.createDirectories(datasetDir);
                        Files.createDirectories(markerDir);
                        this.unfinishedWorkService.restartUnfinishedWork(markerDir, key);
                    }
                }

                if (!inited) {
                    UnfinishedWorkServiceBase.cleanPreparedDir(preparedDir);
                    if (twoLevel) {
                        UnfinishedWorkServiceBase.cleanDatasetCache(datasetDir);
                    }
                }

                // maxIdsInQuery = propertyHandler.getMaxIdsInQuery();

                // threadPool = Executors.newCachedThreadPool();

                // logSet = propertyHandler.getLogSet();

                for(RequestHandlerBase handler : this.handlers.values()) {
                    handler.init();
                }

                inited = true;

                logger.info("created RequestHandlerService");
            }
        } catch (Throwable e) {
            logger.error("Won't start ", e);
            throw new RuntimeException("RequestHandlerService reports " + e.getClass() + " " + e.getMessage());
        }
    }

}