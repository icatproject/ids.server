package org.icatproject.ids.v3;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.handlers.ArchiveHandler;
import org.icatproject.ids.v3.handlers.DeleteHandler;
import org.icatproject.ids.v3.handlers.GetDataFileIdsHandler;
import org.icatproject.ids.v3.handlers.GetDataHandler;
import org.icatproject.ids.v3.handlers.GetIcatUrlHandler;
import org.icatproject.ids.v3.handlers.GetServiceStatusHandler;
import org.icatproject.ids.v3.handlers.GetSizeHandler;
import org.icatproject.ids.v3.handlers.GetStatusHandler;
import org.icatproject.ids.v3.handlers.IsPreparedHandler;
import org.icatproject.ids.v3.handlers.IsReadOnlyHandler;
import org.icatproject.ids.v3.handlers.IsTwoLevelHandler;
import org.icatproject.ids.v3.handlers.PrepareDataHandler;
import org.icatproject.ids.v3.handlers.PutHandler;
import org.icatproject.ids.v3.handlers.ResetHandler;
import org.icatproject.ids.v3.handlers.RestoreHandler;
import org.icatproject.ids.v3.handlers.WriteHandler;
import org.icatproject.ids.v3.models.ValueContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.ejb.Stateless;

@Stateless
public class RequestHandlerService {

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
    private UnfinishedWorkService unfinishedWorkService;


    private void registerHandler(RequestHandlerBase requestHandler) {

        //use only the handlers that supports the configured StorageUnit
        if( requestHandler.supportsStorageUnit(this.propertyHandler.getStorageUnit()) )
            this.handlers.put(requestHandler.getRequestType(), requestHandler);
    }


    public ValueContainer handle(RequestType requestType, HashMap<String, ValueContainer> parameters) throws InternalException, BadRequestException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException {

        if(this.handlers.containsKey(requestType)) {
            return this.handlers.get(requestType).handle(parameters);
        }
        else
            throw new InternalException("No handler found for RequestType " + requestType + " and StorageUnit " + this.propertyHandler.getStorageUnit() + " in RequestHandlerService. Do you forgot to register?");
    }
    
    @PostConstruct
    private void init() {
        try {
            synchronized (inited) {
                logger.info("creating RequestHandlerService");
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

                this.unfinishedWorkService = new UnfinishedWorkService();

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
                    UnfinishedWorkService.cleanPreparedDir(preparedDir);
                    if (twoLevel) {
                        UnfinishedWorkService.cleanDatasetCache(datasetDir);
                    }
                }

                // maxIdsInQuery = propertyHandler.getMaxIdsInQuery();

                // threadPool = Executors.newCachedThreadPool();

                // logSet = propertyHandler.getLogSet();

                this.propertyHandler = PropertyHandler.getInstance();

                this.handlers = new HashMap<RequestType, RequestHandlerBase>();
                this.registerHandler(new GetDataHandler()); 
                this.registerHandler(new ArchiveHandler()); 
                this.registerHandler(new GetIcatUrlHandler());
                this.registerHandler(new GetDataFileIdsHandler());  
                this.registerHandler(new GetServiceStatusHandler());
                this.registerHandler(new GetSizeHandler());
                this.registerHandler(new GetStatusHandler());
                this.registerHandler(new IsPreparedHandler());
                this.registerHandler(new IsReadOnlyHandler());
                this.registerHandler(new IsTwoLevelHandler());
                this.registerHandler(new PrepareDataHandler());
                this.registerHandler(new PutHandler());
                this.registerHandler(new ResetHandler());
                this.registerHandler(new RestoreHandler());
                this.registerHandler(new WriteHandler());
                this.registerHandler(new DeleteHandler());

                logger.info("Initializing " + this.handlers.size() + " RequestHandlers...");
                for(RequestHandlerBase handler : this.handlers.values()) {
                    handler.init();
                }
                logger.info("RequestHandlers initialized");

                inited = true;

                logger.info("created RequestHandlerService");
            }
        } catch (Throwable e) {
            logger.error("Won't start ", e);
            throw new RuntimeException("RequestHandlerService reports " + e.getClass() + " " + e.getMessage());
        }
    }

}