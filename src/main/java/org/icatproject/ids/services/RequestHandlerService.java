package org.icatproject.ids.services;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.requestHandlers.ArchiveHandler;
import org.icatproject.ids.requestHandlers.DeleteHandler;
import org.icatproject.ids.requestHandlers.GetDataFileIdsHandler;
import org.icatproject.ids.requestHandlers.GetDataHandler;
import org.icatproject.ids.requestHandlers.GetIcatUrlHandler;
import org.icatproject.ids.requestHandlers.GetServiceStatusHandler;
import org.icatproject.ids.requestHandlers.GetSizeHandler;
import org.icatproject.ids.requestHandlers.GetStatusHandler;
import org.icatproject.ids.requestHandlers.IsPreparedHandler;
import org.icatproject.ids.requestHandlers.IsReadOnlyHandler;
import org.icatproject.ids.requestHandlers.IsTwoLevelHandler;
import org.icatproject.ids.requestHandlers.PrepareDataHandler;
import org.icatproject.ids.requestHandlers.PutHandler;
import org.icatproject.ids.requestHandlers.RequestHandlerBase;
import org.icatproject.ids.requestHandlers.ResetHandler;
import org.icatproject.ids.requestHandlers.RestoreHandler;
import org.icatproject.ids.requestHandlers.WriteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.ejb.Stateless;

/**
 * This class encapsulates tha handling of all the different requests. In tries to find the right request handler and executes its handling method
 */
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


    /**
     * Use this mwthod to add your new handler to the internal handlers list, so that it can be called when it needs to be called.
     * @param requestHandler The handler which shall be added to the system.
     */
    private void registerHandler(RequestHandlerBase requestHandler) {

        //use only the handlers that supports the configured StorageUnit
        if( requestHandler.supportsStorageUnit(this.propertyHandler.getStorageUnit()) )
            if(this.handlers.containsKey(requestHandler.getRequestType())) {
                throw new RuntimeException("You tried to add a request handler, but it alreay exists a handler which handles the same RequestType.");
            }
            this.handlers.put(requestHandler.getRequestType(), requestHandler);
    }


    /**
     * Call this methis if you want to handle a request. It tries to find a request handler in the internal list which is able to deal with the given RequestType.
     * @param requestType The internal defintion of the RequestType.
     * @param parameters The parameters which where extracted from the request. Why not handing over the request itself here? Because in some cases additional parameters are needed.
     * @return A ValueContainer which is able to carry several types of retun values.
     * @throws InternalException
     * @throws BadRequestException
     * @throws InsufficientPrivilegesException
     * @throws NotFoundException
     * @throws DataNotOnlineException
     * @throws NotImplementedException
     */
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
                archiveStorage = propertyHandler.getArchiveStorage();
                twoLevel = archiveStorage != null;
                preparedDir = propertyHandler.getCacheDir().resolve("prepared");

                Files.createDirectories(preparedDir);

                if (!inited) {
                    key = propertyHandler.getKey();
                    logger.info("Key is " + (key == null ? "not set" : "set"));
                }

                this.unfinishedWorkService = new UnfinishedWorkService();

                if (twoLevel) {
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