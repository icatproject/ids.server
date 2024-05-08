package org.icatproject.ids.services;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import org.icatproject.ids.enums.OperationIdTypes;
import org.icatproject.ids.enums.RequestIdNames;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.requestHandlers.base.RequestHandlerBase;
import org.icatproject.ids.requestHandlers.getSizeHandlers.GetSizeHandlerForPreparedData;
import org.icatproject.ids.requestHandlers.getSizeHandlers.GetSizeHandlerForUnpreparedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.ejb.Stateless;

/**
 * This class encapsulates tha handling of all the different requests. In tries to find the right request handler and executes its handling method
 */
@Stateless
public class RequestHandlerService {

    /**
     * For each possible OperationIdTypes we have a HashMap which associates a RequestType to a request handler
     */
    private HashMap<OperationIdTypes, HashMap<RequestType, RequestHandlerBase> > handlers;


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

    private final Object lock = new Object();


    /**
     * Use this mwthod to add your new handler to the internal handlers list, so that it can be called when it needs to be called.
     * @param requestHandler The handler which shall be added to the system.
     */
    private void registerHandler(RequestHandlerBase requestHandler) {

        for(OperationIdTypes supportedOperationIdType : requestHandler.getSupportedOperationIdTypes())
        {
            if(this.handlers.get(supportedOperationIdType).containsKey(requestHandler.getRequestType())) {
                throw new RuntimeException("You tried to add a request handler, but it alreay exists a handler which handles the same RequestType " + requestHandler.getRequestType() + " for the supported OperationIdType " + supportedOperationIdType + ".");
            }
            this.handlers.get(supportedOperationIdType).put(requestHandler.getRequestType(), requestHandler);
        }
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
        
        // determine the OperationIdTypes of this request
        var operationIdType = OperationIdTypes.ANONYMOUS;
        if(parameters.containsKey(RequestIdNames.sessionId) && !parameters.get(RequestIdNames.sessionId).isNull()) operationIdType = OperationIdTypes.SESSIONID;
        else if(parameters.containsKey(RequestIdNames.preparedId) && !parameters.get(RequestIdNames.preparedId).isNull()) operationIdType = OperationIdTypes.PREPAREDID;

        // handle
        if(this.handlers.get(operationIdType).containsKey(requestType)) {
            return this.handlers.get(operationIdType).get(requestType).handle(parameters);
        }
        else {
            throw new NotFoundException("No handler found for RequestType " + requestType + " and OperationIdType " + operationIdType + " in RequestHandlerService. Do you forgot to register?");
        }
    }
    
    @PostConstruct
    private void init() {
        try {
            synchronized (lock) {
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

                this.handlers = new HashMap<OperationIdTypes, HashMap<RequestType, RequestHandlerBase> >();
                for(var operationIdType : OperationIdTypes.values()) {
                    this.handlers.put(operationIdType, new HashMap<RequestType, RequestHandlerBase>());
                }

                this.registerHandler(new GetSizeHandlerForPreparedData());
                this.registerHandler(new GetSizeHandlerForUnpreparedData());

                logger.info("Initializing " + this.handlers.size() + " RequestHandlers...");
                for(var handlerMap : this.handlers.values()) {
                    for(var handler : handlerMap.values()) {
                        handler.init();
                    }
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