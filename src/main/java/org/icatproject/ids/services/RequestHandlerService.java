package org.icatproject.ids.services;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
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
import org.icatproject.ids.requestHandlers.ArchiveHandler;
import org.icatproject.ids.requestHandlers.DeleteHandler;
import org.icatproject.ids.requestHandlers.GetIcatUrlHandler;
import org.icatproject.ids.requestHandlers.GetServiceStatusHandler;
import org.icatproject.ids.requestHandlers.IsPreparedHandler;
import org.icatproject.ids.requestHandlers.IsReadOnlyHandler;
import org.icatproject.ids.requestHandlers.IsTwoLevelHandler;
import org.icatproject.ids.requestHandlers.PrepareDataHandler;
import org.icatproject.ids.requestHandlers.PutHandler;
import org.icatproject.ids.requestHandlers.RequestHandlerBase;
import org.icatproject.ids.requestHandlers.RestoreHandler;
import org.icatproject.ids.requestHandlers.WriteHandler;
import org.icatproject.ids.requestHandlers.getDataFileIdsHandlers.GetDataFileIdsHandlerForPreparedData;
import org.icatproject.ids.requestHandlers.getDataFileIdsHandlers.GetDataFileIdsHandlerForUnpreparedData;
import org.icatproject.ids.requestHandlers.getDataHandlers.GetDataHandlerForPreparedData;
import org.icatproject.ids.requestHandlers.getDataHandlers.GetDataHandlerForUnpreparedData;
import org.icatproject.ids.requestHandlers.getSizeHandlers.GetSizeHandlerForPreparedData;
import org.icatproject.ids.requestHandlers.getSizeHandlers.GetSizeHandlerForUnpreparedData;
import org.icatproject.ids.requestHandlers.getStatusHandlers.GetStatusHandlerForPreparedData;
import org.icatproject.ids.requestHandlers.getStatusHandlers.GetStatusHandlerForUnpreparedData;
import org.icatproject.ids.requestHandlers.resetHandlers.ResetHandlerForPreparedData;
import org.icatproject.ids.requestHandlers.resetHandlers.ResetHandlerForUnpreparedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.ejb.Stateless;
import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

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
        if(parameters.containsKey(RequestIdNames.sessionId) && !parameters.get(RequestIdNames.sessionId).isNull()) 
            operationIdType = OperationIdTypes.SESSIONID;
        else if(parameters.containsKey(RequestIdNames.preparedId) && !parameters.get(RequestIdNames.preparedId).isNull()) 
            operationIdType = OperationIdTypes.PREPAREDID;


        if(this.handlers.get(operationIdType).containsKey(requestType)) {

            RequestHandlerBase handler = this.handlers.get(operationIdType).get(requestType);

            //pre processing
            long start = System.currentTimeMillis();
            logger.info("New webservice request: " + requestType + " ... ");
            validateUUID(operationIdType, parameters);
            //not yet realized: providing DataSelection


            //handle
            ValueContainer result = handler.handle(parameters);


            //post processing
            this.transmit(start, handler.getCallType(), parameters);

            return result;

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

                this.registerHandler(new GetDataHandlerForPreparedData()); 
                this.registerHandler(new GetDataHandlerForUnpreparedData());

                this.registerHandler(new GetDataFileIdsHandlerForPreparedData());
                this.registerHandler(new GetDataFileIdsHandlerForUnpreparedData());

                this.registerHandler(new GetSizeHandlerForPreparedData());
                this.registerHandler(new GetSizeHandlerForUnpreparedData());

                this.registerHandler(new GetStatusHandlerForPreparedData());
                this.registerHandler(new GetStatusHandlerForUnpreparedData());

                this.registerHandler(new ResetHandlerForPreparedData());
                this.registerHandler(new ResetHandlerForUnpreparedData());

                this.registerHandler(new ArchiveHandler()); 
                this.registerHandler(new GetIcatUrlHandler()); 
                this.registerHandler(new GetServiceStatusHandler());
                this.registerHandler(new IsPreparedHandler());
                this.registerHandler(new IsReadOnlyHandler());
                this.registerHandler(new IsTwoLevelHandler());
                this.registerHandler(new PrepareDataHandler());
                this.registerHandler(new PutHandler());
                this.registerHandler(new RestoreHandler());
                this.registerHandler(new WriteHandler());
                this.registerHandler(new DeleteHandler());

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

    private void transmit(long startTime, CallType callType, HashMap<String, ValueContainer> parameters) throws BadRequestException, InternalException {

        if (ServiceProvider.getInstance().getLogSet().contains(callType)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {

                    for(var parameterKey : parameters.keySet())
                    {
                        if(parameterKey.equals(RequestIdNames.sessionId)) {
                            gen.write("userName", ServiceProvider.getInstance().getIcat().getUserName(parameters.get(parameterKey).getString()));
                        }
                        else if (parameterKey.equals("investigationIds") || parameterKey.equals("datasetIds") || parameterKey.equals("datafileIds")){
                            gen.writeStartArray(parameterKey);
                            for (long invid : DataSelectionBase.getValidIds(parameterKey, parameters.get(parameterKey).getString())) {
                                gen.write(invid);
                            }
                            gen.writeEnd();
                        }
                        else {
                            gen.write(parameterKey, parameters.get(parameterKey).toString()); 
                        }
                    }

                    gen.writeEnd();
                }
                String body = baos.toString();
                ServiceProvider.getInstance().getTransmitter().processMessage("archive", parameters.get("ip").getString(), body, startTime);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }
    }

}