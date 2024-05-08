package org.icatproject.ids.requestHandlers.base;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.Set;
import java.util.SortedMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.enums.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.services.ServiceProvider;
import org.icatproject.ids.services.dataSelectionService.DataSelectionService;
import org.icatproject.ids.services.dataSelectionService.DataSelectionServiceFactory;


/**
 * This base class represents all common properties and methods which are needed by each request handler.
 * Request handlers schould be added to the internal request handler list in RequestHandlerService, to be able to be called.
 */
public abstract class RequestHandlerBase {

    protected final static Logger logger = LoggerFactory.getLogger(RequestHandlerBase.class);
    protected Path preparedDir;
    protected boolean twoLevel;
    protected StorageUnit storageUnit;
    protected RequestType requestType;
    protected boolean readOnly;
    protected String ip;

    /**
     * matches standard UUID format of 8-4-4-4-12 hexadecimal digits
     */
    public static final Pattern uuidRegExp = Pattern
            .compile("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$");


    protected RequestHandlerBase(RequestType requestType, String ip ) {
        this.requestType = requestType;
        this.ip = ip;

        var serviceProvider = ServiceProvider.getInstance();
        var propertyHandler = serviceProvider.getPropertyHandler();
        this.preparedDir = propertyHandler.getCacheDir().resolve("prepared");
        this.storageUnit = propertyHandler.getStorageUnit();
        var archiveStorage = propertyHandler.getArchiveStorage();
        this.twoLevel = archiveStorage != null;
        this.readOnly = propertyHandler.getReadOnly();
    }


    /**
     * Informs about the RequestType the handler ist providing a handling for.
     * @return
     */
    public RequestType getRequestType() {
        return this.requestType;
    }


    /**
     * Creates a DataSelection depending on the RequestType. It Uses the DataSelectionFactory which is creating the DataSelection depending on the configured StorageUnit.
     * @param dsInfos A ready to use Map of DataSetInfos
     * @param dfInfos A ready to use Map of DataFileInfos
     * @param emptyDatasets A list of data set IDs of empty data sets
     * @return
     * @throws InternalException
     */
    public DataSelectionService getDataSelection(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos, Set<Long> emptyDatasets, long fileLength) throws InternalException {

        return DataSelectionServiceFactory.getService(dsInfos, dfInfos, emptyDatasets, fileLength, this.getRequestType());
    }


    /**
     * provides a suitable DataSelection depending on the RequestType. It Uses the DataSelectionFactory which is creating the DataSelection depending on the configured StorageUnit.
     * @param userSessionId The current session id
     * @param investigationIds A String which contains investigation IDs
     * @param datasetIds A String which contains data set IDs
     * @param datafileIds A String which contains data file IDs
     * @return
     * @throws InternalException
     * @throws BadRequestException
     * @throws NotFoundException
     * @throws InsufficientPrivilegesException
     * @throws NotImplementedException
     */
    public DataSelectionService getDataSelection(String userSessionId, String investigationIds, String datasetIds, String datafileIds) 
                                    throws InternalException, BadRequestException, NotFoundException, InsufficientPrivilegesException, NotImplementedException {

        return DataSelectionServiceFactory.get(userSessionId, investigationIds, datasetIds, datafileIds, this.getRequestType());
    }


    /**
     * The core method of each request handler. It has to be overwritten in the concrete implementation to provide an individual request handling
     * @return A ValueContainer with an indiviadual result type.
     * @throws BadRequestException
     * @throws InternalException
     * @throws InsufficientPrivilegesException
     * @throws NotFoundException
     * @throws DataNotOnlineException
     * @throws NotImplementedException
     */
    public ValueContainer handle() throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException {

        // some preprocessing
        long start = System.currentTimeMillis();
        logger.info("New webservice request: " + this.requestType.toString().toLowerCase() + " " + this.getRequestParametersLogString());

        // Do it
        ValueContainer result = this.handleRequest();

        // transmitting information about the current request
        if (ServiceProvider.getInstance().getLogSet().contains(this.getCallType())) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    this.addParametersToTransmitterJSON(gen);
                    gen.writeEnd();
                }
                String body = baos.toString();
                ServiceProvider.getInstance().getTransmitter().processMessage("archive", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }


        return result;

    }
    /**
     * Override to add additional parameters to the log output for the current request
     * @return
     */
    public String getRequestParametersLogString() { return ""; }

    /**
     * Override to add additional parameters to the transmitter JSON
     * @param gen
     * @throws IcatException_Exception
     * @throws BadRequestException
     */
    public void addParametersToTransmitterJSON(JsonGenerator gen) throws IcatException_Exception, BadRequestException {}

    /**
     * The core method of each request handler. It has to be overwritten in the concrete implementation to provide an individual request handling
     * @return A ValueContainer with an indiviadual result type.
     * @throws BadRequestException
     * @throws InternalException
     * @throws InsufficientPrivilegesException
     * @throws NotFoundException
     * @throws DataNotOnlineException
     * @throws NotImplementedException
     */
    public abstract ValueContainer handleRequest() throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException;

    public abstract CallType getCallType();

    

    

}
