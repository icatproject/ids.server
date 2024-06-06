package org.icatproject.ids.requestHandlers.base;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
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
import org.icatproject.ids.services.ServiceProvider;


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
        logger.info("New webservice request: " + this.requestType.toString().toLowerCase() + " " + this.addParametersToLogString());

        // Do it
        ValueContainer result = this.handleRequest();

        // transmitting information about the current request
        this.transmit(start);

        return result;

    }


    private void transmit(long start) throws BadRequestException {
        if (ServiceProvider.getInstance().getLogSet().contains(this.getCallType())) {
            try {
                String body = this.provideTransmissionBody();
                ServiceProvider.getInstance().getTransmitter().processMessage("archive", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }
    }

    public String provideTransmissionBody() throws BadRequestException, IcatException_Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
            this.addParametersToTransmitterJSON(gen);
            gen.writeEnd();
        }
        return baos.toString();
    }


    /**
     * Override to add additional parameters to the log output for the current request
     * @return
     */
    protected String addParametersToLogString() { return ""; }

    /**
     * Override to add additional parameters to the transmitter JSON
     * @param gen
     * @throws IcatException_Exception
     * @throws BadRequestException
     */
    protected void addParametersToTransmitterJSON(JsonGenerator gen) throws IcatException_Exception, BadRequestException {}

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
    protected abstract ValueContainer handleRequest() throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException;

    /**
     * each handler should provide its own CallType which is needed to create the Transmitter message
     * @return the Calltype of the request
     */
    protected abstract CallType getCallType();

}
