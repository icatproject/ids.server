package org.icatproject.ids.requestHandlers.base;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;

import jakarta.json.stream.JsonGenerator;


public abstract class DataRequestHandler extends RequestHandlerBase2 {

    protected DataControllerBase dataController;

    
    protected DataRequestHandler(RequestType requestType, String ip, String sessionId, String investigationIds, String datasetIds, String datafileIds) {
        super(requestType, ip);

        this.dataController = new UnpreparedDataController(sessionId, investigationIds, datasetIds, datafileIds);
    }

    protected DataRequestHandler(RequestType requestType, String ip, String preparedId) {
        super(requestType, ip);

        this.dataController = new PreparedDataController(preparedId);
    }

    protected DataRequestHandler(RequestType requestType, String ip, String preparedId, String sessionId, String investigationIds, String datasetIds, String datafileIds) {
        super(requestType, ip);

        if(sessionId != null) {
            this.dataController = new UnpreparedDataController(sessionId, investigationIds, datasetIds, datafileIds);
        } else {
            this.dataController = new PreparedDataController(preparedId);
        }

    }

    @Override
    public ValueContainer handleRequest() throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException {

        this.dataController.validateUUID();

        DataSelectionBase dataSelection = this.dataController.provideDataSelection(this.requestType);
        ValueContainer result = this.handleDataRequest(dataSelection);

        return result;

    }

    @Override
    public void addParametersToTransmitterJSON(JsonGenerator gen) throws IcatException_Exception, BadRequestException {
        this.dataController.addParametersToTransmitterJSON(gen);
        this.addCustomParametersToTransmitterJSON(gen);
    }

    public abstract ValueContainer handleDataRequest(DataSelectionBase dataSelection) throws NotImplementedException, InternalException, BadRequestException, NotFoundException, InsufficientPrivilegesException, DataNotOnlineException;

    public String getRequestParametersLogString() {
        return this.dataController.getRequestParametersLogString() + " " + this.getCustomRequestParametersLogString();
    }

    /**
     * Override this method in your concrete DataRequestHandler to add custom parameters to the JSON which will be transmitted.
     * @param gen
     */
    public void addCustomParametersToTransmitterJSON(JsonGenerator gen) {}

    /**
     * Override this method in your concrete DataRequestHandler to add custom parameters to the log output.
     */
    public String getCustomRequestParametersLogString() { return "";}
}
