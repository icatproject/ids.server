package org.icatproject.ids.requestHandlers.base;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.dataSelection.DataSelectionFactory;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.services.ServiceProvider;

import jakarta.json.stream.JsonGenerator;

public class UnpreparedDataController extends DataControllerBase {

    public String sessionId;
    public String investigationIds;
    public String datasetIds;
    public String datafileIds;

    public UnpreparedDataController(String sessionId, String investigationIds, String datasetIds, String datafileIds) {
        this.sessionId = sessionId;
        this.investigationIds = investigationIds;
        this.datasetIds = datasetIds;
        this.datafileIds = datafileIds;
    }

    @Override
    public void validateUUID() throws BadRequestException {
        validateUUID("sessionId", sessionId);
    }

    @Override
    public String getRequestParametersLogString() {
        return "investigationIds='" + investigationIds + "' " + "datasetIds='"
        + datasetIds + "' " + "datafileIds='" + datafileIds + "'";
    }

    @Override
    public DataSelectionBase provideDataSelection(RequestType requestType) throws InternalException, BadRequestException, NotFoundException, InsufficientPrivilegesException, NotImplementedException {
        return DataSelectionFactory.get(sessionId, investigationIds, datasetIds, datafileIds, requestType);
    }

    @Override
    public void addParametersToTransmitterJSON(JsonGenerator gen) throws IcatException_Exception, BadRequestException {
        gen.write("userName", ServiceProvider.getInstance().getIcat().getUserName(sessionId));
        addIds(gen, investigationIds, datasetIds, datafileIds);
    }



    protected void addIds(JsonGenerator gen, String investigationIds, String datasetIds, String datafileIds)
            throws BadRequestException {
        if (investigationIds != null) {
            gen.writeStartArray("investigationIds");
            for (long invid : DataSelectionBase.getValidIds("investigationIds", investigationIds)) {
                gen.write(invid);
            }
            gen.writeEnd();
        }
        if (datasetIds != null) {
            gen.writeStartArray("datasetIds");
            for (long invid : DataSelectionBase.getValidIds("datasetIds", datasetIds)) {
                gen.write(invid);
            }
            gen.writeEnd();
        }
        if (datafileIds != null) {
            gen.writeStartArray("datafileIds");
            for (long invid : DataSelectionBase.getValidIds("datafileIds", datafileIds)) {
                gen.write(invid);
            }
            gen.writeEnd();
        }
    }

    @Override
    public boolean mustZip(boolean zip, DataSelectionBase dataSelection) {
        return zip ? true : dataSelection.mustZip();
    }

    @Override
    public String getOperationId() {
        return this.sessionId;
    }

    @Override
    public String forceGetSessionId() throws InternalException {
        if (this.sessionId == null) {
            this.sessionId = this.createSessionId();
        }
        
        return this.sessionId;
    }
}
