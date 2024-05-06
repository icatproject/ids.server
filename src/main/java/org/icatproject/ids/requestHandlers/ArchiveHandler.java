package org.icatproject.ids.requestHandlers;


import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.requestHandlers.base.DataRequestHandler;

public class ArchiveHandler extends DataRequestHandler {

    public ArchiveHandler(String ip, String sessionId, String investigationIds, String datasetIds, String datafileIds) {
        super(RequestType.ARCHIVE, ip, sessionId, investigationIds, datasetIds, datafileIds);
    }

    @Override
    public ValueContainer handleDataRequest(DataSelectionBase dataSelection) throws NotImplementedException, InternalException {
        
        dataSelection.scheduleTasks(DeferredOp.ARCHIVE);
        return ValueContainer.getVoid();
    }

    @Override
    public CallType getCallType() {
        return CallType.MIGRATE;
    }

}