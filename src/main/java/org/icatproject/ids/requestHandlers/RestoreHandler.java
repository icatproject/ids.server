package org.icatproject.ids.requestHandlers;

import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.requestHandlers.base.DataRequestHandler;

public class RestoreHandler extends DataRequestHandler {

    public RestoreHandler(String ip, String sessionId, String investigationIds, String datasetIds, String datafileIds) {
        super(RequestType.RESTORE, ip, sessionId, investigationIds, datasetIds, datafileIds);
    }

    @Override
    public ValueContainer handleDataRequest(DataSelectionBase dataSelection)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {

        dataSelection.scheduleTasks(DeferredOp.RESTORE);
        return ValueContainer.getVoid();
    }

    @Override
    public CallType getCallType() {
        return CallType.MIGRATE;
    }
}
