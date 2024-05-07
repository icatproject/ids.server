package org.icatproject.ids.requestHandlers;

import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.finiteStateMachine.FiniteStateMachine;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.requestHandlers.base.DataRequestHandler;
import org.icatproject.ids.services.ServiceProvider;


public class ResetHandler extends DataRequestHandler {

    public ResetHandler(String ip, String preparedId) {
        super(RequestType.RESET, ip, preparedId);
    }

    public ResetHandler(String ip, String sessionId, String investigationIds, String datasetIds, String datafileIds) {
        super(RequestType.RESET, ip, sessionId, investigationIds, datasetIds, datafileIds);
    }

    @Override
    public ValueContainer handleDataRequest(DataSelectionBase dataSelection)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {

        FiniteStateMachine fsm = ServiceProvider.getInstance().getFsm();
        for (DataInfoBase dataInfo : dataSelection.getPrimaryDataInfos().values()) {
            fsm.recordSuccess(dataInfo.getId());
        }

        return ValueContainer.getVoid();
    }

    @Override
    public CallType getCallType() {
        return CallType.MIGRATE;
    }
    

    
}