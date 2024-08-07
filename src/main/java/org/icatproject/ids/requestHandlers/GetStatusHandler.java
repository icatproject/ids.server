package org.icatproject.ids.requestHandlers;

import java.util.Set;

import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.enums.Status;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.requestHandlers.base.DataRequestHandler;
import org.icatproject.ids.services.ServiceProvider;
import org.icatproject.ids.services.dataSelectionService.DataSelectionService;

public class GetStatusHandler extends DataRequestHandler {

    public GetStatusHandler(String ip, String preparedId, String sessionId, String investigationIds, String datasetIds, String datafileIds) {
        super(RequestType.GETSTATUS, ip, preparedId, sessionId, investigationIds, datasetIds, datafileIds);
    }

    @Override
    public ValueContainer handleDataRequest(DataSelectionService dataSelectionService)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {

        Status status = Status.ONLINE;
        var serviceProvider = ServiceProvider.getInstance();

        Set<DataInfoBase> restoring = serviceProvider.getFsm().getRestoring();
        Set<DataInfoBase> maybeOffline = serviceProvider.getFsm().getMaybeOffline();
        for (DataInfoBase dataInfo : dataSelectionService.getPrimaryDataInfos().values()) {
            serviceProvider.getFsm().checkFailure(dataInfo.getId());
            if (restoring.contains(dataInfo)) {
                status = Status.RESTORING;
            } else if (maybeOffline.contains(dataInfo)) {
                status = Status.ARCHIVED;
                break;
            } else if (!dataSelectionService.existsInMainStorage(dataInfo)) {
                status = Status.ARCHIVED;
                break;
            }
        }

        logger.debug("Status is " + status.name());

        return new ValueContainer(status.name());
    }

    @Override
    public CallType getCallType() {
        return CallType.INFO;
    }
}
