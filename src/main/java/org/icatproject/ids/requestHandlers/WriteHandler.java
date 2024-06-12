package org.icatproject.ids.requestHandlers;

import java.io.IOException;
import java.util.Map;

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
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.plugin.AlreadyLockedException;
import org.icatproject.ids.requestHandlers.base.DataRequestHandler;
import org.icatproject.ids.services.ServiceProvider;
import org.icatproject.ids.services.LockManager.Lock;
import org.icatproject.ids.services.LockManager.LockType;
import org.icatproject.ids.services.dataSelectionService.DataSelectionService;

public class WriteHandler extends DataRequestHandler {

    public WriteHandler(String ip, String sessionId, String investigationIds, String datasetIds, String datafileIds) {
        super(RequestType.WRITE, ip, sessionId, investigationIds, datasetIds, datafileIds);
    }

    @Override
    public ValueContainer handleDataRequest(DataSelectionService dataSelectionService)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {

        var serviceProvider = ServiceProvider.getInstance();

        if (!serviceProvider.getPropertyHandler().getEnableWrite()) {
            throw new NotImplementedException("This operation has been configured to be unavailable");
        }

        Map<Long, DataInfoBase> dsInfos = dataSelectionService.getDsInfo();

        try (Lock lock = serviceProvider.getLockManager().lock(dsInfos.values(), LockType.SHARED)) {
            if (twoLevel) {
                boolean maybeOffline = false;
                for (DataInfoBase dataInfo : dataSelectionService.getPrimaryDataInfos().values()) {
                    if (!dataSelectionService.existsInMainStorage(dataInfo)) {
                        maybeOffline = true;
                    }
                }
                if (maybeOffline) {
                    throw new DataNotOnlineException("Requested data is not online, write request refused");
                }
            }

            dataSelectionService.scheduleTasks(DeferredOp.WRITE);

        } catch (AlreadyLockedException e) {
            logger.debug("Could not acquire lock, write failed");
            throw new DataNotOnlineException("Data is busy");
        } catch (IOException e) {
            logger.error("I/O error " + e.getMessage() + " writing");
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        return ValueContainer.getVoid();
    }

    @Override
    public CallType getCallType() {
        return CallType.MIGRATE;
    }
}
