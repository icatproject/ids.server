package org.icatproject.ids.requestHandlers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.icatproject.Datafile;
import org.icatproject.EntityBaseBean;
import org.icatproject.IcatExceptionType;
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
import org.icatproject.ids.plugin.AlreadyLockedException;
import org.icatproject.ids.requestHandlers.base.DataRequestHandler;
import org.icatproject.ids.services.LockManager.Lock;
import org.icatproject.ids.services.LockManager.LockType;
import org.icatproject.ids.services.ServiceProvider;
import org.icatproject.ids.services.dataSelectionService.DataSelectionService;

public class DeleteHandler extends DataRequestHandler {

    public DeleteHandler(String ip, String sessionId, String investigationIds,
            String datasetIds, String datafileIds) {
        super(RequestType.DELETE, ip, sessionId, investigationIds, datasetIds,
                datafileIds);
    }

    @Override
    public ValueContainer handleDataRequest(
            DataSelectionService dataSelectionService)
            throws BadRequestException, InternalException,
            InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {

        if (readOnly) {
            throw new NotImplementedException(
                    "This operation has been configured to be unavailable");
        }

        var serviceProvider = ServiceProvider.getInstance();

        // Do it
        try (Lock lock = serviceProvider.getLockManager().lock(
                dataSelectionService.getDsInfo().values(),
                LockType.EXCLUSIVE)) {
            if (storageUnit == StorageUnit.DATASET) {
                dataSelectionService.checkOnline();
            }

            /* Now delete from ICAT */
            List<EntityBaseBean> dfs = new ArrayList<>();
            for (DataInfoBase dfInfo : dataSelectionService.getDfInfo()
                    .values()) {
                Datafile df = new Datafile();
                df.setId(dfInfo.getId());
                dfs.add(df);
            }
            try {
                serviceProvider.getIcat()
                        .deleteMany(this.dataController.getOperationId(), dfs);
            } catch (IcatException_Exception e) {
                IcatExceptionType type = e.getFaultInfo().getType();

                if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES
                        || type == IcatExceptionType.SESSION) {
                    throw new InsufficientPrivilegesException(e.getMessage());
                }
                if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
                    throw new NotFoundException(e.getMessage());
                }
                throw new InternalException(type + " " + e.getMessage());
            }

            dataSelectionService.delete();

        } catch (AlreadyLockedException e) {
            logger.debug("Could not acquire lock, delete failed");
            throw new DataNotOnlineException("Data is busy");
        } catch (IOException e) {
            logger.error("I/O error " + e.getMessage());
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        return ValueContainer.getVoid();
    }

    @Override
    public CallType getCallType() {
        return CallType.WRITE;
    }
}
