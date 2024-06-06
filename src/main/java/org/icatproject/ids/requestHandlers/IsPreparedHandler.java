package org.icatproject.ids.requestHandlers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.requestHandlers.base.DataRequestHandler;
import org.icatproject.ids.services.dataSelectionService.DataSelectionService;

public class IsPreparedHandler extends DataRequestHandler {

    public IsPreparedHandler(String ip, String preparedId) {
        super(RequestType.ISPREPARED, ip, preparedId);
    }

    class PreparedStatus {

        public ReentrantLock lock = new ReentrantLock();
        public Long fromElement;
        public Future<?> future;
    }

    private Map<String, PreparedStatus> preparedStatusMap = new ConcurrentHashMap<>();

    @Override
    public ValueContainer handleDataRequest(
        DataSelectionService dataSelectionService
    )
        throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException {
        // Do it
        boolean prepared = true;

        PreparedStatus status = preparedStatusMap.computeIfAbsent(
            this.dataController.getOperationId(),
            k -> new PreparedStatus()
        );

        if (!status.lock.tryLock()) {
            logger.debug(
                "Lock held for evaluation of isPrepared for preparedId {}",
                this.dataController.getOperationId()
            );
            return new ValueContainer(false);
        }
        try {
            Future<?> future = status.future;
            if (future != null) {
                if (future.isDone()) {
                    try {
                        future.get();
                    } catch (ExecutionException e) {
                        throw new InternalException(
                            e.getClass() + " " + e.getMessage()
                        );
                    } catch (InterruptedException e) {
                        // Ignore
                    } finally {
                        status.future = null;
                    }
                } else {
                    logger.debug(
                        "Background process still running for preparedId {}",
                        this.dataController.getOperationId()
                    );
                    return new ValueContainer(false);
                }
            }

            prepared =
                dataSelectionService.isPrepared(
                    this.dataController.getOperationId()
                );

            return new ValueContainer(prepared);
        } finally {
            status.lock.unlock();
        }
    }

    @Override
    public CallType getCallType() {
        return CallType.INFO;
    }
}
