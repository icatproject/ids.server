package org.icatproject.ids.requestHandlers.getStatusHandlers;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Set;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.OperationIdTypes;
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
import org.icatproject.ids.requestHandlers.RequestHandlerBase;
import org.icatproject.ids.services.ServiceProvider;

public abstract class GetStatusHandler extends RequestHandlerBase {

    public GetStatusHandler(OperationIdTypes[] operationIdTypes) {
        super(operationIdTypes, RequestType.GETSTATUS );
    }

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {

        long start = System.currentTimeMillis();
        var serviceProvider = ServiceProvider.getInstance();

        // Do it
        var dataSelection = this.provideDataSelection(parameters);

        Status status = this.getStatus(dataSelection);

        logger.debug("Status is " + status.name());

        if (serviceProvider.getLogSet().contains(CallType.INFO)) {
            try {
                ByteArrayOutputStream baos = this.generateStreamForTransmitter(parameters);
                String body = baos.toString();
                serviceProvider.getTransmitter().processMessage("getStatus", parameters.get("ip").getString(), body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return new ValueContainer(status.name());
    }

    protected abstract DataSelectionBase provideDataSelection(HashMap<String, ValueContainer> parameters) throws InternalException, BadRequestException, NotFoundException, InsufficientPrivilegesException, NotImplementedException;

    protected abstract ByteArrayOutputStream generateStreamForTransmitter(HashMap<String, ValueContainer> parameters) throws InternalException, BadRequestException, IcatException_Exception;


    private Status getStatus(DataSelectionBase dataSelection) throws InternalException {
        Status status = Status.ONLINE;
        var serviceProvider = ServiceProvider.getInstance();

        Set<DataInfoBase> restoring = serviceProvider.getFsm().getRestoring();
        Set<DataInfoBase> maybeOffline = serviceProvider.getFsm().getMaybeOffline();
        for (DataInfoBase dataInfo : dataSelection.getPrimaryDataInfos().values()) {
            serviceProvider.getFsm().checkFailure(dataInfo.getId());
            if (restoring.contains(dataInfo)) {
                status = Status.RESTORING;
            } else if (maybeOffline.contains(dataInfo)) {
                status = Status.ARCHIVED;
                break;
            } else if (!dataSelection.existsInMainStorage(dataInfo)) {
                status = Status.ARCHIVED;
                break;
            }
        }

        return status;
    }
}