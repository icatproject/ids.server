package org.icatproject.ids.requestHandlers.restHandlers;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.OperationIdTypes;
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
import org.icatproject.ids.requestHandlers.RequestHandlerBase;
import org.icatproject.ids.services.ServiceProvider;


public abstract class ResetHandler extends RequestHandlerBase {

    public ResetHandler(OperationIdTypes operationIdType) {
        super(operationIdType, RequestType.RESET);
    }

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {

        long start = System.currentTimeMillis();
        var serviceProvider = ServiceProvider.getInstance();

        DataSelectionBase dataSelection = this.provideDataSelection(parameters);

        this.recordSuccess(dataSelection, serviceProvider.getFsm());

        if (serviceProvider.getLogSet().contains(CallType.MIGRATE)) {
            try {
                ByteArrayOutputStream baos = this.generateStreamForTransmitter(parameters);
                String body = baos.toString();
                serviceProvider.getTransmitter().processMessage("reset", parameters.get("ip").getString(), body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
            
        }

        return ValueContainer.getVoid();
    }

    protected abstract DataSelectionBase provideDataSelection(HashMap<String, ValueContainer> parameters) throws InternalException, BadRequestException, NotFoundException, InsufficientPrivilegesException, NotImplementedException;

    protected abstract ByteArrayOutputStream generateStreamForTransmitter(HashMap<String, ValueContainer> parameters) throws InternalException, IcatException_Exception, BadRequestException;
    

    private void recordSuccess(DataSelectionBase dataSelection, FiniteStateMachine fsm) {
        for (DataInfoBase dataInfo : dataSelection.getPrimaryDataInfos().values()) {
            fsm.recordSuccess(dataInfo.getId());
        }
    }

    
}