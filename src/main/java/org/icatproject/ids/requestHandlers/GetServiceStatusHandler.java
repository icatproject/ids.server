package org.icatproject.ids.requestHandlers;

import java.util.Set;

import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.requestHandlers.base.RequestHandlerBase;
import org.icatproject.ids.services.ServiceProvider;

import jakarta.json.stream.JsonGenerator;

public class GetServiceStatusHandler extends RequestHandlerBase {

    private Set<String> rootUserNames;
    private String sessionId;
    private ServiceProvider serviceProvider;

    public GetServiceStatusHandler(String ip, String sessionId) {
        super(RequestType.GETSERVICESTATUS, ip);

        this.sessionId = sessionId;
        rootUserNames = ServiceProvider.getInstance().getPropertyHandler()
                .getRootUserNames();
        this.serviceProvider = ServiceProvider.getInstance();
    }

    @Override
    public ValueContainer handleRequest() throws BadRequestException,
            InternalException, InsufficientPrivilegesException,
            NotFoundException, DataNotOnlineException, NotImplementedException {

        try {
            String uname = serviceProvider.getIcat().getUserName(sessionId);
            if (!rootUserNames.contains(uname)) {
                throw new InsufficientPrivilegesException(uname
                        + " is not included in the ids rootUserNames set.");
            }
        } catch (IcatException_Exception e) {
            IcatExceptionType type = e.getFaultInfo().getType();
            if (type == IcatExceptionType.SESSION) {
                throw new InsufficientPrivilegesException(
                        e.getClass() + " " + e.getMessage());
            }
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        return new ValueContainer(serviceProvider.getFsm().getServiceStatus());
    }

    @Override
    public void addParametersToTransmitterJSON(JsonGenerator gen)
            throws IcatException_Exception, BadRequestException {
        gen.write("userName", serviceProvider.getIcat().getUserName(sessionId));
    }

    @Override
    public CallType getCallType() {
        return CallType.INFO;
    }

}
