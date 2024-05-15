package org.icatproject.ids.requestHandlers;

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

public class IsTwoLevelHandler extends RequestHandlerBase {

    public IsTwoLevelHandler(String ip) {
        super(RequestType.ISTWOLEVEL, ip);
    }

    @Override
    public ValueContainer handleRequest()
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {
        
        return new ValueContainer(twoLevel);
    }

    @Override
    public CallType getCallType() {
        return CallType.INFO;
    }
}
