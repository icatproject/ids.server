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

public class IsReadOnlyHandler extends RequestHandlerBase {
    
    public IsReadOnlyHandler(String ip) {
        super(RequestType.ISREADONLY, ip);

    }

    @Override
    public ValueContainer handleRequest()
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {

        return new ValueContainer(this.readOnly);
    }

    @Override
    public CallType getCallType() {
        return CallType.INFO;
    }
}
