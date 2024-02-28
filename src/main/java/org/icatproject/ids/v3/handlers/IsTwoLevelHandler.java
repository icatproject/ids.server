package org.icatproject.ids.v3.handlers;

import java.util.HashMap;

import org.icatproject.ids.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.v3.RequestHandlerBase;
import org.icatproject.ids.v3.ServiceProvider;
import org.icatproject.ids.v3.enums.CallType;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.models.ValueContainer;

public class IsTwoLevelHandler extends RequestHandlerBase {

    public IsTwoLevelHandler() {
        super(new StorageUnit[]{StorageUnit.DATAFILE, StorageUnit.DATASET, null}, RequestType.ISTWOLEVEL);
    }

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {
        
        var serviceProvider = ServiceProvider.getInstance();

        if (serviceProvider.getLogSet().contains(CallType.INFO)) {
            serviceProvider.getTransmitter().processMessage("isTwoLevel", parameters.get("ip").getString(), "{}", System.currentTimeMillis());
        }
        return new ValueContainer(twoLevel);
    }
}