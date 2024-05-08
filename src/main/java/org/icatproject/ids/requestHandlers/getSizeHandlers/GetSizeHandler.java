package org.icatproject.ids.requestHandlers.getSizeHandlers;

import java.util.Map;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.RequestType;
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

public class GetSizeHandler extends DataRequestHandler {

    public GetSizeHandler(String ip, String preparedId, String sessionId, String investigationIds, String datasetIds, String datafileIds) {
        super(RequestType.GETSIZE, ip, preparedId, sessionId, investigationIds, datasetIds, datafileIds);
    }

    @Override
    public ValueContainer handleDataRequest(DataSelectionBase dataSelection)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {
        
        logger.debug("Slow computation for normal case");

        long size = 0;
        size = this.updateSizeFromDataInfoIds(size, dataSelection.getDfInfo(), this.dataController.forceGetSessionId());

        return new ValueContainer(size);
    }


    protected long updateSizeFromDataInfoIds(long size, Map<Long, DataInfoBase> dataInfos, String sessionId) throws InternalException {
        StringBuilder sb = new StringBuilder();
        int n = 0;
        for (DataInfoBase dataInfo : dataInfos.values()) {
            if (sb.length() != 0) {
                sb.append(',');
            }
            sb.append(dataInfo.getId());
            if (n++ == 500) {
                size += getSizeFor(sessionId, sb);
                sb = new StringBuilder();
                n = 0;
            }
        }
        if (n > 0) {
            size += getSizeFor(sessionId, sb);
        }

        return size;
    }


    private long getSizeFor(String sessionId, StringBuilder sb) throws InternalException {
        String query = "SELECT SUM(df.fileSize) from Datafile df WHERE df.id IN (" + sb.toString() + ") AND df.location IS NOT NULL";
        try {
            return (Long) ServiceProvider.getInstance().getIcat().search(sessionId, query).get(0);
        } catch (IcatException_Exception e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        } catch (IndexOutOfBoundsException e) {
            return 0L;
        }
    }

    @Override
    public CallType getCallType() {
        return CallType.INFO;
    }
    
}
