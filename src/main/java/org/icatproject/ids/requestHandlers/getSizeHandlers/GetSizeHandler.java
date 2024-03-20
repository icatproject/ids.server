package org.icatproject.ids.requestHandlers.getSizeHandlers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.enums.PreparedDataStatus;
import org.icatproject.ids.enums.RequestType;
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

public abstract class GetSizeHandler extends RequestHandlerBase {

    public GetSizeHandler(PreparedDataStatus dataStatus) {
        super(dataStatus, RequestType.GETSIZE);
    }

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {
        
        return new ValueContainer(this.getSize(parameters));
    }

    public abstract long getSize(HashMap<String, ValueContainer> parameters) throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException, NotImplementedException;


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


    protected long getSizeFor(String sessionId, List<Long> ids, String where) throws InternalException {

        long size = 0;
        if (ids != null) {

            StringBuilder sb = new StringBuilder();
            int n = 0;
            for (Long id : ids) {
                if (sb.length() != 0) {
                    sb.append(',');
                }
                sb.append(id);
                if (n++ == 500) {
                    size += evalSizeFor(sessionId, where, sb);
                    sb = new StringBuilder();
                    n = 0;
                }
            }
            if (n > 0) {
                size += evalSizeFor(sessionId, where, sb);
            }
        }
        return size;
    }


    private long evalSizeFor(String sessionId, String where, StringBuilder sb) throws InternalException {
        String query = "SELECT SUM(df.fileSize) from Datafile df WHERE " + where + " IN (" + sb.toString() + ") AND df.location IS NOT NULL";
        logger.debug("icat query for size: {}", query);
        try {
            return (Long) ServiceProvider.getInstance().getIcat().search(sessionId, query).get(0);
        } catch (IcatException_Exception e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        } catch (IndexOutOfBoundsException e) {
            return 0L;
        }
    }
    
}