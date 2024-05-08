package org.icatproject.ids.requestHandlers.getSizeHandlers;

import java.util.List;

import org.icatproject.Datafile;
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
import org.icatproject.ids.requestHandlers.base.UnpreparedDataController;
import org.icatproject.ids.services.ServiceProvider;
import org.icatproject.ids.services.dataSelectionService.DataSelectionService;

import jakarta.json.stream.JsonGenerator;

public class GetSizeHandlerForFastProcessing extends RequestHandlerBase {

    UnpreparedDataController dataController;

    public GetSizeHandlerForFastProcessing(String ip, String sessionId, String investigationIds, String datasetIds, String datafileIds) {
        super(RequestType.GETSIZE, ip);

        this.dataController = new UnpreparedDataController(sessionId, investigationIds, datasetIds, datafileIds);
    }

    @Override
    public ValueContainer handleRequest() throws BadRequestException, InternalException,
            InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException {
        
        this.dataController.validateUUID();
        var serviceProvider = ServiceProvider.getInstance();

        List<Long> dfids = DataSelectionService.getValidIds("datafileIds", dataController.datafileIds);
        List<Long> dsids = DataSelectionService.getValidIds("datasetIds", dataController.datasetIds);
        List<Long> invids = DataSelectionService.getValidIds("investigationIds", dataController.investigationIds);

        
        if (dfids.size() + dsids.size() + invids.size() == 1) {

            long size = 0;
            size = getSizeFor(dataController.getOperationId(), invids, "df.dataset.investigation.id")
                    + getSizeFor(dataController.getOperationId(), dsids, "df.dataset.id") 
                    + getSizeFor(dataController.getOperationId(), dfids, "df.id");
            logger.debug("Fast computation for simple case");
            if (size == 0) {
                try {
                    if (dfids.size() != 0) {
                        Datafile datafile = (Datafile) serviceProvider.getIcat().get(dataController.getOperationId(), "Datafile", dfids.get(0));
                        if (datafile.getLocation() == null) {
                            throw new NotFoundException("Datafile not found");
                        }
                    }
                    if (dsids.size() != 0) {
                        serviceProvider.getIcat().get(dataController.getOperationId(), "Dataset", dsids.get(0));
                    }
                    if (invids.size() != 0) {
                        serviceProvider.getIcat().get(dataController.getOperationId(), "Investigation", invids.get(0));
                    }
                } catch (IcatException_Exception e) {
                    throw new NotFoundException(e.getMessage());
                }
            }

            return new ValueContainer(size);
        }
        
        return ValueContainer.getInvalid(); //is case of fast computation is not the right way.

        
    }

    @Override
    public CallType getCallType() {
        return CallType.INFO;
    }

    public String getRequestParametersLogString() { return this.dataController.getRequestParametersLogString(); }

    public void addParametersToTransmitterJSON(JsonGenerator gen) throws IcatException_Exception, BadRequestException {
        this.dataController.addParametersToTransmitterJSON(gen);
    }

    private long getSizeFor(String sessionId, List<Long> ids, String where) throws InternalException {

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
