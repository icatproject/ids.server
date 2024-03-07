package org.icatproject.ids.v3.handlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.icatproject.Datafile;
import org.icatproject.EntityBaseBean;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.LockManager.Lock;
import org.icatproject.ids.LockManager.LockType;
import org.icatproject.ids.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.plugin.AlreadyLockedException;
import org.icatproject.ids.v3.DataSelectionV3Base;
import org.icatproject.ids.v3.RequestHandlerBase;
import org.icatproject.ids.v3.ServiceProvider;
import org.icatproject.ids.v3.enums.CallType;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.models.DataInfoBase;
import org.icatproject.ids.v3.models.ValueContainer;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class DeleteHandler extends RequestHandlerBase {

    public DeleteHandler() {
        super(new StorageUnit[]{StorageUnit.DATAFILE, StorageUnit.DATASET, null}, RequestType.DELETE);
    }

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {
        
        long start = System.currentTimeMillis();
        var serviceProvider = ServiceProvider.getInstance();

        String sessionId = parameters.get("sessionId").getString();
        String investigationIds = parameters.get("investigationIds").getString();
        String datasetIds = parameters.get("datasetIds").getString();
        String datafileIds = parameters.get("datafileIds").getString();
        String ip = parameters.get("ip").getString();

        logger.info("New webservice request: delete " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
                + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

        if (readOnly) {
            throw new NotImplementedException("This operation has been configured to be unavailable");
        }

        validateUUID("sessionId", sessionId);

        DataSelectionV3Base dataSelection = this.getDataSelection( sessionId, investigationIds, datasetIds, datafileIds);

        // Do it
        try (Lock lock = serviceProvider.getLockManager().lock(dataSelection.getDsInfo().values(), LockType.EXCLUSIVE)) {
            if (storageUnit == StorageUnit.DATASET) {
                dataSelection.checkOnline();
            }

            /* Now delete from ICAT */
            List<EntityBaseBean> dfs = new ArrayList<>();
            for (DataInfoBase dfInfo : dataSelection.getDfInfo().values()) {
                Datafile df = new Datafile();
                df.setId(dfInfo.getId());
                dfs.add(df);
            }
            try {
                serviceProvider.getIcat().deleteMany(sessionId, dfs);
            } catch (IcatException_Exception e) {
                IcatExceptionType type = e.getFaultInfo().getType();

                if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES || type == IcatExceptionType.SESSION) {
                    throw new InsufficientPrivilegesException(e.getMessage());
                }
                if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
                    throw new NotFoundException(e.getMessage());
                }
                throw new InternalException(type + " " + e.getMessage());
            }

            dataSelection.delete();

        } catch (AlreadyLockedException e) {
            logger.debug("Could not acquire lock, delete failed");
            throw new DataNotOnlineException("Data is busy");
        } catch (IOException e) {
            logger.error("I/O error " + e.getMessage());
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        if (serviceProvider.getLogSet().contains(CallType.WRITE)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", serviceProvider.getIcat().getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                String body = baos.toString();
                serviceProvider.getTransmitter().processMessage("delete", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return ValueContainer.getVoid();
    }
}