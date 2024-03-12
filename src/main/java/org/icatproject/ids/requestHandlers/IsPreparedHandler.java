package org.icatproject.ids.requestHandlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.enums.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.Prepared;
import org.icatproject.ids.services.ServiceProvider;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class IsPreparedHandler extends RequestHandlerBase {

    public IsPreparedHandler() {
        super(new StorageUnit[]{StorageUnit.DATAFILE, StorageUnit.DATASET, null}, RequestType.ISPREPARED);
    }

    class PreparedStatus {
        public ReentrantLock lock = new ReentrantLock();
        public Long fromElement;
        public Future<?> future;

    }

    private Map<String, PreparedStatus> preparedStatusMap = new ConcurrentHashMap<>();

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {
        
        long start = System.currentTimeMillis();

        String preparedId = parameters.get("preparedId").getString();
        String ip = parameters.get("ip").getString();

        logger.info(String.format("New webservice request: isPrepared preparedId=%s", preparedId));

        // Validate
        validateUUID("preparedId", preparedId);

        // Do it
        boolean prepared = true;

        Prepared preparedJson;
        try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
            preparedJson = unpack(stream);
        } catch (NoSuchFileException e) {
            throw new NotFoundException("The preparedId " + preparedId + " is not known");
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        PreparedStatus status = preparedStatusMap.computeIfAbsent(preparedId, k -> new PreparedStatus());

        if (!status.lock.tryLock()) {
            logger.debug("Lock held for evaluation of isPrepared for preparedId {}", preparedId);
            return new ValueContainer(false);
        }
        try {
            Future<?> future = status.future;
            if (future != null) {
                if (future.isDone()) {
                    try {
                        future.get();
                    } catch (ExecutionException e) {
                        throw new InternalException(e.getClass() + " " + e.getMessage());
                    } catch (InterruptedException e) {
                        // Ignore
                    } finally {
                        status.future = null;
                    }
                } else {
                    logger.debug("Background process still running for preparedId {}", preparedId);
                    return new ValueContainer(false);
                }
            }

            var serviceProvider = ServiceProvider.getInstance();
            DataSelectionBase dataSelection = this.getDataSelection(preparedJson.dsInfos, preparedJson.dfInfos, preparedJson.emptyDatasets);

            prepared = dataSelection.isPrepared(preparedId);

            if (serviceProvider.getLogSet().contains(CallType.INFO)) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("preparedId", preparedId);
                    gen.writeEnd();
                }
                String body = baos.toString();
                serviceProvider.getTransmitter().processMessage("isPrepared", ip, body, start);
            }

            return new ValueContainer(prepared);

        } finally {
            status.lock.unlock();
        }
    }
}