package org.icatproject.ids.v3.handlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.icatproject.Datafile;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.Prepared;
import org.icatproject.ids.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.v3.DataSelectionV3Base;
import org.icatproject.ids.v3.PreparedV3;
import org.icatproject.ids.v3.RequestHandlerBase;
import org.icatproject.ids.v3.ServiceProvider;
import org.icatproject.ids.v3.enums.CallType;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.models.DataInfoBase;
import org.icatproject.ids.v3.models.ValueContainer;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class GetSizeHandler extends RequestHandlerBase {

    public GetSizeHandler() {
        super(new StorageUnit[] {StorageUnit.DATAFILE, StorageUnit.DATASET, null}, RequestType.GETSIZE);
    }

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {
        
        String preparedId = parameters.get("preparedId").getString();
        String sessionId = parameters.get("sessionId").getString();
        String investigationIds = parameters.get("investigationIds").getString();
        String datasetIds = parameters.get("datasetIds").getString();
        String datafileIds = parameters.get("datafileIds").getString();
        String ip = parameters.get("ip").getString();
        
        if (preparedId != null) {
            return new ValueContainer(this.getSize(preparedId, ip));
        } else {
            return new ValueContainer(this.getSize(sessionId, investigationIds, datasetIds, datafileIds, ip));
        }
    }


    public long getSize(String preparedId, String ip)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

        long start = System.currentTimeMillis();

        var serviceProvider = ServiceProvider.getInstance();

        // Log and validate
        logger.info("New webservice request: getSize preparedId = '{}'", preparedId);
        validateUUID("preparedId", preparedId);

        // Do it
        PreparedV3 prepared;
        try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
            prepared = unpack(stream);
        } catch (NoSuchFileException e) {
            throw new NotFoundException("The preparedId " + preparedId + " is not known");
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        final Map<Long, DataInfoBase> dfInfos = prepared.dfInfos;

        // Note that the "fast computation for the simple case" (see the other getSize() implementation) is not
        // available when calling getSize() with a preparedId.
        logger.debug("Slow computation for normal case");
        String sessionId;
        try {
            sessionId = serviceProvider.getIcatReader().getSessionId();
        } catch (IcatException_Exception e) {
            throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
        }
        long size = 0;

        StringBuilder sb = new StringBuilder();
        int n = 0;
        for (DataInfoBase df : dfInfos.values()) {
            if (sb.length() != 0) {
                sb.append(',');
            }
            sb.append(df.getId());
            if (n++ == 500) {
                size += getSizeFor(sessionId, sb);
                sb = new StringBuilder();
                n = 0;
            }
        }
        if (n > 0) {
            size += getSizeFor(sessionId, sb);
        }

        if (serviceProvider.getLogSet().contains(CallType.INFO)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                gen.write("preparedId", preparedId);
                gen.writeEnd();
            }
            String body = baos.toString();
            serviceProvider.getTransmitter().processMessage("getSize", ip, body, start);
        }

        return size;
    }


    public long getSize(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException, NotImplementedException {

        long start = System.currentTimeMillis();
        var serviceProvider = ServiceProvider.getInstance();

        // Log and validate
        logger.info(String.format("New webservice request: getSize investigationIds=%s, datasetIds=%s, datafileIds=%s",
                investigationIds, datasetIds, datafileIds));

        validateUUID("sessionId", sessionId);

        List<Long> dfids = DataSelectionV3Base.getValidIds("datafileIds", datafileIds);
        List<Long> dsids = DataSelectionV3Base.getValidIds("datasetIds", datasetIds);
        List<Long> invids = DataSelectionV3Base.getValidIds("investigationIds", investigationIds);

        long size = 0;
        if (dfids.size() + dsids.size() + invids.size() == 1) {
            size = getSizeFor(sessionId, invids, "df.dataset.investigation.id")
                    + getSizeFor(sessionId, dsids, "df.dataset.id") + getSizeFor(sessionId, dfids, "df.id");
            logger.debug("Fast computation for simple case");
            if (size == 0) {
                try {
                    if (dfids.size() != 0) {
                        Datafile datafile = (Datafile) serviceProvider.getIcat().get(sessionId, "Datafile", dfids.get(0));
                        if (datafile.getLocation() == null) {
                            throw new NotFoundException("Datafile not found");
                        }
                    }
                    if (dsids.size() != 0) {
                        serviceProvider.getIcat().get(sessionId, "Dataset", dsids.get(0));
                    }
                    if (invids.size() != 0) {
                        serviceProvider.getIcat().get(sessionId, "Investigation", invids.get(0));
                    }
                } catch (IcatException_Exception e) {
                    throw new NotFoundException(e.getMessage());
                }
            }
        } else {
            logger.debug("Slow computation for normal case");
            final DataSelectionV3Base dataSelection = this.getDataSelection(sessionId, investigationIds, datasetIds, datafileIds);

            StringBuilder sb = new StringBuilder();
            int n = 0;
            for (DataInfoBase df : dataSelection.getDfInfo().values()) {
                if (sb.length() != 0) {
                    sb.append(',');
                }
                sb.append(df.getId());
                if (n++ == 500) {
                    size += getSizeFor(sessionId, sb);
                    sb = new StringBuilder();
                    n = 0;
                }
            }
            if (n > 0) {
                size += getSizeFor(sessionId, sb);
            }
        }

        if (serviceProvider.getLogSet().contains(CallType.INFO)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", serviceProvider.getIcat().getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                String body = baos.toString();
                serviceProvider.getTransmitter().processMessage("getSize", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
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