package org.icatproject.ids.v3.handlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.security.NoSuchAlgorithmException;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.CRC32;
import javax.xml.datatype.DatatypeFactory;

import org.icatproject.Datafile;
import org.icatproject.DatafileFormat;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.CheckedWithSizeInputStream;
import org.icatproject.ids.LockManager.Lock;
import org.icatproject.ids.LockManager.LockType;
import org.icatproject.ids.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.IdsException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.plugin.AlreadyLockedException;
import org.icatproject.ids.v3.DataSelectionV3Base;
import org.icatproject.ids.v3.RequestHandlerBase;
import org.icatproject.ids.v3.ServiceProvider;
import org.icatproject.ids.v3.enums.CallType;
import org.icatproject.ids.v3.enums.DeferredOp;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.models.DataFileInfo;
import org.icatproject.ids.v3.models.DataInfoBase;
import org.icatproject.ids.v3.models.DataSetInfo;
import org.icatproject.ids.v3.models.ValueContainer;
import org.icatproject.utils.IcatSecurity;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;
import jakarta.ws.rs.core.Response;

public class PutHandler extends RequestHandlerBase {

    private DatatypeFactory datatypeFactory;
    private static String paddedPrefix;
    private static final String prefix = "<html><script type=\"text/javascript\">window.name='";
    private static final String suffix = "';</script></html>";

    static {
        paddedPrefix = "<html><script type=\"text/javascript\">/*";
        for (int n = 1; n < 25; n++) {
            paddedPrefix += " *        \n";
        }
        paddedPrefix += "*/window.name='";
    }

    public PutHandler() {
        super(new StorageUnit[]{StorageUnit.DATAFILE, StorageUnit.DATASET, null}, RequestType.PUT);
    }

    public void init() throws InternalException {
        super.init();
        
        try {

            this.datatypeFactory = DatatypeFactory.newInstance();

        } catch (Throwable e) {
            logger.error("Won't start ", e);
            throw new RuntimeException("PutHandler reports " + e.getClass() + " " + e.getMessage());
        }
    }

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {
        
        long start = System.currentTimeMillis();
        var serviceProvider = ServiceProvider.getInstance();

        String sessionId = parameters.get("sessionId").getString();
        InputStream body = parameters.get("body").getInputStream();
        String name = parameters.get("name").getString();
        String datafileFormatIdString = parameters.get("datafileFormatId").getString();
        String datasetIdString = parameters.get("datasetId").getString();
        String description = parameters.get("description").getString();
        String doi = parameters.get("doi").getString();
        String datafileCreateTimeString = parameters.get("datafileCreateTime").getString();
        String datafileModTimeString = parameters.get("datafileModTime").getString();
        boolean wrap = parameters.get("wrap").getBool();
        boolean padding = parameters.get("padding").getBool();
        String ip = parameters.get("ip").getString();

        try {
            // Log and validate
            logger.info("New webservice request: put " + "name='" + name + "' " + "datafileFormatId='"
                    + datafileFormatIdString + "' " + "datasetId='" + datasetIdString + "' " + "description='"
                    + description + "' " + "doi='" + doi + "' " + "datafileCreateTime='" + datafileCreateTimeString
                    + "' " + "datafileModTime='" + datafileModTimeString + "'");

            if (readOnly) {
                throw new NotImplementedException("This operation has been configured to be unavailable");
            }

            validateUUID("sessionId", sessionId);
            if (name == null) {
                throw new BadRequestException("The name parameter must be set");
            }

            if (datafileFormatIdString == null) {
                throw new BadRequestException("The datafileFormatId parameter must be set");
            }
            long datafileFormatId;
            try {
                datafileFormatId = Long.parseLong(datafileFormatIdString);
            } catch (NumberFormatException e) {
                throw new BadRequestException("The datafileFormatId parameter must be numeric");
            }

            if (datasetIdString == null) {
                throw new BadRequestException("The datasetId parameter must be set");
            }
            long datasetId;
            try {
                datasetId = Long.parseLong(datasetIdString);
            } catch (NumberFormatException e) {
                throw new BadRequestException("The datasetId parameter must be numeric");
            }

            Long datafileCreateTime = null;
            if (datafileCreateTimeString != null) {
                try {
                    datafileCreateTime = Long.parseLong(datafileCreateTimeString);
                } catch (NumberFormatException e) {
                    throw new BadRequestException("The datafileCreateTime parameter must be numeric");
                }
            }

            Long datafileModTime = null;
            if (datafileModTimeString != null) {
                try {
                    datafileModTime = Long.parseLong(datafileModTimeString);
                } catch (NumberFormatException e) {
                    throw new BadRequestException("The datafileModTime parameter must be numeric");
                }
            }

            // Do it
            Dataset ds;
            try {
                ds = (Dataset) serviceProvider.getIcat().get(sessionId, "Dataset INCLUDE Investigation, Facility", datasetId);
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

            DataSetInfo dsInfo = new DataSetInfo(ds);
            try (Lock lock = serviceProvider.getLockManager().lock(dsInfo, LockType.SHARED)) {
                if (storageUnit == StorageUnit.DATASET) {
                    var dfInfos = new TreeMap<Long, DataInfoBase>();
                    Set<Long> emptyDatasets = new HashSet<>();
                    try {
                        List<Object> counts = serviceProvider.getIcat().search(sessionId,
                                "COUNT(Datafile) <-> Dataset [id=" + dsInfo.getId() + "]");
                        if ((Long) counts.get(0) == 0) {
                            emptyDatasets.add(dsInfo.getId());
                        }
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
                    var dsInfos = new TreeMap<Long, DataInfoBase>();
                    dsInfos.put(dsInfo.getId(), dsInfo);
                    DataSelectionV3Base dataSelection = this.getDataSelection(dsInfos, dfInfos, emptyDatasets);
                    dataSelection.checkOnline();
                }

                CRC32 crc = new CRC32();
                CheckedWithSizeInputStream is = new CheckedWithSizeInputStream(body, crc);
                String location;
                try {
                    location = serviceProvider.getMainStorage().put(dsInfo, name, is);
                } catch (IllegalArgumentException e) {
                    throw new BadRequestException("Illegal filename or dataset: " + e.getMessage());
                }
                is.close();
                long checksum = crc.getValue();
                long size = is.getSize();
                Long dfId;
                try {
                    dfId = registerDatafile(sessionId, name, datafileFormatId, location, checksum, size, ds,
                            description, doi, datafileCreateTime, datafileModTime);
                } catch (InsufficientPrivilegesException | NotFoundException | InternalException
                         | BadRequestException e) {
                    logger.debug("Problem with registration " + e.getClass() + " " + e.getMessage()
                            + " datafile will now be deleted");
                    String userId = null;
                    try {
                        userId = serviceProvider.getIcat().getUserName(sessionId);
                    } catch (IcatException_Exception e1) {
                        logger.error("Unable to get user name for session " + sessionId + " so mainStorage.delete of "
                                + location + " may fail");
                    }
                    serviceProvider.getMainStorage().delete(location, userId, userId);
                    throw e;
                }

                if (storageUnit == StorageUnit.DATASET) {
                    serviceProvider.getFsm().queue(dsInfo, DeferredOp.WRITE);
                } else if (storageUnit == StorageUnit.DATAFILE) {
                    Datafile df;
                    try {
                        df = (Datafile) serviceProvider.getIcatReader().get("Datafile", dfId);
                    } catch (IcatException_Exception e) {
                        throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
                    }
                    serviceProvider.getFsm().queue(new DataFileInfo(dfId, name, location, df.getCreateId(), df.getModId(), dsInfo.getId()),
                            DeferredOp.WRITE);
                }

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                Json.createGenerator(baos).writeStartObject().write("id", dfId).write("checksum", checksum)
                        .write("location", location.replace("\\", "\\\\").replace("'", "\\'")).write("size", size)
                        .writeEnd().close();
                String resp = wrap ? prefix + baos.toString() + suffix : baos.toString();

                if (serviceProvider.getLogSet().contains(CallType.WRITE)) {
                    try {
                        baos = new ByteArrayOutputStream();
                        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                            gen.write("userName", serviceProvider.getIcat().getUserName(sessionId));
                            gen.write("datafileId", dfId);
                            gen.writeEnd();
                        }
                        serviceProvider.getTransmitter().processMessage("put", ip, baos.toString(), start);
                    } catch (IcatException_Exception e) {
                        logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
                    }
                }

                return new ValueContainer(Response.status(HttpURLConnection.HTTP_CREATED).entity(resp).build());

            } catch (AlreadyLockedException e) {
                logger.debug("Could not acquire lock, put failed");
                throw new DataNotOnlineException("Data is busy");
            } catch (IOException e) {
                logger.error("I/O exception " + e.getMessage() + " putting " + name + " to Dataset with id "
                        + datasetIdString);
                throw new InternalException(e.getClass() + " " + e.getMessage());
            }
        } catch (IdsException e) {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            JsonGenerator gen = Json.createGenerator(baos);
            gen.writeStartObject().write("code", e.getClass().getSimpleName()).write("message", e.getShortMessage());
            gen.writeEnd().close();
            if (wrap) {
                String pre = padding ? paddedPrefix : prefix;
                return new ValueContainer(Response.status(e.getHttpStatusCode()).entity(pre + baos.toString().replace("'", "\\'") + suffix)
                        .build());
            } else {
                return new ValueContainer(Response.status(e.getHttpStatusCode()).entity(baos.toString()).build());
            }
        }
    }


    private Long registerDatafile(String sessionId, String name, long datafileFormatId, String location, long checksum,
                                  long size, Dataset dataset, String description, String doi, Long datafileCreateTime, Long datafileModTime)
            throws InsufficientPrivilegesException, NotFoundException, InternalException, BadRequestException {

        
        var serviceProvider = ServiceProvider.getInstance();
        ICAT icat = serviceProvider.getIcat();
        
        final Datafile df = new Datafile();
        DatafileFormat format;
        try {
            format = (DatafileFormat) icat.get(sessionId, "DatafileFormat", datafileFormatId);
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

        df.setDatafileFormat(format);
        df.setLocation(location);
        df.setFileSize(size);
        df.setChecksum(Long.toHexString(checksum));
        df.setName(name);
        df.setDataset(dataset);
        df.setDescription(description);
        df.setDoi(doi);
        if (datafileCreateTime != null) {
            GregorianCalendar gregorianCalendar = new GregorianCalendar();
            gregorianCalendar.setTimeInMillis(datafileCreateTime);
            df.setDatafileCreateTime(datatypeFactory.newXMLGregorianCalendar(gregorianCalendar));
        }
        if (datafileModTime != null) {
            GregorianCalendar gregorianCalendar = new GregorianCalendar();
            gregorianCalendar.setTimeInMillis(datafileModTime);
            df.setDatafileModTime(datatypeFactory.newXMLGregorianCalendar(gregorianCalendar));
        }
        try {
            long dfId = icat.create(sessionId, df);
            df.setId(dfId);

            String key = serviceProvider.getPropertyHandler().getKey();
            if (key != null) {
                df.setLocation(location + " " + IcatSecurity.digest(dfId, location, key));
                icat.update(sessionId, df);
            }

            logger.debug("Registered datafile for dataset {} for {}", dataset.getId(), name + " at " + location);
            return dfId;
        } catch (IcatException_Exception e) {
            IcatExceptionType type = e.getFaultInfo().getType();
            if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES || type == IcatExceptionType.SESSION) {
                throw new InsufficientPrivilegesException(e.getMessage());
            }
            if (type == IcatExceptionType.VALIDATION) {
                throw new BadRequestException(e.getMessage());
            }
            throw new InternalException(type + " " + e.getMessage());
        } catch (NoSuchAlgorithmException e) {
            throw new InternalException(e.getMessage());
        }
    }
}