package org.icatproject.ids;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import javax.xml.datatype.DatatypeFactory;

import jakarta.annotation.PostConstruct;
import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import jakarta.json.Json;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonGenerator;
import jakarta.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.icatproject.Datafile;
import org.icatproject.DatafileFormat;
import org.icatproject.Dataset;
import org.icatproject.EntityBaseBean;
import org.icatproject.ICAT;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.DataSelection.Returns;
import org.icatproject.ids.LockManager.Lock;
import org.icatproject.ids.LockManager.LockType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.IdsException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.plugin.AlreadyLockedException;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
import org.icatproject.ids.v3.FiniteStateMachine.FiniteStateMachine;
import org.icatproject.ids.v3.enums.CallType;
import org.icatproject.ids.v3.enums.DeferredOp;
import org.icatproject.ids.v3.models.DataFileInfo;
import org.icatproject.ids.v3.models.DataInfoBase;
import org.icatproject.ids.v3.models.DataSetInfo;
import org.icatproject.utils.IcatSecurity;

@Stateless
public class IdsBean {

    public class RunPrepDsCheck implements Callable<Void> {

        private Collection<DataSetInfo> toCheck;
        private Set<Long> emptyDatasets;

        public RunPrepDsCheck(Collection<DataSetInfo> toCheck, Set<Long> emptyDatasets) {
            this.toCheck = toCheck;
            this.emptyDatasets = emptyDatasets;
        }

        @Override
        public Void call() throws Exception {
            for (DataSetInfo dsInfo : toCheck) {
                fsm.checkFailure(dsInfo.getId());
                DataSelection.restoreIfOffline(dsInfo, emptyDatasets);
            }
            return null;
        }

    }

    public class RunPrepDfCheck implements Callable<Void> {

        private SortedSet<DataFileInfo> toCheck;

        public RunPrepDfCheck(SortedSet<DataFileInfo> toCheck) {
            this.toCheck = toCheck;
        }

        @Override
        public Void call() throws Exception {
            for (DataFileInfo dfInfo : toCheck) {
                fsm.checkFailure(dfInfo.getId());
                DataSelection.restoreIfOffline(dfInfo);
            }
            return null;
        }

    }

    public class RestoreDfTask implements Callable<Void> {

        private Set<DataFileInfo> dfInfos;

        public RestoreDfTask(Set<DataFileInfo> dfInfos) {
            this.dfInfos = dfInfos;
        }

        @Override
        public Void call() throws Exception {
            for (DataFileInfo dfInfo : dfInfos) {
                DataSelection.restoreIfOffline(dfInfo);
            }
            return null;
        }

    }

    public class RestoreDsTask implements Callable<Void> {
        private Collection<DataSetInfo> dsInfos;
        private Set<Long> emptyDs;

        public RestoreDsTask(Collection<DataSetInfo> dsInfos, Set<Long> emptyDs) {
            this.dsInfos = dsInfos;
            this.emptyDs = emptyDs;
        }

        @Override
        public Void call() throws Exception {
            for (DataSetInfo dsInfo : dsInfos) {
                DataSelection.restoreIfOffline(dsInfo, emptyDs);
            }
            return null;
        }
    }

    private static Boolean inited = false;

    private static String key;

    private final static Logger logger = LoggerFactory.getLogger(IdsBean.class);
    private static String paddedPrefix;
    private static final String prefix = "<html><script type=\"text/javascript\">window.name='";
    private static final String suffix = "';</script></html>";

    /**
     * matches standard UUID format of 8-4-4-4-12 hexadecimal digits
     */
    public static final Pattern uuidRegExp = Pattern
            .compile("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$");

    static {
        paddedPrefix = "<html><script type=\"text/javascript\">/*";
        for (int n = 1; n < 25; n++) {
            paddedPrefix += " *        \n";
        }
        paddedPrefix += "*/window.name='";
    }

    static void cleanDatasetCache(Path datasetDir) {
        for (File dsFile : datasetDir.toFile().listFiles()) {
            Path path = dsFile.toPath();
            try {
                long thisSize = Files.size(path);
                Files.delete(path);
                logger.debug("Deleted " + path + " to reclaim " + thisSize + " bytes");
            } catch (IOException e) {
                logger.debug("Failed to delete " + path + " " + e.getClass() + " " + e.getMessage());
            }
        }
    }

    static void cleanPreparedDir(Path preparedDir) {
        for (File file : preparedDir.toFile().listFiles()) {
            Path path = file.toPath();
            String pf = path.getFileName().toString();
            if (pf.startsWith("tmp.") || pf.endsWith(".tmp")) {
                try {
                    long thisSize = 0;
                    if (Files.isDirectory(path)) {
                        for (File notZipFile : file.listFiles()) {
                            thisSize += Files.size(notZipFile.toPath());
                            Files.delete(notZipFile.toPath());
                        }
                    }
                    thisSize += Files.size(path);
                    Files.delete(path);
                    logger.debug("Deleted " + path + " to reclaim " + thisSize + " bytes");
                } catch (IOException e) {
                    logger.debug("Failed to delete " + path + e.getMessage());
                }
            }
        }
    }

    public static String getLocation(long dfid, String location)
            throws InsufficientPrivilegesException, InternalException {
        if (location == null) {
            throw new InternalException("location is null");
        }
        if (key == null) {
            return location;
        } else {
            return getLocationFromDigest(dfid, location, key);
        }
    }

    static String getLocationFromDigest(long id, String locationWithHash, String key)
            throws InternalException, InsufficientPrivilegesException {
        int i = locationWithHash.lastIndexOf(' ');
        try {
            String location = locationWithHash.substring(0, i);
            String hash = locationWithHash.substring(i + 1);
            if (!hash.equals(IcatSecurity.digest(id, location, key))) {
                throw new InsufficientPrivilegesException(
                        "Location \"" + locationWithHash + "\" does not contain a valid hash.");
            }
            return location;
        } catch (IndexOutOfBoundsException e) {
            throw new InsufficientPrivilegesException("Location \"" + locationWithHash + "\" does not contain hash.");
        } catch (NoSuchAlgorithmException e) {
            throw new InternalException(e.getMessage());
        }
    }

    static void pack(OutputStream stream, boolean zip, boolean compress, Map<Long, DataSetInfo> dsInfos,
                     Set<DataFileInfo> dfInfos, Set<Long> emptyDatasets) {
        JsonGenerator gen = Json.createGenerator(stream);
        gen.writeStartObject();
        gen.write("zip", zip);
        gen.write("compress", compress);

        gen.writeStartArray("dsInfo");
        for (DataSetInfo dsInfo : dsInfos.values()) {
            logger.debug("dsInfo " + dsInfo);
            gen.writeStartObject().write("dsId", dsInfo.getId())

                    .write("dsName", dsInfo.getDsName()).write("facilityId", dsInfo.getFacilityId())
                    .write("facilityName", dsInfo.getFacilityName()).write("invId", dsInfo.getInvId())
                    .write("invName", dsInfo.getInvName()).write("visitId", dsInfo.getVisitId());
            if (dsInfo.getDsLocation() != null) {
                gen.write("dsLocation", dsInfo.getDsLocation());
            } else {
                gen.writeNull("dsLocation");
            }
            gen.writeEnd();
        }
        gen.writeEnd();

        gen.writeStartArray("dfInfo");
        for (DataFileInfo dfInfo : dfInfos) {
            DataSetInfo dsInfo = dsInfos.get(dfInfo.getDsId());
            gen.writeStartObject().write("dsId", dsInfo.getId()).write("dfId", dfInfo.getId())
                    .write("dfName", dfInfo.getDfName()).write("createId", dfInfo.getCreateId())
                    .write("modId", dfInfo.getModId());
            if (dfInfo.getDfLocation() != null) {
                gen.write("dfLocation", dfInfo.getDfLocation());
            } else {
                gen.writeNull("dfLocation");
            }
            gen.writeEnd();

        }
        gen.writeEnd();

        gen.writeStartArray("emptyDs");
        for (Long emptyDs : emptyDatasets) {
            gen.write(emptyDs);
        }
        gen.writeEnd();

        gen.writeEnd().close();

    }

    static Prepared unpack(InputStream stream) throws InternalException {
        Prepared prepared = new Prepared();
        JsonObject pd;
        try (JsonReader jsonReader = Json.createReader(stream)) {
            pd = jsonReader.readObject();
        }
        prepared.zip = pd.getBoolean("zip");
        prepared.compress = pd.getBoolean("compress");
        SortedMap<Long, DataSetInfo> dsInfos = new TreeMap<>();
        SortedSet<DataFileInfo> dfInfos = new TreeSet<>();
        Set<Long> emptyDatasets = new HashSet<>();

        for (JsonValue itemV : pd.getJsonArray("dfInfo")) {
            JsonObject item = (JsonObject) itemV;
            String dfLocation = item.isNull("dfLocation") ? null : item.getString("dfLocation");
            dfInfos.add(new DataFileInfo(item.getJsonNumber("dfId").longValueExact(), item.getString("dfName"),
                    dfLocation, item.getString("createId"), item.getString("modId"),
                    item.getJsonNumber("dsId").longValueExact()));

        }
        prepared.dfInfos = dfInfos;

        for (JsonValue itemV : pd.getJsonArray("dsInfo")) {
            JsonObject item = (JsonObject) itemV;
            long dsId = item.getJsonNumber("dsId").longValueExact();
            String dsLocation = item.isNull("dsLocation") ? null : item.getString("dsLocation");
            dsInfos.put(dsId, new DataSetInfo(dsId, item.getString("dsName"), dsLocation,
                    item.getJsonNumber("invId").longValueExact(), item.getString("invName"), item.getString("visitId"),
                    item.getJsonNumber("facilityId").longValueExact(), item.getString("facilityName")));
        }
        prepared.dsInfos = dsInfos;

        for (JsonValue itemV : pd.getJsonArray("emptyDs")) {
            emptyDatasets.add(((JsonNumber) itemV).longValueExact());
        }
        prepared.emptyDatasets = emptyDatasets;

        return prepared;
    }

    public static void validateUUID(String thing, String id) throws BadRequestException {
        if (id == null || !uuidRegExp.matcher(id).matches())
            throw new BadRequestException("The " + thing + " parameter '" + id + "' is not a valid UUID");
    }

    @EJB
    Transmitter transmitter;

    private ExecutorService threadPool;

    private ArchiveStorageInterface archiveStorage;

    private Path datasetDir;

    private DatatypeFactory datatypeFactory;

    private boolean enableWrite;

    private FiniteStateMachine fsm = null;

    @EJB
    private LockManager lockManager;

    private ICAT icat;

    private MainStorageInterface mainStorage;

    private Path markerDir;

    private Path preparedDir;

    private PropertyHandler propertyHandler;

    @EJB
    IcatReader reader;

    private boolean readOnly;

    private Set<String> rootUserNames;

    private StorageUnit storageUnit;

    private boolean twoLevel;

    private Set<CallType> logSet;

    class PreparedStatus {
        public ReentrantLock lock = new ReentrantLock();
        public DataFileInfo fromDfElement;
        public Future<?> future;
        public Long fromDsElement;
    }

    private Map<String, PreparedStatus> preparedStatusMap = new ConcurrentHashMap<>();

    private void addIds(JsonGenerator gen, String investigationIds, String datasetIds, String datafileIds)
            throws BadRequestException {
        if (investigationIds != null) {
            gen.writeStartArray("investigationIds");
            for (long invid : DataSelection.getValidIds("investigationIds", investigationIds)) {
                gen.write(invid);
            }
            gen.writeEnd();
        }
        if (datasetIds != null) {
            gen.writeStartArray("datasetIds");
            for (long invid : DataSelection.getValidIds("datasetIds", datasetIds)) {
                gen.write(invid);
            }
            gen.writeEnd();
        }
        if (datafileIds != null) {
            gen.writeStartArray("datafileIds");
            for (long invid : DataSelection.getValidIds("datafileIds", datafileIds)) {
                gen.write(invid);
            }
            gen.writeEnd();
        }
    }

    public void archive(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
            throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, InternalException,
            NotFoundException {

        long start = System.currentTimeMillis();

        // Log and validate
        logger.info("New webservice request: archive " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
                + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

        validateUUID("sessionId", sessionId);

        // Do it
        if (storageUnit == StorageUnit.DATASET) {
            DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
                    investigationIds, datasetIds, datafileIds, Returns.DATASETS);
            Map<Long, DataSetInfo> dsInfos = dataSelection.getDsInfo();
            for (DataSetInfo dsInfo : dsInfos.values()) {
                fsm.queue(dsInfo, DeferredOp.ARCHIVE);
            }
        } else if (storageUnit == StorageUnit.DATAFILE) {
            DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
                    investigationIds, datasetIds, datafileIds, Returns.DATAFILES);
            Set<DataFileInfo> dfInfos = dataSelection.getDfInfo();
            for (DataFileInfo dfInfo : dfInfos) {
                fsm.queue(dfInfo, DeferredOp.ARCHIVE);
            }
        } else {
            throw new NotImplementedException("This operation is unavailable for single level storage");
        }

        if (logSet.contains(CallType.MIGRATE)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", icat.getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                String body = baos.toString();
                transmitter.processMessage("archive", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }
    }

    public void delete(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
            throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, InternalException,
            NotFoundException, DataNotOnlineException {

        long start = System.currentTimeMillis();

        logger.info("New webservice request: delete " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
                + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

        if (readOnly) {
            throw new NotImplementedException("This operation has been configured to be unavailable");
        }

        IdsBean.validateUUID("sessionId", sessionId);

        DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
                investigationIds, datasetIds, datafileIds, Returns.DATASETS_AND_DATAFILES);

        // Do it
        Collection<DataSetInfo> dsInfos = dataSelection.getDsInfo().values();

        try (Lock lock = lockManager.lock(dsInfos, LockType.EXCLUSIVE)) {
            if (storageUnit == StorageUnit.DATASET) {
                dataSelection.checkOnline();
            }

            /* Now delete from ICAT */
            List<EntityBaseBean> dfs = new ArrayList<>();
            for (DataFileInfo dfInfo : dataSelection.getDfInfo()) {
                Datafile df = new Datafile();
                df.setId(dfInfo.getId());
                dfs.add(df);
            }
            try {
                icat.deleteMany(sessionId, dfs);
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

            /*
             * Delete the local copy directly rather than queueing it as it has
             * been removed from ICAT so will not be accessible to any
             * subsequent IDS calls.
             */
            for (DataFileInfo dfInfo : dataSelection.getDfInfo()) {
                String location = dfInfo.getDfLocation();
                try {
                    if ((long) reader
                            .search("SELECT COUNT(df) FROM Datafile df WHERE df.location LIKE '" + location.replaceAll("'", "''") + "%'")
                            .get(0) == 0) {
                        if (mainStorage.exists(location)) {
                            logger.debug("Delete physical file " + location + " from main storage");
                            mainStorage.delete(location, dfInfo.getCreateId(), dfInfo.getModId());
                        }
                        if (storageUnit == StorageUnit.DATAFILE) {
                            fsm.queue(dfInfo, DeferredOp.DELETE);
                        }
                    }
                } catch (IcatException_Exception e) {
                    throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
                } catch (IOException e) {
                    logger.error("I/O error " + e.getMessage() + " deleting " + dfInfo);
                    throw new InternalException(e.getClass() + " " + e.getMessage());
                }
            }

            if (storageUnit == StorageUnit.DATASET) {
                for (DataSetInfo dsInfo : dsInfos) {
                    fsm.queue(dsInfo, DeferredOp.WRITE);
                }
            }

        } catch (AlreadyLockedException e) {
            logger.debug("Could not acquire lock, delete failed");
            throw new DataNotOnlineException("Data is busy");
        } catch (IOException e) {
            logger.error("I/O error " + e.getMessage());
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        if (logSet.contains(CallType.WRITE)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", icat.getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                String body = baos.toString();
                transmitter.processMessage("delete", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }
    }

    public String getDatafileIds(String preparedId, String ip)
            throws BadRequestException, InternalException, NotFoundException {

        long start = System.currentTimeMillis();

        // Log and validate
        logger.info("New webservice request: getDatafileIds preparedId = '" + preparedId);

        validateUUID("preparedId", preparedId);

        // Do it
        Prepared prepared;
        try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
            prepared = unpack(stream);
        } catch (NoSuchFileException e) {
            throw new NotFoundException("The preparedId " + preparedId + " is not known");
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        final boolean zip = prepared.zip;
        final boolean compress = prepared.compress;
        final Set<DataFileInfo> dfInfos = prepared.dfInfos;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
            gen.write("zip", zip);
            gen.write("compress", compress);
            gen.writeStartArray("ids");
            for (DataFileInfo dfInfo : dfInfos) {
                gen.write(dfInfo.getId());
            }
            gen.writeEnd().writeEnd().close();
        }
        String resp = baos.toString();

        if (logSet.contains(CallType.INFO)) {
            baos = new ByteArrayOutputStream();
            try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                gen.write("preparedId", preparedId);
                gen.writeEnd();
            }
            transmitter.processMessage("getDatafileIds", ip, baos.toString(), start);
        }

        return resp;
    }

    public String getDatafileIds(String sessionId, String investigationIds, String datasetIds, String datafileIds,
                                 String ip)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

        long start = System.currentTimeMillis();

        // Log and validate
        logger.info(String.format(
                "New webservice request: getDatafileIds investigationIds=%s, datasetIds=%s, datafileIds=%s",
                investigationIds, datasetIds, datafileIds));

        validateUUID("sessionId", sessionId);

        final DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
                investigationIds, datasetIds, datafileIds, Returns.DATAFILES);

        // Do it
        Set<DataFileInfo> dfInfos = dataSelection.getDfInfo();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
            gen.writeStartArray("ids");
            for (DataFileInfo dfInfo : dfInfos) {
                gen.write(dfInfo.getId());
            }
            gen.writeEnd().writeEnd().close();
        }
        String resp = baos.toString();

        if (logSet.contains(CallType.INFO)) {
            baos = new ByteArrayOutputStream();
            try {
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", icat.getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                transmitter.processMessage("getDatafileIds", ip, baos.toString(), start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return resp;

    }

    public String getIcatUrl(String ip) {
        if (logSet.contains(CallType.INFO)) {
            transmitter.processMessage("getIcatUrl", ip, "{}", System.currentTimeMillis());
        }
        return propertyHandler.getIcatUrl();
    }

    public String getServiceStatus(String sessionId, String ip)
            throws InternalException, InsufficientPrivilegesException {

        long start = System.currentTimeMillis();

        // Log and validate
        logger.info("New webservice request: getServiceStatus");

        try {
            String uname = icat.getUserName(sessionId);
            if (!rootUserNames.contains(uname)) {
                throw new InsufficientPrivilegesException(uname + " is not included in the ids rootUserNames set.");
            }
        } catch (IcatException_Exception e) {
            IcatExceptionType type = e.getFaultInfo().getType();
            if (type == IcatExceptionType.SESSION) {
                throw new InsufficientPrivilegesException(e.getClass() + " " + e.getMessage());
            }
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        if (logSet.contains(CallType.INFO)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", icat.getUserName(sessionId));
                    gen.writeEnd();
                }
                String body = baos.toString();
                transmitter.processMessage("getServiceStatus", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return fsm.getServiceStatus();
    }

    public long getSize(String preparedId, String ip)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

        long start = System.currentTimeMillis();

        // Log and validate
        logger.info("New webservice request: getSize preparedId = '{}'", preparedId);
        validateUUID("preparedId", preparedId);

        // Do it
        Prepared prepared;
        try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
            prepared = unpack(stream);
        } catch (NoSuchFileException e) {
            throw new NotFoundException("The preparedId " + preparedId + " is not known");
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        final Set<DataFileInfo> dfInfos = prepared.dfInfos;

        // Note that the "fast computation for the simple case" (see the other getSize() implementation) is not
        // available when calling getSize() with a preparedId.
        logger.debug("Slow computation for normal case");
        String sessionId;
        try {
            sessionId = reader.getSessionId();
        } catch (IcatException_Exception e) {
            throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
        }
        long size = 0;

        StringBuilder sb = new StringBuilder();
        int n = 0;
        for (DataFileInfo df : dfInfos) {
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

        if (logSet.contains(CallType.INFO)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                gen.write("preparedId", preparedId);
                gen.writeEnd();
            }
            String body = baos.toString();
            transmitter.processMessage("getSize", ip, body, start);
        }

        return size;
    }


    public long getSize(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

        long start = System.currentTimeMillis();

        // Log and validate
        logger.info(String.format("New webservice request: getSize investigationIds=%s, datasetIds=%s, datafileIds=%s",
                investigationIds, datasetIds, datafileIds));

        validateUUID("sessionId", sessionId);

        List<Long> dfids = DataSelection.getValidIds("datafileIds", datafileIds);
        List<Long> dsids = DataSelection.getValidIds("datasetIds", datasetIds);
        List<Long> invids = DataSelection.getValidIds("investigationIds", investigationIds);

        long size = 0;
        if (dfids.size() + dsids.size() + invids.size() == 1) {
            size = getSizeFor(sessionId, invids, "df.dataset.investigation.id")
                    + getSizeFor(sessionId, dsids, "df.dataset.id") + getSizeFor(sessionId, dfids, "df.id");
            logger.debug("Fast computation for simple case");
            if (size == 0) {
                try {
                    if (dfids.size() != 0) {
                        Datafile datafile = (Datafile) icat.get(sessionId, "Datafile", dfids.get(0));
                        if (datafile.getLocation() == null) {
                            throw new NotFoundException("Datafile not found");
                        }
                    }
                    if (dsids.size() != 0) {
                        icat.get(sessionId, "Dataset", dsids.get(0));
                    }
                    if (invids.size() != 0) {
                        icat.get(sessionId, "Investigation", invids.get(0));
                    }
                } catch (IcatException_Exception e) {
                    throw new NotFoundException(e.getMessage());
                }
            }
        } else {
            logger.debug("Slow computation for normal case");
            final DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
                    investigationIds, datasetIds, datafileIds, Returns.DATASETS_AND_DATAFILES);

            StringBuilder sb = new StringBuilder();
            int n = 0;
            for (DataFileInfo df : dataSelection.getDfInfo()) {
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

        if (logSet.contains(CallType.INFO)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", icat.getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                String body = baos.toString();
                transmitter.processMessage("getSize", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return size;
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

    private long getSizeFor(String sessionId, StringBuilder sb) throws InternalException {
        String query = "SELECT SUM(df.fileSize) from Datafile df WHERE df.id IN (" + sb.toString() + ") AND df.location IS NOT NULL";
        try {
            return (Long) icat.search(sessionId, query).get(0);
        } catch (IcatException_Exception e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        } catch (IndexOutOfBoundsException e) {
            return 0L;
        }
    }

    private long evalSizeFor(String sessionId, String where, StringBuilder sb) throws InternalException {
        String query = "SELECT SUM(df.fileSize) from Datafile df WHERE " + where + " IN (" + sb.toString() + ") AND df.location IS NOT NULL";
        logger.debug("icat query for size: {}", query);
        try {
            return (Long) icat.search(sessionId, query).get(0);
        } catch (IcatException_Exception e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        } catch (IndexOutOfBoundsException e) {
            return 0L;
        }
    }

    public String getStatus(String preparedId, String ip)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

        long start = System.currentTimeMillis();

        // Log and validate
        logger.info("New webservice request: getSize preparedId = '{}'", preparedId);
        validateUUID("preparedId", preparedId);

        // Do it
        Prepared prepared;
        try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
            prepared = unpack(stream);
        } catch (NoSuchFileException e) {
            throw new NotFoundException("The preparedId " + preparedId + " is not known");
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        final Set<DataFileInfo> dfInfos = prepared.dfInfos;
        final Map<Long, DataSetInfo> dsInfos = prepared.dsInfos;
        Set<Long> emptyDatasets = prepared.emptyDatasets;

        Status status = Status.ONLINE;

        if (storageUnit == StorageUnit.DATASET) {
            Set<DataInfoBase> restoring = fsm.getRestoring();
            Set<DataInfoBase> maybeOffline = fsm.getMaybeOffline();
            for (DataSetInfo dsInfo : dsInfos.values()) {
                fsm.checkFailure(dsInfo.getId());
                if (restoring.contains(dsInfo)) {
                    status = Status.RESTORING;
                } else if (maybeOffline.contains(dsInfo)) {
                    status = Status.ARCHIVED;
                    break;
                } else if (!emptyDatasets.contains(dsInfo.getId()) && !mainStorage.exists(dsInfo)) {
                    status = Status.ARCHIVED;
                    break;
                }
            }
        } else if (storageUnit == StorageUnit.DATAFILE) {
            Set<DataInfoBase> restoring = fsm.getRestoring();
            Set<DataInfoBase> maybeOffline = fsm.getMaybeOffline();
            for (DataFileInfo dfInfo : dfInfos) {
                fsm.checkFailure(dfInfo.getId());
                if (restoring.contains(dfInfo)) {
                    status = Status.RESTORING;
                } else if (maybeOffline.contains(dfInfo)) {
                    status = Status.ARCHIVED;
                    break;
                } else if (!mainStorage.exists(dfInfo.getDfLocation())) {
                    status = Status.ARCHIVED;
                    break;
                }
            }
        }

        logger.debug("Status is " + status.name());

        if (logSet.contains(CallType.INFO)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                gen.write("preparedId", preparedId);
                gen.writeEnd();
            }
            String body = baos.toString();
            transmitter.processMessage("getStatus", ip, body, start);
        }

        return status.name();

    }

    public String getStatus(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

        long start = System.currentTimeMillis();

        // Log and validate
        logger.info(
                String.format("New webservice request: getStatus investigationIds=%s, datasetIds=%s, datafileIds=%s",
                        investigationIds, datasetIds, datafileIds));

        if (sessionId == null) {
            try {
                sessionId = reader.getSessionId();
            } catch (IcatException_Exception e) {
                throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
            }
        } else {
            validateUUID("sessionId", sessionId);
        }

        // Do it
        Status status = Status.ONLINE;

        if (storageUnit == StorageUnit.DATASET) {
            DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
                    investigationIds, datasetIds, datafileIds, Returns.DATASETS);
            Map<Long, DataSetInfo> dsInfos = dataSelection.getDsInfo();

            Set<DataInfoBase> restoring = fsm.getRestoring();
            Set<DataInfoBase> maybeOffline = fsm.getMaybeOffline();
            Set<Long> emptyDatasets = dataSelection.getEmptyDatasets();
            for (DataSetInfo dsInfo : dsInfos.values()) {
                fsm.checkFailure(dsInfo.getId());
                if (restoring.contains(dsInfo)) {
                    status = Status.RESTORING;
                } else if (maybeOffline.contains(dsInfo)) {
                    status = Status.ARCHIVED;
                    break;
                } else if (!emptyDatasets.contains(dsInfo.getId()) && !mainStorage.exists(dsInfo)) {
                    status = Status.ARCHIVED;
                    break;
                }
            }
        } else if (storageUnit == StorageUnit.DATAFILE) {
            DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
                    investigationIds, datasetIds, datafileIds, Returns.DATAFILES);
            Set<DataFileInfo> dfInfos = dataSelection.getDfInfo();

            Set<DataInfoBase> restoring = fsm.getRestoring();
            Set<DataInfoBase> maybeOffline = fsm.getMaybeOffline();
            for (DataFileInfo dfInfo : dfInfos) {
                fsm.checkFailure(dfInfo.getId());
                if (restoring.contains(dfInfo)) {
                    status = Status.RESTORING;
                } else if (maybeOffline.contains(dfInfo)) {
                    status = Status.ARCHIVED;
                    break;
                } else if (!mainStorage.exists(dfInfo.getDfLocation())) {
                    status = Status.ARCHIVED;
                    break;
                }
            }
        } else {
            // Throw exception if selection does not exist
            new DataSelection(propertyHandler, reader, sessionId,
                    investigationIds, datasetIds, datafileIds, Returns.DATASETS);
        }

        logger.debug("Status is " + status.name());

        if (logSet.contains(CallType.INFO)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    if (sessionId != null) {
                        gen.write("userName", icat.getUserName(sessionId));
                    }
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                String body = baos.toString();
                transmitter.processMessage("getStatus", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return status.name();

    }

    @PostConstruct
    private void init() {
        try {
            synchronized (inited) {
                logger.info("creating IdsBean");

                this.fsm = FiniteStateMachine.getInstance();
                propertyHandler = PropertyHandler.getInstance();
                mainStorage = propertyHandler.getMainStorage();
                archiveStorage = propertyHandler.getArchiveStorage();
                twoLevel = archiveStorage != null;
                datatypeFactory = DatatypeFactory.newInstance();
                preparedDir = propertyHandler.getCacheDir().resolve("prepared");
                Files.createDirectories(preparedDir);

                rootUserNames = propertyHandler.getRootUserNames();
                readOnly = propertyHandler.getReadOnly();
                enableWrite = propertyHandler.getEnableWrite();

                icat = propertyHandler.getIcatService();

                if (!inited) {
                    key = propertyHandler.getKey();
                    logger.info("Key is " + (key == null ? "not set" : "set"));
                }

                if (twoLevel) {
                    storageUnit = propertyHandler.getStorageUnit();
                    datasetDir = propertyHandler.getCacheDir().resolve("dataset");
                    markerDir = propertyHandler.getCacheDir().resolve("marker");
                    if (!inited) {
                        Files.createDirectories(datasetDir);
                        Files.createDirectories(markerDir);
                        restartUnfinishedWork();
                    }
                }

                if (!inited) {
                    cleanPreparedDir(preparedDir);
                    if (twoLevel) {
                        cleanDatasetCache(datasetDir);
                    }
                }

                threadPool = Executors.newCachedThreadPool();

                logSet = propertyHandler.getLogSet();

                inited = true;

                logger.info("created IdsBean");
            }
        } catch (Throwable e) {
            logger.error("Won't start ", e);
            throw new RuntimeException("IdsBean reports " + e.getClass() + " " + e.getMessage());
        }
    }

    public Boolean isPrepared(String preparedId, String ip)
            throws BadRequestException, NotFoundException, InternalException {

        long start = System.currentTimeMillis();

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
            return false;
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
                    return false;
                }
            }

            if (storageUnit == StorageUnit.DATASET) {
                Collection<DataSetInfo> toCheck = status.fromDsElement == null ? preparedJson.dsInfos.values()
                        : preparedJson.dsInfos.tailMap(status.fromDsElement).values();
                logger.debug("Will check online status of {} entries", toCheck.size());
                for (DataSetInfo dsInfo : toCheck) {
                    fsm.checkFailure(dsInfo.getId());
                    if (DataSelection.restoreIfOffline(dsInfo, preparedJson.emptyDatasets)) {
                        prepared = false;
                        status.fromDsElement = dsInfo.getId();
                        toCheck = preparedJson.dsInfos.tailMap(status.fromDsElement).values();
                        logger.debug("Will check in background status of {} entries", toCheck.size());
                        status.future = threadPool.submit(new RunPrepDsCheck(toCheck, preparedJson.emptyDatasets));
                        break;
                    }
                }
                if (prepared) {
                    toCheck = status.fromDsElement == null ? Collections.emptySet()
                            : preparedJson.dsInfos.headMap(status.fromDsElement).values();
                    logger.debug("Will check finally online status of {} entries", toCheck.size());
                    for (DataSetInfo dsInfo : toCheck) {
                        fsm.checkFailure(dsInfo.getId());
                        if (DataSelection.restoreIfOffline(dsInfo, preparedJson.emptyDatasets)) {
                            prepared = false;
                        }
                    }
                }
            } else if (storageUnit == StorageUnit.DATAFILE) {
                SortedSet<DataFileInfo> toCheck = status.fromDfElement == null ? preparedJson.dfInfos
                        : preparedJson.dfInfos.tailSet(status.fromDfElement);
                logger.debug("Will check online status of {} entries", toCheck.size());
                for (DataFileInfo dfInfo : toCheck) {
                    fsm.checkFailure(dfInfo.getId());
                    if (DataSelection.restoreIfOffline(dfInfo)) {
                        prepared = false;
                        status.fromDfElement = dfInfo;
                        toCheck = preparedJson.dfInfos.tailSet(status.fromDfElement);
                        logger.debug("Will check in background status of {} entries", toCheck.size());
                        status.future = threadPool.submit(new RunPrepDfCheck(toCheck));
                        break;
                    }
                }
                if (prepared) {
                    toCheck = status.fromDfElement == null ? new TreeSet<>()
                            : preparedJson.dfInfos.headSet(status.fromDfElement);
                    logger.debug("Will check finally online status of {} entries", toCheck.size());
                    for (DataFileInfo dfInfo : toCheck) {
                        fsm.checkFailure(dfInfo.getId());
                        if (DataSelection.restoreIfOffline(dfInfo)) {
                            prepared = false;
                        }
                    }
                }
            }

            if (logSet.contains(CallType.INFO)) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("preparedId", preparedId);
                    gen.writeEnd();
                }
                String body = baos.toString();
                transmitter.processMessage("isPrepared", ip, body, start);
            }

            return prepared;

        } finally {
            status.lock.unlock();
        }

    }

    public boolean isReadOnly(String ip) {
        if (logSet.contains(CallType.INFO)) {
            transmitter.processMessage("isReadOnly", ip, "{}", System.currentTimeMillis());
        }
        return readOnly;
    }

    public boolean isTwoLevel(String ip) {
        if (logSet.contains(CallType.INFO)) {
            transmitter.processMessage("isTwoLevel", ip, "{}", System.currentTimeMillis());
        }
        return twoLevel;
    }

    public String prepareData(String sessionId, String investigationIds, String datasetIds, String datafileIds,
                              boolean compress, boolean zip, String ip)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException {

        long start = System.currentTimeMillis();

        // Log and validate
        logger.info("New webservice request: prepareData " + "investigationIds='" + investigationIds + "' "
                + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "' " + "compress='" + compress
                + "' " + "zip='" + zip + "'");

        validateUUID("sessionId", sessionId);

        final DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
                investigationIds, datasetIds, datafileIds, Returns.DATASETS_AND_DATAFILES);

        // Do it
        String preparedId = UUID.randomUUID().toString();

        Map<Long, DataSetInfo> dsInfos = dataSelection.getDsInfo();
        Set<Long> emptyDs = dataSelection.getEmptyDatasets();
        Set<DataFileInfo> dfInfos = dataSelection.getDfInfo();

        if (storageUnit == StorageUnit.DATASET) {
            for (DataSetInfo dsInfo : dsInfos.values()) {
                fsm.recordSuccess(dsInfo.getId());
            }
            threadPool.submit(new RestoreDsTask(dsInfos.values(), emptyDs));

        } else if (storageUnit == StorageUnit.DATAFILE) {
            for (DataFileInfo dfInfo : dfInfos) {
                fsm.recordSuccess(dfInfo.getId());
            }
            threadPool.submit(new RestoreDfTask(dfInfos));
        }

        if (dataSelection.mustZip()) {
            zip = true;
        }

        logger.debug("Writing to " + preparedDir.resolve(preparedId));
        try (OutputStream stream = new BufferedOutputStream(Files.newOutputStream(preparedDir.resolve(preparedId)))) {
            pack(stream, zip, compress, dsInfos, dfInfos, emptyDs);
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        logger.debug("preparedId is " + preparedId);

        if (logSet.contains(CallType.PREPARE)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", icat.getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.write("preparedId", preparedId);
                    gen.writeEnd();
                }
                String body = baos.toString();
                transmitter.processMessage("prepareData", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return preparedId;
    }

    public Response put(InputStream body, String sessionId, String name, String datafileFormatIdString,
                        String datasetIdString, String description, String doi, String datafileCreateTimeString,
                        String datafileModTimeString, boolean wrap, boolean padding, String ip)
            throws NotFoundException, DataNotOnlineException, BadRequestException, InsufficientPrivilegesException,
            InternalException, NotImplementedException {

        long start = System.currentTimeMillis();

        try {
            // Log and validate
            logger.info("New webservice request: put " + "name='" + name + "' " + "datafileFormatId='"
                    + datafileFormatIdString + "' " + "datasetId='" + datasetIdString + "' " + "description='"
                    + description + "' " + "doi='" + doi + "' " + "datafileCreateTime='" + datafileCreateTimeString
                    + "' " + "datafileModTime='" + datafileModTimeString + "'");

            if (readOnly) {
                throw new NotImplementedException("This operation has been configured to be unavailable");
            }

            IdsBean.validateUUID("sessionId", sessionId);
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
                ds = (Dataset) icat.get(sessionId, "Dataset INCLUDE Investigation, Facility", datasetId);
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
            try (Lock lock = lockManager.lock(dsInfo, LockType.SHARED)) {
                if (storageUnit == StorageUnit.DATASET) {
                    Set<DataFileInfo> dfInfos = Collections.emptySet();
                    Set<Long> emptyDatasets = new HashSet<>();
                    try {
                        List<Object> counts = icat.search(sessionId,
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
                    var dsInfos = new HashMap<Long, DataSetInfo>();
                    dsInfos.put(dsInfo.getId(), dsInfo);
                    DataSelection dataSelection = new DataSelection(dsInfos, dfInfos, emptyDatasets);
                    dataSelection.checkOnline();
                }

                CRC32 crc = new CRC32();
                CheckedWithSizeInputStream is = new CheckedWithSizeInputStream(body, crc);
                String location;
                try {
                    location = mainStorage.put(dsInfo, name, is);
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
                        userId = icat.getUserName(sessionId);
                    } catch (IcatException_Exception e1) {
                        logger.error("Unable to get user name for session " + sessionId + " so mainStorage.delete of "
                                + location + " may fail");
                    }
                    mainStorage.delete(location, userId, userId);
                    throw e;
                }

                if (storageUnit == StorageUnit.DATASET) {
                    fsm.queue(dsInfo, DeferredOp.WRITE);
                } else if (storageUnit == StorageUnit.DATAFILE) {
                    Datafile df;
                    try {
                        df = (Datafile) reader.get("Datafile", dfId);
                    } catch (IcatException_Exception e) {
                        throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
                    }
                    fsm.queue(new DataFileInfo(dfId, name, location, df.getCreateId(), df.getModId(), dsInfo.getId()),
                            DeferredOp.WRITE);
                }

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                Json.createGenerator(baos).writeStartObject().write("id", dfId).write("checksum", checksum)
                        .write("location", location.replace("\\", "\\\\").replace("'", "\\'")).write("size", size)
                        .writeEnd().close();
                String resp = wrap ? prefix + baos.toString() + suffix : baos.toString();

                if (logSet.contains(CallType.WRITE)) {
                    try {
                        baos = new ByteArrayOutputStream();
                        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                            gen.write("userName", icat.getUserName(sessionId));
                            gen.write("datafileId", dfId);
                            gen.writeEnd();
                        }
                        transmitter.processMessage("put", ip, baos.toString(), start);
                    } catch (IcatException_Exception e) {
                        logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
                    }
                }

                return Response.status(HttpURLConnection.HTTP_CREATED).entity(resp).build();

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
                return Response.status(e.getHttpStatusCode()).entity(pre + baos.toString().replace("'", "\\'") + suffix)
                        .build();
            } else {
                return Response.status(e.getHttpStatusCode()).entity(baos.toString()).build();
            }
        }

    }

    private Long registerDatafile(String sessionId, String name, long datafileFormatId, String location, long checksum,
                                  long size, Dataset dataset, String description, String doi, Long datafileCreateTime, Long datafileModTime)
            throws InsufficientPrivilegesException, NotFoundException, InternalException, BadRequestException {
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

    public void reset(String preparedId, String ip) throws BadRequestException, InternalException, NotFoundException {
        long start = System.currentTimeMillis();

        logger.info(String.format("New webservice request: reset preparedId=%s", preparedId));

        // Validate
        validateUUID("preparedId", preparedId);

        // Do it
        Prepared preparedJson;
        try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
            preparedJson = unpack(stream);
        } catch (NoSuchFileException e) {
            throw new NotFoundException("The preparedId " + preparedId + " is not known");
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        if (storageUnit == StorageUnit.DATASET) {
            for (DataSetInfo dsInfo : preparedJson.dsInfos.values()) {
                fsm.recordSuccess(dsInfo.getId());
            }
        } else if (storageUnit == StorageUnit.DATAFILE) {
            for (DataFileInfo dfInfo : preparedJson.dfInfos) {
                fsm.recordSuccess(dfInfo.getId());
            }
        }

        if (logSet.contains(CallType.MIGRATE)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                gen.write("preparedId", preparedId);
                gen.writeEnd();
            }
            String body = baos.toString();
            transmitter.processMessage("reset", ip, body, start);
        }
    }

    public void reset(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {
        long start = System.currentTimeMillis();

        // Log and validate
        logger.info("New webservice request: reset " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
                + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

        validateUUID("sessionId", sessionId);

        final DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
                investigationIds, datasetIds, datafileIds, Returns.DATASETS_AND_DATAFILES);

        // Do it
        if (storageUnit == StorageUnit.DATASET) {
            for (DataInfoBase dsInfo : dataSelection.getDsInfo().values()) {
                fsm.recordSuccess(dsInfo.getId());
            }
        } else if (storageUnit == StorageUnit.DATAFILE) {
            for (DataInfoBase dfInfo : dataSelection.getDfInfo()) {
                fsm.recordSuccess(dfInfo.getId());
            }
        }

        if (logSet.contains(CallType.MIGRATE)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", icat.getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                String body = baos.toString();
                transmitter.processMessage("reset", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }
    }

    private void restartUnfinishedWork() throws InternalException {

        try {
            for (File file : markerDir.toFile().listFiles()) {
                if (storageUnit == StorageUnit.DATASET) {
                    long dsid = Long.parseLong(file.toPath().getFileName().toString());
                    Dataset ds = null;
                    try {
                        ds = (Dataset) reader.get("Dataset ds INCLUDE ds.investigation.facility", dsid);
                        DataSetInfo dsInfo = new DataSetInfo(ds);
                        fsm.queue(dsInfo, DeferredOp.WRITE);
                        logger.info("Queued dataset with id " + dsid + " " + dsInfo
                                + " to be written as it was not written out previously by IDS");
                    } catch (IcatException_Exception e) {
                        if (e.getFaultInfo().getType() == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
                            logger.warn("Dataset with id " + dsid
                                    + " was not written out by IDS and now no longer known to ICAT");
                            Files.delete(file.toPath());
                        } else {
                            throw e;
                        }
                    }
                } else if (storageUnit == StorageUnit.DATAFILE) {
                    long dfid = Long.parseLong(file.toPath().getFileName().toString());
                    Datafile df = null;
                    try {
                        df = (Datafile) reader.get("Datafile ds INCLUDE ds.dataset", dfid);
                        String location = getLocation(df.getId(), df.getLocation());
                        DataFileInfo dfInfo = new DataFileInfo(dfid, df.getName(), location, df.getCreateId(),
                                df.getModId(), df.getDataset().getId());
                        fsm.queue(dfInfo, DeferredOp.WRITE);
                        logger.info("Queued datafile with id " + dfid + " " + dfInfo
                                + " to be written as it was not written out previously by IDS");
                    } catch (IcatException_Exception e) {
                        if (e.getFaultInfo().getType() == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
                            logger.warn("Datafile with id " + dfid
                                    + " was not written out by IDS and now no longer known to ICAT");
                            Files.delete(file.toPath());
                        } else {
                            throw e;
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }
    }

    public void restore(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
            throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, InternalException,
            NotFoundException {

        long start = System.currentTimeMillis();

        // Log and validate
        logger.info("New webservice request: restore " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
                + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

        validateUUID("sessionId", sessionId);

        // Do it
        if (storageUnit == StorageUnit.DATASET) {
            DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
                    investigationIds, datasetIds, datafileIds, Returns.DATASETS);
            Map<Long, DataSetInfo> dsInfos = dataSelection.getDsInfo();
            for (DataSetInfo dsInfo : dsInfos.values()) {
                fsm.queue(dsInfo, DeferredOp.RESTORE);
            }
        } else if (storageUnit == StorageUnit.DATAFILE) {
            DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
                    investigationIds, datasetIds, datafileIds, Returns.DATAFILES);
            Set<DataFileInfo> dfInfos = dataSelection.getDfInfo();
            for (DataFileInfo dfInfo : dfInfos) {
                fsm.queue(dfInfo, DeferredOp.RESTORE);
            }
        } else {
            throw new NotImplementedException("This operation is unavailable for single level storage");
        }

        if (logSet.contains(CallType.MIGRATE)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", icat.getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                transmitter.processMessage("restore", ip, baos.toString(), start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }
    }

    public void write(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
            throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, InternalException,
            NotFoundException, DataNotOnlineException {

        long start = System.currentTimeMillis();

        // Log and validate
        logger.info("New webservice request: write " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
                + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

        if (!enableWrite) {
            throw new NotImplementedException("This operation has been configured to be unavailable");
        }

        validateUUID("sessionId", sessionId);

        final DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
                investigationIds, datasetIds, datafileIds, Returns.DATASETS_AND_DATAFILES);

        // Do it
        Map<Long, DataSetInfo> dsInfos = dataSelection.getDsInfo();
        Set<DataFileInfo> dfInfos = dataSelection.getDfInfo();

        try (Lock lock = lockManager.lock(dsInfos.values(), LockType.SHARED)) {
            if (twoLevel) {
                boolean maybeOffline = false;
                if (storageUnit == StorageUnit.DATASET) {
                    for (DataSetInfo dsInfo : dsInfos.values()) {
                        if (!dataSelection.getEmptyDatasets().contains(dsInfo.getId()) &&
                                !mainStorage.exists(dsInfo)) {
                            maybeOffline = true;
                        }
                    }
                } else if (storageUnit == StorageUnit.DATAFILE) {
                    for (DataFileInfo dfInfo : dfInfos) {
                        if (!mainStorage.exists(dfInfo.getDfLocation())) {
                            maybeOffline = true;
                        }
                    }
                }
                if (maybeOffline) {
                    throw new DataNotOnlineException("Requested data is not online, write request refused");
                }
            }

            if (storageUnit == StorageUnit.DATASET) {
                for (DataSetInfo dsInfo : dsInfos.values()) {
                    fsm.queue(dsInfo, DeferredOp.WRITE);
                }
            } else if (storageUnit == StorageUnit.DATAFILE) {
                for (DataFileInfo dfInfo : dfInfos) {
                    fsm.queue(dfInfo, DeferredOp.WRITE);
                }
            } else {
                throw new NotImplementedException("This operation is unavailable for single level storage");
            }
        } catch (AlreadyLockedException e) {
            logger.debug("Could not acquire lock, write failed");
            throw new DataNotOnlineException("Data is busy");
        } catch (IOException e) {
            logger.error("I/O error " + e.getMessage() + " writing");
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        if (logSet.contains(CallType.MIGRATE)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", icat.getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                transmitter.processMessage("write", ip, baos.toString(), start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }
    }
}
