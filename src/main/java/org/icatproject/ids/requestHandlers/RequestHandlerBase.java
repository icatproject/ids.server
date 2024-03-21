package org.icatproject.ids.requestHandlers;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.json.Json;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonGenerator;

import org.icatproject.ids.dataSelection.DataSelectionFactory;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.OperationIdTypes;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.enums.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.DataFileInfo;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.models.DataSetInfo;
import org.icatproject.ids.models.Prepared;
import org.icatproject.ids.services.ServiceProvider;


/**
 * This base class represents all common properties and methods which are needed by each request handler.
 * Request handlers schould be added to the internal request handler list in RequestHandlerService, to be able to be called.
 */
public abstract class RequestHandlerBase {

    /**
     * This List contains the possible StorageUnit values (don't forget the null here) a handler is able to work with.
     * At the moment (13th of March in 2024) each handler is able to deal with all possible StorageUnitValues so maybe this could be removed. But maybe we need this in future.
     */
    private List<OperationIdTypes> supportedOperationIdTypes;

    protected final static Logger logger = LoggerFactory.getLogger(RequestHandlerBase.class);
    protected Path preparedDir;
    protected boolean twoLevel;
    protected StorageUnit storageUnit;
    protected RequestType requestType;
    protected boolean readOnly;

    /**
     * matches standard UUID format of 8-4-4-4-12 hexadecimal digits
     */
    public static final Pattern uuidRegExp = Pattern
            .compile("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$");


    protected RequestHandlerBase(OperationIdTypes supportedDataStatus, RequestType requestType ) {
        this(new OperationIdTypes[] {supportedDataStatus}, requestType);
    }

    protected RequestHandlerBase(OperationIdTypes[] supportedOperationIdTypes, RequestType requestType ) {
        this.supportedOperationIdTypes = Arrays.asList(supportedOperationIdTypes);
        this.requestType = requestType;
    }


    /**
     * Informs about if the request handler is able to work on the defined PrepareDataStatus
     * @param neededOperationIdTypes
     * @return
     */
    public boolean supportsOperationIdType(OperationIdTypes supportedOperationIdType) {
        return this.supportedOperationIdTypes.contains(supportedOperationIdType);
    }


    /**
     * Informs about the RequestType the handler ist providing a handling for.
     * @return
     */
    public RequestType getRequestType() {
        return this.requestType;
    }


    /**
     * returns the supported OperationIdTypes
     * @return
     */
    public List<OperationIdTypes> getSupportedOperationIdTypes() {
        return this.supportedOperationIdTypes;
    }


    /**
     * Creates a DataSelection depending on the RequestType. It Uses the DataSelectionFactory which is creating the DataSelection depending on the configured StorageUnit.
     * @param dsInfos A ready to use Map of DataSetInfos
     * @param dfInfos A ready to use Map of DataFileInfos
     * @param emptyDatasets A list of data set IDs of empty data sets
     * @return
     * @throws InternalException
     */
    public DataSelectionBase getDataSelection(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos, Set<Long> emptyDatasets, long fileLength) throws InternalException {

        return DataSelectionFactory.get(dsInfos, dfInfos, emptyDatasets, fileLength, this.getRequestType());
    }


    /**
     * provides a suitable DataSelection depending on the RequestType. It Uses the DataSelectionFactory which is creating the DataSelection depending on the configured StorageUnit.
     * @param userSessionId The current session id
     * @param investigationIds A String which contains investigation IDs
     * @param datasetIds A String which contains data set IDs
     * @param datafileIds A String which contains data file IDs
     * @return
     * @throws InternalException
     * @throws BadRequestException
     * @throws NotFoundException
     * @throws InsufficientPrivilegesException
     * @throws NotImplementedException
     */
    public DataSelectionBase getDataSelection(String userSessionId, String investigationIds, String datasetIds, String datafileIds) 
                                    throws InternalException, BadRequestException, NotFoundException, InsufficientPrivilegesException, NotImplementedException {

        return DataSelectionFactory.get(userSessionId, investigationIds, datasetIds, datafileIds, this.getRequestType());
    }


    /**
     * This method initializes the base class part of the RequestHandler.
     * You can overload it, but please don't overwrite it, because this base class part has also to be initialized
     * @throws InternalException
     */
    public void init() throws InternalException {

        //logger.info("Initialize RequestHandlerBase...");

        var serviceProvider = ServiceProvider.getInstance();
        var propertyHandler = serviceProvider.getPropertyHandler();
        this.preparedDir = propertyHandler.getCacheDir().resolve("prepared");

        this.storageUnit = propertyHandler.getStorageUnit();

        var archiveStorage = propertyHandler.getArchiveStorage();
        this.twoLevel = archiveStorage != null;

        this.readOnly = propertyHandler.getReadOnly();

        //logger.info("RequestHandlerBase initialized");
    }


    /**
     * The core method of each request handler. It has to be overwritten in the concrete implementation to provide an individual request handling
     * @param parameters A Map of parameters which where extracted from th incoming request and maybe more.
     * @return A ValueContainer with an indiviadual result type.
     * @throws BadRequestException
     * @throws InternalException
     * @throws InsufficientPrivilegesException
     * @throws NotFoundException
     * @throws DataNotOnlineException
     * @throws NotImplementedException
     */
    public abstract ValueContainer handle(HashMap<String, ValueContainer> parameters) throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException;


    /**
     * Provides a validity check for UUIDs
     * @param thing You can give here a name of the prameter or whatever has been checked here (to provide a qualified error message if needed).
     * @param id The String which has to be checked if it is a valid UUID
     * @throws BadRequestException
     */
    public static void validateUUID(String thing, String id) throws BadRequestException {
        if (id == null || !uuidRegExp.matcher(id).matches())
            throw new BadRequestException("The " + thing + " parameter '" + id + "' is not a valid UUID");
    }


    public static void pack(OutputStream stream, boolean zip, boolean compress, Map<Long, DataInfoBase> dsInfos,
                        Map<Long, DataInfoBase> dfInfos, Set<Long> emptyDatasets, long fileLength) {
        JsonGenerator gen = Json.createGenerator(stream);
        gen.writeStartObject();
        gen.write("zip", zip);
        gen.write("compress", compress);
        gen.write("fileLength", fileLength);

        gen.writeStartArray("dsInfo");
        for (DataInfoBase dataInfo : dsInfos.values()) {
            var dsInfo = (DataSetInfo)dataInfo;
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
        for (DataInfoBase dataInfo : dfInfos.values()) {
            var dfInfo = (DataFileInfo)dataInfo;
            DataInfoBase dsInfo = dsInfos.get(dfInfo.getDsId());
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


    public static Prepared unpack(InputStream stream) throws InternalException {
        Prepared prepared = new Prepared();
        JsonObject pd;
        try (JsonReader jsonReader = Json.createReader(stream)) {
            pd = jsonReader.readObject();
        }
        prepared.zip = pd.getBoolean("zip");
        prepared.compress = pd.getBoolean("compress");
        prepared.fileLength = pd.containsKey("fileLength") ? pd.getInt("fileLength") : 0;
        SortedMap<Long, DataInfoBase> dsInfos = new TreeMap<>();
        SortedMap<Long, DataInfoBase> dfInfos = new TreeMap<>();
        Set<Long> emptyDatasets = new HashSet<>();

        for (JsonValue itemV : pd.getJsonArray("dfInfo")) {
            JsonObject item = (JsonObject) itemV;
            String dfLocation = item.isNull("dfLocation") ? null : item.getString("dfLocation");
            long dfid = item.getJsonNumber("dfId").longValueExact();
            dfInfos.put(dfid, new DataFileInfo(dfid, item.getString("dfName"),
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


    protected void addIds(JsonGenerator gen, String investigationIds, String datasetIds, String datafileIds)
            throws BadRequestException {
        if (investigationIds != null) {
            gen.writeStartArray("investigationIds");
            for (long invid : DataSelectionBase.getValidIds("investigationIds", investigationIds)) {
                gen.write(invid);
            }
            gen.writeEnd();
        }
        if (datasetIds != null) {
            gen.writeStartArray("datasetIds");
            for (long invid : DataSelectionBase.getValidIds("datasetIds", datasetIds)) {
                gen.write(invid);
            }
            gen.writeEnd();
        }
        if (datafileIds != null) {
            gen.writeStartArray("datafileIds");
            for (long invid : DataSelectionBase.getValidIds("datafileIds", datafileIds)) {
                gen.write(invid);
            }
            gen.writeEnd();
        }
    }

}