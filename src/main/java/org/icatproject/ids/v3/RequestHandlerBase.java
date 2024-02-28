package org.icatproject.ids.v3;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.models.DataFileInfo;
import org.icatproject.ids.v3.models.DataInfoBase;
import org.icatproject.ids.v3.models.DataSetInfo;
import org.icatproject.ids.v3.models.ValueContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.json.Json;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonGenerator;

import org.icatproject.ids.DataSelection;
import org.icatproject.ids.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;

public abstract class RequestHandlerBase {

    private List<StorageUnit> supportedStorageUnits;
    protected final static Logger logger = LoggerFactory.getLogger(RequestHandlerBase.class);
    protected Path preparedDir;
    protected boolean twoLevel;
    protected StorageUnit storageUnit;
    protected RequestType requestType;

    /**
     * matches standard UUID format of 8-4-4-4-12 hexadecimal digits
     */
    public static final Pattern uuidRegExp = Pattern
            .compile("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$");

    protected RequestHandlerBase(StorageUnit[] supportedStorageUnitsArray, RequestType requestType ) {
        this.supportedStorageUnits = Arrays.asList(supportedStorageUnitsArray);
        this.requestType = requestType;
    }

    protected RequestHandlerBase(StorageUnit supportedStorageUnit, RequestType requestType) {
        this(new StorageUnit[]{supportedStorageUnit}, requestType);
    }

    public boolean supportsStorageUnit(StorageUnit neededStorageUnit) {
        return this.supportedStorageUnits.contains(neededStorageUnit);
    }

    public RequestType getRequestType() {
        return this.requestType;
    }

    public DataSelectionV3Base getDataSelection(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos, Set<Long> emptyDatasets) throws InternalException {

        return DataSelectionFactory.get(dsInfos, dfInfos, emptyDatasets, this.getRequestType());
    }

    public DataSelectionV3Base getDataSelection(String userSessionId, String investigationIds, String datasetIds, String datafileIds) 
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

        //logger.info("RequestHandlerBase initialized");
    }

    public abstract ValueContainer handle(HashMap<String, ValueContainer> parameters) throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException;


    protected static void validateUUID(String thing, String id) throws BadRequestException {
        if (id == null || !uuidRegExp.matcher(id).matches())
            throw new BadRequestException("The " + thing + " parameter '" + id + "' is not a valid UUID");
    }

    protected static PreparedV3 unpack(InputStream stream) throws InternalException {
        PreparedV3 prepared = new PreparedV3();
        JsonObject pd;
        try (JsonReader jsonReader = Json.createReader(stream)) {
            pd = jsonReader.readObject();
        }
        prepared.zip = pd.getBoolean("zip");
        prepared.compress = pd.getBoolean("compress");
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

}