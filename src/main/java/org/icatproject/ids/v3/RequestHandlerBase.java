package org.icatproject.ids.v3;

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

public abstract class RequestHandlerBase {

    private List<StorageUnit> supportedStorageUnits;
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

    public DataSelectionBase getDataSelection(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos, Set<Long> emptyDatasets) throws InternalException {

        return DataSelectionFactory.get(dsInfos, dfInfos, emptyDatasets, this.getRequestType());
    }

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

    public abstract ValueContainer handle(HashMap<String, ValueContainer> parameters) throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException;


    public static void validateUUID(String thing, String id) throws BadRequestException {
        if (id == null || !uuidRegExp.matcher(id).matches())
            throw new BadRequestException("The " + thing + " parameter '" + id + "' is not a valid UUID");
    }

    public static void pack(OutputStream stream, boolean zip, boolean compress, Map<Long, DataInfoBase> dsInfos,
                        Map<Long, DataInfoBase> dfInfos, Set<Long> emptyDatasets) {
        JsonGenerator gen = Json.createGenerator(stream);
        gen.writeStartObject();
        gen.write("zip", zip);
        gen.write("compress", compress);

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