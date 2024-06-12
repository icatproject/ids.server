package org.icatproject.ids.models;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.requestHandlers.base.RequestHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.json.Json;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonGenerator;

/* This is a POJO with only package access so don't make data private */
public class Prepared {

    protected final static Logger logger = LoggerFactory
            .getLogger(RequestHandlerBase.class);

    public SortedMap<Long, DataInfoBase> dsInfos;
    public SortedMap<Long, DataInfoBase> dfInfos;
    public Set<Long> emptyDatasets;
    public long fileLength;
    public boolean zip;
    public boolean compress;

    public Prepared(SortedMap<Long, DataInfoBase> dsInfos,
            SortedMap<Long, DataInfoBase> dfInfos, Set<Long> emptyDatasets,
            long fileLength) {
        this.dsInfos = dsInfos;
        this.dfInfos = dfInfos;
        this.emptyDatasets = emptyDatasets;
        this.fileLength = fileLength;
        this.zip = false;
        this.compress = false;
    }

    public Prepared(SortedMap<Long, DataInfoBase> dsInfos,
            SortedMap<Long, DataInfoBase> dfInfos, Set<Long> emptyDatasets,
            long fileLength, Boolean zip, Boolean compress) {
        this(dsInfos, dfInfos, emptyDatasets, fileLength);
        this.zip = zip;
        this.compress = compress;
    }

    public static void pack(OutputStream stream, boolean zip, boolean compress,
            Map<Long, DataInfoBase> dsInfos, Map<Long, DataInfoBase> dfInfos,
            Set<Long> emptyDatasets, long fileLength) {
        JsonGenerator gen = Json.createGenerator(stream);
        gen.writeStartObject();
        gen.write("zip", zip);
        gen.write("compress", compress);
        gen.write("fileLength", fileLength);

        gen.writeStartArray("dsInfo");
        for (DataInfoBase dataInfo : dsInfos.values()) {
            var dsInfo = (DatasetInfo) dataInfo;
            logger.debug("dsInfo " + dsInfo);
            gen.writeStartObject().write("dsId", dsInfo.getId())

                    .write("dsName", dsInfo.getDsName())
                    .write("facilityId", dsInfo.getFacilityId())
                    .write("facilityName", dsInfo.getFacilityName())
                    .write("invId", dsInfo.getInvId())
                    .write("invName", dsInfo.getInvName())
                    .write("visitId", dsInfo.getVisitId());
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
            var dfInfo = (DatafileInfo) dataInfo;
            DataInfoBase dsInfo = dsInfos.get(dfInfo.getDsId());
            gen.writeStartObject().write("dsId", dsInfo.getId())
                    .write("dfId", dfInfo.getId())
                    .write("dfName", dfInfo.getDfName())
                    .write("createId", dfInfo.getCreateId())
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

        JsonObject pd;
        try (JsonReader jsonReader = Json.createReader(stream)) {
            pd = jsonReader.readObject();
        }
        var zip = pd.getBoolean("zip");
        var compress = pd.getBoolean("compress");
        var fileLength = pd.containsKey("fileLength") ? pd.getInt("fileLength")
                : 0;
        SortedMap<Long, DataInfoBase> dsInfos = new TreeMap<>();
        SortedMap<Long, DataInfoBase> dfInfos = new TreeMap<>();
        Set<Long> emptyDatasets = new HashSet<>();

        for (JsonValue itemV : pd.getJsonArray("dfInfo")) {
            JsonObject item = (JsonObject) itemV;
            String dfLocation = item.isNull("dfLocation") ? null
                    : item.getString("dfLocation");
            long dfid = item.getJsonNumber("dfId").longValueExact();
            dfInfos.put(dfid,
                    new DatafileInfo(dfid, item.getString("dfName"), dfLocation,
                            item.getString("createId"), item.getString("modId"),
                            item.getJsonNumber("dsId").longValueExact()));

        }

        for (JsonValue itemV : pd.getJsonArray("dsInfo")) {
            JsonObject item = (JsonObject) itemV;
            long dsId = item.getJsonNumber("dsId").longValueExact();
            String dsLocation = item.isNull("dsLocation") ? null
                    : item.getString("dsLocation");
            dsInfos.put(dsId, new DatasetInfo(dsId, item.getString("dsName"),
                    dsLocation, item.getJsonNumber("invId").longValueExact(),
                    item.getString("invName"), item.getString("visitId"),
                    item.getJsonNumber("facilityId").longValueExact(),
                    item.getString("facilityName")));
        }

        for (JsonValue itemV : pd.getJsonArray("emptyDs")) {
            emptyDatasets.add(((JsonNumber) itemV).longValueExact());
        }

        return new Prepared(dsInfos, dfInfos, emptyDatasets, fileLength, zip,
                compress);
    }

}
