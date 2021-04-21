package org.icatproject.ids;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.json.Json;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.stream.JsonGenerator;

import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.plugin.DsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to handle the reading and writing of "prepared" files which are 
 * created when a prepareData call to the IDS is made, and then can be read 
 * back whenever a request comes in specifying the preparedId, as a 
 * reference of what the parameters of the original request were.
 * All prepared files are held in a single folder containing only prepared
 * files and the files are given the name of the preparedId.
 */
public class PreparedFilesManager {

	private static Logger logger = LoggerFactory.getLogger(PreparedFilesManager.class);
    private Path preparedDir;

    public PreparedFilesManager(Path preparedDir) {
        this.preparedDir = preparedDir;
    }
    
	public void pack(String preparedId, boolean zip, boolean compress, Map<Long, DsInfo> dsInfos,
			Set<DfInfoImpl> dfInfos, Set<Long> emptyDatasets) throws InternalException {
        Path preparedFilePath = preparedDir.resolve(preparedId);
        logger.debug("Writing to prepared file: {}", preparedFilePath);
        JsonGenerator gen = null;
        try (OutputStream stream = new BufferedOutputStream(Files.newOutputStream(preparedFilePath))) {
            gen = Json.createGenerator(stream);
            gen.writeStartObject();
            gen.write("zip", zip);
            gen.write("compress", compress);

            gen.writeStartArray("dsInfo");
            for (DsInfo dsInfo : dsInfos.values()) {
                logger.debug("dsInfo: {}", dsInfo);
                gen.writeStartObject().write("dsId", dsInfo.getDsId())
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
            for (DfInfoImpl dfInfo : dfInfos) {
                DsInfo dsInfo = dsInfos.get(dfInfo.getDsId());
                gen.writeStartObject().write("dsId", dsInfo.getDsId()).write("dfId", dfInfo.getDfId())
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
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        } finally {
            if (gen != null) {
                gen.close();
            }
        }
	}

	public Prepared unpack(String preparedId) throws InternalException, NotFoundException {
		try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
            Prepared prepared = new Prepared();
            JsonObject pd;
            try (JsonReader jsonReader = Json.createReader(stream)) {
                pd = jsonReader.readObject();
            }
            prepared.zip = pd.getBoolean("zip");
            prepared.compress = pd.getBoolean("compress");
            SortedMap<Long, DsInfo> dsInfos = new TreeMap<>();
            SortedSet<DfInfoImpl> dfInfos = new TreeSet<>();
            Set<Long> emptyDatasets = new HashSet<>();

            for (JsonValue itemV : pd.getJsonArray("dfInfo")) {
                JsonObject item = (JsonObject) itemV;
                String dfLocation = item.isNull("dfLocation") ? null : item.getString("dfLocation");
                dfInfos.add(new DfInfoImpl(item.getJsonNumber("dfId").longValueExact(), item.getString("dfName"),
                        dfLocation, item.getString("createId"), item.getString("modId"),
                        item.getJsonNumber("dsId").longValueExact()));

            }
            prepared.dfInfos = dfInfos;

            for (JsonValue itemV : pd.getJsonArray("dsInfo")) {
                JsonObject item = (JsonObject) itemV;
                long dsId = item.getJsonNumber("dsId").longValueExact();
                String dsLocation = item.isNull("dsLocation") ? null : item.getString("dsLocation");
                dsInfos.put(dsId, new DsInfoImpl(dsId, item.getString("dsName"), dsLocation,
                        item.getJsonNumber("invId").longValueExact(), item.getString("invName"), item.getString("visitId"),
                        item.getJsonNumber("facilityId").longValueExact(), item.getString("facilityName")));
            }
            prepared.dsInfos = dsInfos;

            for (JsonValue itemV : pd.getJsonArray("emptyDs")) {
                emptyDatasets.add(((JsonNumber) itemV).longValueExact());
            }
            prepared.emptyDatasets = emptyDatasets;

            return prepared;
		} catch (NoSuchFileException e) {
			throw new NotFoundException("The preparedId " + preparedId + " is not known");
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

}
