package org.icatproject.ids.v3;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.icat.client.IcatException;
import org.icatproject.icat.client.Session;
import org.icatproject.ids.IcatReader;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.helper.LocationHelper;
import org.icatproject.ids.v3.models.DataFileInfo;
import org.icatproject.ids.v3.models.DataInfoBase;
import org.icatproject.ids.v3.models.DataSetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonValue;

public class DataSelectionFactory {

    private final static Logger logger = LoggerFactory.getLogger(DataSelectionFactory.class);

    private static DataSelectionFactory instance = null;

    private PropertyHandler propertyHandler;
    private ICAT icat;
    private IcatReader icatReader;
    private org.icatproject.icat.client.ICAT restIcat;
    private int maxEntities;
    private HashMap<RequestType, Returns> requestTypeToReturnsMapping;

    public enum Returns {
        DATASETS, DATASETS_AND_DATAFILES, DATAFILES
    }

    public static DataSelectionFactory getInstance() throws InternalException {
        if (instance == null) {
            var serviceProvider = ServiceProvider.getInstance();
            instance = new DataSelectionFactory(serviceProvider.getPropertyHandler(), serviceProvider.getIcatReader());
        }
        return instance;
    }

    public static DataSelectionFactory getInstanceOnlyForTesting(PropertyHandler propertyHandler, IcatReader reader) throws InternalException {
        if (instance == null) {
            instance = new DataSelectionFactory(propertyHandler, reader);
        }
        return instance;
    }

    public static DataSelectionV3Base get(String userSessionId, String investigationIds, String datasetIds, String datafileIds, RequestType requestType) 
                                            throws InternalException, BadRequestException, NotFoundException, InsufficientPrivilegesException, NotImplementedException {

        return DataSelectionFactory.getInstance().getSelection(userSessionId, investigationIds, datasetIds, datafileIds, requestType);
    }


    
    public static DataSelectionV3Base get(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos, Set<Long> emptyDatasets, RequestType requestType) throws InternalException {
        List<Long> dsids = new ArrayList<Long>(dsInfos.keySet());
        List<Long> dfids = new ArrayList<Long>();
        var dataFileInfos = new HashMap<Long, DataInfoBase>();
        for(DataInfoBase dfInfo: dfInfos.values()) {
            dfids.add(dfInfo.getId());
            dataFileInfos.put(dfInfo.getId(), dfInfo);
        }
        return DataSelectionFactory.getInstance().createSelection(dsInfos, dfInfos, emptyDatasets, new ArrayList<Long>(), dsids, dfids, requestType);
    }

    private DataSelectionFactory(PropertyHandler propertyHandler, IcatReader reader) throws InternalException
    {
        logger.info("### Constructing...");
        this.propertyHandler = propertyHandler;
        this.icat = propertyHandler.getIcatService();
        this.icatReader = reader;
        this.restIcat = propertyHandler.getRestIcat();
        this.maxEntities = propertyHandler.getMaxEntities();

        this.createRequestTypeToReturnsMapping();

        logger.info("### Constructing finished");
    }

    private DataSelectionV3Base createSelection(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos, Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids, RequestType requestType) throws InternalException {

        StorageUnit storageUnit = this.propertyHandler.getStorageUnit();

        if(storageUnit == null )
            return new DataSelectionForSingleLevelStorage(dsInfos, dfInfos, emptyDatasets, invids2, dsids, dfids, requestType);

        else if (storageUnit == StorageUnit.DATAFILE)
            return new DataSelectionForStorageUnitDatafile(dsInfos, dfInfos, emptyDatasets, invids2, dsids, dfids, requestType);

        else if(storageUnit == StorageUnit.DATASET)
            return new DataSelectionForStorageUnitDataset(dsInfos, dfInfos, emptyDatasets, invids2, dsids, dfids, requestType);

        else throw new InternalException("StorageUnit " + storageUnit + " unknown. Maybe you forgot to handle a new StorageUnit here?");

    }

    public DataSelectionV3Base getSelection( String userSessionId, String investigationIds, String datasetIds, String datafileIds, RequestType requestType) 
                                    throws InternalException, BadRequestException, NotFoundException, InsufficientPrivilegesException, NotImplementedException {
        
        List<Long> dfids = getValidIds("datafileIds", datafileIds);
        List<Long> dsids = getValidIds("datasetIds", datasetIds);
        List<Long> invids = getValidIds("investigationIds", investigationIds);

        Returns returns = this.getReturns(requestType);
        boolean dfWanted = returns == Returns.DATASETS_AND_DATAFILES || returns == Returns.DATAFILES;
        boolean dsWanted = returns == Returns.DATASETS_AND_DATAFILES || returns == Returns.DATASETS;

        Session userRestSession = restIcat.getSession(userSessionId);
        // by default use the user's REST ICAT session
        Session restSessionToUse = userRestSession;
        
        try {
            logger.debug("useReaderForPerformance = {}", propertyHandler.getUseReaderForPerformance());
            if (propertyHandler.getUseReaderForPerformance()) {
                // if this is set, use a REST session for the reader account where possible
                // to improve performance due to the final database queries being simpler
                restSessionToUse = restIcat.getSession(this.icatReader.getSessionId());
            }
        } catch (IcatException_Exception e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        logger.debug("dfids: {} dsids: {} invids: {}", dfids, dsids, invids);

        return prepareFromIds(dfWanted, dsWanted, dfids, dsids, invids, userSessionId, restSessionToUse, userRestSession, requestType);
    }

    

    private DataSelectionV3Base prepareFromIds(boolean dfWanted, boolean dsWanted, List<Long> dfids, List<Long> dsids, List<Long> invids, String userSessionId, Session restSessionToUse, Session userRestSession, RequestType requestType)
            throws NotFoundException, InsufficientPrivilegesException, InternalException, BadRequestException {
        var dsInfos = new TreeMap<Long, DataInfoBase>();
        var emptyDatasets = new HashSet<Long>();
        var dfInfos = new TreeMap<Long, DataInfoBase>();
        if (dfWanted) { //redundant ?
            dfInfos = new TreeMap<Long, DataInfoBase>();
        }

        try {

            for (Long dfid : dfids) {
                List<Object> dss = icat.search(userSessionId,
                        "SELECT ds FROM Dataset ds JOIN ds.datafiles df WHERE df.id = " + dfid
                                + " AND df.location IS NOT NULL INCLUDE ds.investigation.facility");
                if (dss.size() == 1) {
                    Dataset ds = (Dataset) dss.get(0);
                    long dsid = ds.getId();
                    dsInfos.put(dsid, new DataSetInfo(ds));
                    if (dfWanted) {
                        Datafile df = (Datafile) icat.get(userSessionId, "Datafile", dfid);
                        String location = LocationHelper.getLocation(dfid, df.getLocation());
                        dfInfos.put( df.getId(),
                                new DataFileInfo(dfid, df.getName(), location, df.getCreateId(), df.getModId(), dsid));
                    }
                } else {
                    // Next line may reveal a permissions problem
                    icat.get(userSessionId, "Datafile", dfid);
                    throw new NotFoundException("Datafile " + dfid);
                }
            }

            for (Long dsid : dsids) {
                Dataset ds = (Dataset) icat.get(userSessionId, "Dataset ds INCLUDE ds.investigation.facility", dsid);
                dsInfos.put(dsid, new DataSetInfo(ds));
                // dataset access for the user has been checked so the REST session for the
                // reader account can be used if the IDS setting to allow this is enabled
                String query = "SELECT min(df.id), max(df.id), count(df.id) FROM Datafile df WHERE df.dataset.id = "
                        + dsid + " AND df.location IS NOT NULL";
                JsonArray result = Json.createReader(new ByteArrayInputStream(restSessionToUse.search(query).getBytes()))
                        .readArray().getJsonArray(0);
                if (result.getJsonNumber(2).longValueExact() == 0) { // Count 0
                    emptyDatasets.add(dsid);
                } else if (dfWanted) {
                    manyDfs(dfInfos, dsid, restSessionToUse, result);
                }
            }

            for (Long invid : invids) {
                String query = "SELECT min(ds.id), max(ds.id), count(ds.id) FROM Dataset ds WHERE ds.investigation.id = "
                        + invid;
                JsonArray result = Json.createReader(new ByteArrayInputStream(userRestSession.search(query).getBytes()))
                        .readArray().getJsonArray(0);
                manyDss(dsInfos, emptyDatasets, dfInfos, invid, dfWanted, userRestSession, restSessionToUse, result);

            }

        } catch (IcatException_Exception e) {
            IcatExceptionType type = e.getFaultInfo().getType();
            if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES || type == IcatExceptionType.SESSION) {
                throw new InsufficientPrivilegesException(e.getMessage());
            } else if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
                throw new NotFoundException(e.getMessage());
            } else {
                throw new InternalException(e.getClass() + " " + e.getMessage());
            }

        } catch (IcatException e) {
            org.icatproject.icat.client.IcatException.IcatExceptionType type = e.getType();
            if (type == org.icatproject.icat.client.IcatException.IcatExceptionType.INSUFFICIENT_PRIVILEGES
                    || type == org.icatproject.icat.client.IcatException.IcatExceptionType.SESSION) {
                throw new InsufficientPrivilegesException(e.getMessage());
            } else if (type == org.icatproject.icat.client.IcatException.IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
                throw new NotFoundException(e.getMessage());
            } else {
                throw new InternalException(e.getClass() + " " + e.getMessage());
            }
        }
        /*
         * TODO: don't calculate what is not needed - however this ensures that
         * the flag is respected
         */
        if (!dsWanted) {
            dsInfos = null;
            emptyDatasets = null;
        }

        return this.createSelection(dsInfos, dfInfos, emptyDatasets, invids, dsids, dfids, requestType);
    }

    /**
     * Checks to see if the investigation, dataset or datafile id list is a
     * valid comma separated list of longs. No spaces or leading 0's. Also
     * accepts null.
     */
    public static List<Long> getValidIds(String thing, String idList) throws BadRequestException {

        List<Long> result;
        if (idList == null) {
            result = Collections.emptyList();
        } else {
            String[] ids = idList.split("\\s*,\\s*");
            result = new ArrayList<>(ids.length);
            for (String id : ids) {
                try {
                    result.add(Long.parseLong(id));
                } catch (NumberFormatException e) {
                    throw new BadRequestException("The " + thing + " parameter '" + idList + "' is not a valid "
                            + "string representation of a comma separated list of longs");
                }
            }
        }
        return result;
    }

    private void manyDfs(Map<Long, DataInfoBase> dfInfos, long dsid, Session restSessionToUse, JsonArray result)
            throws IcatException, InsufficientPrivilegesException, InternalException {
        // dataset access for the user has been checked so the REST session for the
        // reader account can be used if the IDS setting to allow this is enabled
        long min = result.getJsonNumber(0).longValueExact();
        long max = result.getJsonNumber(1).longValueExact();
        long count = result.getJsonNumber(2).longValueExact();
        logger.debug("manyDfs min: {} max: {} count: {}", min, max, count);
        if (count != 0) {
            if (count <= maxEntities) {
                String query = "SELECT df.id, df.name, df.location, df.createId, df.modId FROM Datafile df WHERE df.dataset.id = "
                        + dsid + " AND df.location IS NOT NULL AND df.id BETWEEN " + min + " AND " + max;
                result = Json.createReader(new ByteArrayInputStream(restSessionToUse.search(query).getBytes())).readArray();
                for (JsonValue tupV : result) {
                    JsonArray tup = (JsonArray) tupV;
                    long dfid = tup.getJsonNumber(0).longValueExact();
                    String location = LocationHelper.getLocation(dfid, tup.getString(2, null));
                    dfInfos.put(dfid,
                            new DataFileInfo(dfid, tup.getString(1), location, tup.getString(3), tup.getString(4), dsid));
                }
            } else {
                long half = (min + max) / 2;
                String query = "SELECT min(df.id), max(df.id), count(df.id) FROM Datafile df WHERE df.dataset.id = "
                        + dsid + " AND df.location IS NOT NULL AND df.id BETWEEN " + min + " AND " + half;
                result = Json.createReader(new ByteArrayInputStream(restSessionToUse.search(query).getBytes())).readArray()
                        .getJsonArray(0);
                manyDfs(dfInfos, dsid, restSessionToUse, result);
                query = "SELECT min(df.id), max(df.id), count(df.id) FROM Datafile df WHERE df.dataset.id = " + dsid
                        + " AND df.location IS NOT NULL AND df.id BETWEEN " + (half + 1) + " AND " + max;
                result = Json.createReader(new ByteArrayInputStream(restSessionToUse.search(query).getBytes())).readArray()
                        .getJsonArray(0);
                manyDfs(dfInfos, dsid, restSessionToUse, result);
            }
        }
    }

    private void manyDss(Map<Long, DataInfoBase> dsInfos, HashSet<Long> emptyDatasets, Map<Long, DataInfoBase> dfInfos, Long invid, boolean dfWanted, Session userRestSession, Session restSessionToUseForDfs, JsonArray result)
            throws IcatException, InsufficientPrivilegesException, InternalException {
        long min = result.getJsonNumber(0).longValueExact();
        long max = result.getJsonNumber(1).longValueExact();
        long count = result.getJsonNumber(2).longValueExact();
        logger.debug("manyDss min: {} max: {} count: {}", min, max, count);
        if (count != 0) {
            if (count <= maxEntities) {
                String query = "SELECT inv.name, inv.visitId, inv.facility.id,  inv.facility.name FROM Investigation inv WHERE inv.id = "
                        + invid;
                result = Json.createReader(new ByteArrayInputStream(userRestSession.search(query).getBytes())).readArray();
                if (result.size() == 0) {
                    return;
                }
                result = result.getJsonArray(0);
                String invName = result.getString(0);
                String visitId = result.getString(1);
                long facilityId = result.getJsonNumber(2).longValueExact();
                String facilityName = result.getString(3);

                query = "SELECT ds.id, ds.name, ds.location FROM Dataset ds WHERE ds.investigation.id = " + invid
                        + " AND ds.id BETWEEN " + min + " AND " + max;
                result = Json.createReader(new ByteArrayInputStream(userRestSession.search(query).getBytes())).readArray();
                for (JsonValue tupV : result) {
                    JsonArray tup = (JsonArray) tupV;
                    long dsid = tup.getJsonNumber(0).longValueExact();
                    dsInfos.put(dsid, new DataSetInfo(dsid, tup.getString(1), tup.getString(2, null), invid, invName,
                            visitId, facilityId, facilityName));

                    query = "SELECT min(df.id), max(df.id), count(df.id) FROM Datafile df WHERE df.dataset.id = "
                            + dsid + " AND df.location IS NOT NULL";
                    result = Json.createReader(new ByteArrayInputStream(userRestSession.search(query).getBytes()))
                            .readArray().getJsonArray(0);
                    if (result.getJsonNumber(2).longValueExact() == 0) {
                        emptyDatasets.add(dsid);
                    } else if (dfWanted) {
                        manyDfs(dfInfos, dsid, restSessionToUseForDfs, result);
                    }

                }
            } else {
                long half = (min + max) / 2;
                String query = "SELECT min(ds.id), max(ds.id), count(ds.id) FROM Dataset ds WHERE ds.investigation.id = "
                        + invid + " AND ds.id BETWEEN " + min + " AND " + half;
                result = Json.createReader(new ByteArrayInputStream(userRestSession.search(query).getBytes())).readArray();
                manyDss(dsInfos, emptyDatasets, dfInfos, invid, dfWanted, userRestSession, restSessionToUseForDfs, result);
                query = "SELECT min(ds.id), max(ds.id), count(ds.id) FROM Dataset ds WHERE ds.investigation.id = "
                        + invid + " AND ds.id BETWEEN " + half + 1 + " AND " + max;
                result = Json.createReader(new ByteArrayInputStream(userRestSession.search(query).getBytes())).readArray()
                        .getJsonArray(0);
                manyDss(dsInfos, emptyDatasets, dfInfos, invid, dfWanted, userRestSession, restSessionToUseForDfs, result);
            }
        }

    }

    private void createRequestTypeToReturnsMapping() throws InternalException {

        this.requestTypeToReturnsMapping = new HashMap<RequestType, Returns>();
        StorageUnit storageUnit = this.propertyHandler.getStorageUnit();

        this.requestTypeToReturnsMapping.put(RequestType.DELETE, Returns.DATASETS_AND_DATAFILES);
        this.requestTypeToReturnsMapping.put(RequestType.GETDATAFILEIDS, Returns.DATAFILES);
        this.requestTypeToReturnsMapping.put(RequestType.GETSIZE, Returns.DATASETS_AND_DATAFILES);
        this.requestTypeToReturnsMapping.put(RequestType.PREPAREDATA, Returns.DATASETS_AND_DATAFILES);
        this.requestTypeToReturnsMapping.put(RequestType.RESET, Returns.DATASETS_AND_DATAFILES);
        this.requestTypeToReturnsMapping.put(RequestType.WRITE, Returns.DATASETS_AND_DATAFILES);
        this.requestTypeToReturnsMapping.put(RequestType.GETDATA, Returns.DATASETS_AND_DATAFILES);

        if(storageUnit == null ) {
            this.requestTypeToReturnsMapping.put(RequestType.GETSTATUS, Returns.DATASETS);
        }
        else if (storageUnit == StorageUnit.DATAFILE) {
            this.requestTypeToReturnsMapping.put(RequestType.GETSTATUS, Returns.DATAFILES);
            this.requestTypeToReturnsMapping.put(RequestType.RESTORE, Returns.DATAFILES);
            this.requestTypeToReturnsMapping.put(RequestType.ARCHIVE, Returns.DATAFILES);
        }
        else if(storageUnit == StorageUnit.DATASET) {
            this.requestTypeToReturnsMapping.put(RequestType.GETSTATUS, Returns.DATASETS);
            this.requestTypeToReturnsMapping.put(RequestType.RESTORE, Returns.DATASETS);
            this.requestTypeToReturnsMapping.put(RequestType.ARCHIVE, Returns.DATASETS);
        }


        else throw new InternalException("StorageUnit " + storageUnit + " unknown. Maybe you forgot to handle a new StorageUnit here?");
    }

    private Returns getReturns(RequestType requestType) throws NotImplementedException {

        if(this.requestTypeToReturnsMapping.containsKey(requestType))
            return this.requestTypeToReturnsMapping.get(requestType);

        
        // is this needed here?
        //if(this.propertyHandler.getStorageUnit() == null) throw new NotImplementedException("This operation is unavailable for single level storage");

        throw new NotImplementedException("There is to mapping for RequestType." + requestType + " and StorageUnit." + this.propertyHandler.getStorageUnit() + " defined. Did you forgot to register it in createRequestTypeToReturnsMapping()?");
    }

}