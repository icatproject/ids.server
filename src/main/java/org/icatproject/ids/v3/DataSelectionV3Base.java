package org.icatproject.ids.v3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.icatproject.ids.DeferredOp;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.models.DataFileInfo;
import org.icatproject.ids.v3.models.DataSetInfo;

public abstract class DataSelectionV3Base {

    protected Map<Long, DataSetInfo> dsInfos;
    protected Set<DataFileInfo> dfInfos;
    protected Set<Long> emptyDatasets;
    protected List<Long> invids;
    protected List<Long> dsids;
    protected List<Long> dfids;
    protected RequestType requestType;
    protected HashMap<RequestType, DeferredOp> requestTypeToDeferredOpMapping;

    protected DataSelectionV3Base(Map<Long, DataSetInfo> dsInfos, Set<DataFileInfo> dfInfos, Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids, RequestType requestType) {

        this.dsInfos = dsInfos;
        this.dfInfos = dfInfos;
        this.emptyDatasets = emptyDatasets;
        this.invids = invids2;
        this.dsids = dsids;
        this.dfids = dfids;
        this.requestType = requestType;

        this.requestTypeToDeferredOpMapping = new HashMap<RequestType, DeferredOp>();
        this.requestTypeToDeferredOpMapping.put(RequestType.ARCHIVE, DeferredOp.ARCHIVE);
        this.requestTypeToDeferredOpMapping.put(RequestType.GETDATA, null);
    }


    public abstract void checkOnline() throws InternalException, DataNotOnlineException;

    protected abstract void scheduleTask(DeferredOp operation) throws NotImplementedException, InternalException;


    public Map<Long, DataSetInfo> getDsInfo() {
        return dsInfos;
    }


    public Set<DataFileInfo> getDfInfo() {
        return dfInfos;
    }


    public boolean mustZip() {
        return dfids.size() > 1L || !dsids.isEmpty() || !invids.isEmpty()
                || (dfids.isEmpty() && dsids.isEmpty() && invids.isEmpty());
    }

    public boolean isSingleDataset() {
        return dfids.isEmpty() && dsids.size() == 1 && invids.isEmpty();
    }


    public Set<Long> getEmptyDatasets() {
        return emptyDatasets;
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


    public void scheduleTask() throws NotImplementedException, InternalException {


        DeferredOp operation = this.requestTypeToDeferredOpMapping.get(this.requestType);

        if(operation == null) throw new InternalException("No DeferredOp defined for RequestType." + this.requestType);
            // ... or did you forget to add an entry for your new RequestType in this.requestTypeToDeferredOpMapping (constructor)?

        this.scheduleTask(operation);
    }

}