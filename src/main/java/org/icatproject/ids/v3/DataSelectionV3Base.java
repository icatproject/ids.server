package org.icatproject.ids.v3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.v3.models.DataFileInfo;
import org.icatproject.ids.v3.models.DataSetInfo;

public abstract class DataSelectionV3Base {

    protected Map<Long, DataSetInfo> dsInfos;
    protected Set<DataFileInfo> dfInfos;
    protected Set<Long> emptyDatasets;
    protected List<Long> invids;
    protected List<Long> dsids;
    protected List<Long> dfids;

    protected DataSelectionV3Base(Map<Long, DataSetInfo> dsInfos, Set<DataFileInfo> dfInfos, Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids) {

        this.dsInfos = dsInfos;
        this.dfInfos = dfInfos;
        this.emptyDatasets = emptyDatasets;
        this.invids = invids2;
        this.dsids = dsids;
        this.dfids = dfids;
    }


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


    public abstract void checkOnline() throws InternalException, DataNotOnlineException;

}