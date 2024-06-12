package org.icatproject.ids.models;

import java.util.List;
import java.util.Set;
import java.util.SortedMap;

public class DataSelection {
    protected SortedMap<Long, DataInfoBase> dsInfos;
    protected SortedMap<Long, DataInfoBase> dfInfos;
    protected Set<Long> emptyDatasets;
    protected List<Long> invids;
    protected List<Long> dsids;
    protected List<Long> dfids;
    protected Boolean zip;
    protected Boolean compress;
    private long length;

    public DataSelection(SortedMap<Long, DataInfoBase> dsInfos,
            SortedMap<Long, DataInfoBase> dfInfos, Set<Long> emptyDatasets,
            List<Long> invids2, List<Long> dsids, List<Long> dfids, long length,
            Boolean zip, Boolean compress) {

        this.dsInfos = dsInfos;
        this.dfInfos = dfInfos;
        this.emptyDatasets = emptyDatasets;
        this.invids = invids2;
        this.dsids = dsids;
        this.dfids = dfids;
        this.length = length;
        this.zip = zip;
        this.compress = compress;
    }

    public SortedMap<Long, DataInfoBase> getDsInfo() {
        return dsInfos;
    }

    public SortedMap<Long, DataInfoBase> getDfInfo() {
        return dfInfos;
    }

    public Boolean getZip() {
        return this.zip;
    }

    public Boolean getCompress() {
        return this.compress;
    }

    public long getLength() {
        return this.length;
    }

    public boolean mustZip() {
        // if(this.zip == null) {
        return dfids.size() > 1L || !dsids.isEmpty() || !invids.isEmpty()
                || (dfids.isEmpty() && dsids.isEmpty() && invids.isEmpty());
        // }

        // return this.zip;
    }

    public boolean isSingleDataset() {
        return dfids.isEmpty() && dsids.size() == 1 && invids.isEmpty();
    }

    public Set<Long> getEmptyDatasets() {
        return emptyDatasets;
    }
}
