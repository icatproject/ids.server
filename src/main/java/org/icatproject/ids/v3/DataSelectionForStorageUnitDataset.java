package org.icatproject.ids.v3;

import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.v3.enums.DeferredOp;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.models.DataInfoBase;
import org.icatproject.ids.v3.models.DataSetInfo;

public class DataSelectionForStorageUnitDataset extends DataSelectionV3Base {

    protected DataSelectionForStorageUnitDataset(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos,
            Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids, RequestType requestType) {

        super(dsInfos, dfInfos, emptyDatasets, invids2, dsids, dfids, requestType);
    }


    @Override
    protected void scheduleTask(DeferredOp operation) throws NotImplementedException, InternalException {

        for (DataInfoBase dsInfo : dsInfos.values()) {
            ServiceProvider.getInstance().getFsm().queue(dsInfo, operation);
        }
    }

    @Override
    public SortedMap<Long, DataInfoBase> getPrimaryDataInfos() {
        return this.dsInfos;
    }

    @Override
    public boolean existsInMainStorage(DataInfoBase dataInfo) throws InternalException {

        var dsInfo = (DataSetInfo) dataInfo;
        if(dsInfo == null) throw new InternalException("Could not cast DataInfoBase to DataSetInfo. Did you handed over another sub type?");
        
        return emptyDatasets.contains(dataInfo.getId()) || ServiceProvider.getInstance().getMainStorage().exists(dsInfo);
    }


    @Override
    public boolean isPrepared(String preparedId) throws InternalException {
        return this.areDataInfosPrepared(preparedId);
    }
      

}