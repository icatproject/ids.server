package org.icatproject.ids.dataSelection;

import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.models.DataSetInfo;
import org.icatproject.ids.services.ServiceProvider;

public class DataSelectionForStorageUnitDataset extends DataSelectionBase {

    protected DataSelectionForStorageUnitDataset(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos,
            Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids, RequestType requestType) {

        super(dsInfos, dfInfos, emptyDatasets, invids2, dsids, dfids, requestType);
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


    @Override
    public void queueDelete() throws NotImplementedException, InternalException {
        this.scheduleTasks(DeferredOp.WRITE);
    }


    @Override
    public void scheduleTasks(DeferredOp operation) throws NotImplementedException, InternalException {

        for (DataInfoBase dataInfo : this.getPrimaryDataInfos().values()) {
            ServiceProvider.getInstance().getFsm().queue(dataInfo, operation);
        }
    }
      

}