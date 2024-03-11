package org.icatproject.ids.v3;

import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.v3.models.DataInfoBase;

public class DataSelectionForStorageUnitDatafile extends DataSelectionV3Base {

    protected DataSelectionForStorageUnitDatafile(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos,
            Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids, RequestType requestType) {

        super(dsInfos, dfInfos, emptyDatasets, invids2, dsids, dfids, requestType);
    }


    @Override
    public SortedMap<Long, DataInfoBase> getPrimaryDataInfos() {
        return this.dfInfos;
    }

    @Override
    public boolean existsInMainStorage(DataInfoBase dataInfo) throws InternalException {
        return ServiceProvider.getInstance().getMainStorage().exists(dataInfo.getLocation());
    }


    @Override
    public boolean isPrepared(String preparedId) throws InternalException {
        return areDataInfosPrepared(preparedId);
    }


    @Override
    public void queueDelete() throws NotImplementedException, InternalException {
        this.scheduleTasks(DeferredOp.DELETE);
    }


    @Override
    public void scheduleTasks(DeferredOp operation) throws NotImplementedException, InternalException {

        for (DataInfoBase dataInfo : this.getPrimaryDataInfos().values()) {
            ServiceProvider.getInstance().getFsm().queue(dataInfo, operation);
        }
    }
    
}