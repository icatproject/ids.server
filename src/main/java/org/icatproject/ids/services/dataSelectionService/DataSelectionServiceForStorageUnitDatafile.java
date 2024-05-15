package org.icatproject.ids.services.dataSelectionService;

import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.services.ServiceProvider;

public class DataSelectionServiceForStorageUnitDatafile extends DataSelectionService {

    protected DataSelectionServiceForStorageUnitDatafile(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos,
            Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids, long length, Boolean zip, Boolean compress, RequestType requestType) {

        super(dsInfos, dfInfos, emptyDatasets, invids2, dsids, dfids, length, zip, compress, requestType);
    }


    @Override
    public SortedMap<Long, DataInfoBase> getPrimaryDataInfos() {
        return this.dataSelection.getDfInfo();
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
