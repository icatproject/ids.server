package org.icatproject.ids.v3;

import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.v3.enums.DeferredOp;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.models.DataInfoBase;

public class DataSelectionForSingleLevelStorage extends DataSelectionV3Base {

    protected DataSelectionForSingleLevelStorage(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos,
            Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids, RequestType requestType) {
                
        super(dsInfos, dfInfos, emptyDatasets, invids2, dsids, dfids, requestType);
    }


    @Override
    protected void scheduleTask(DeferredOp operation) throws NotImplementedException, InternalException {

        throw new NotImplementedException("This operation is unavailable for single level storage");
    }


    @Override
    public SortedMap<Long, DataInfoBase> getPrimaryDataInfos() {
        return new TreeMap<Long, DataInfoBase>();
    }


    @Override
    public boolean existsInMainStorage(DataInfoBase dataInfo) throws InternalException {
        
        throw new InternalException("This operation is unavailable for single level storage");
    }


    @Override
    public boolean isPrepared(String preparedId) throws InternalException {
        return true;
    }


    
    
}