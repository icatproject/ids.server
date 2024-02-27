package org.icatproject.ids.v3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.v3.enums.DeferredOp;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.models.DataInfoBase;

public class DataSelectionForSingleLevelStorage extends DataSelectionV3Base {

    protected DataSelectionForSingleLevelStorage(Map<Long, DataInfoBase> dsInfos, Map<Long, DataInfoBase> dfInfos,
            Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids, RequestType requestType) {
                
        super(dsInfos, dfInfos, emptyDatasets, invids2, dsids, dfids, requestType);
    }


    @Override
    protected void scheduleTask(DeferredOp operation) throws NotImplementedException, InternalException {

        throw new InternalException("This operation is unavailable for single level storage");
    }


    @Override
    public Collection<DataInfoBase> getPrimaryDataInfos() {
        return new ArrayList<DataInfoBase>();
    }


    @Override
    protected boolean existsInMainStorage(DataInfoBase dataInfo) throws InternalException {
        
        throw new InternalException("This operation is unavailable for single level storage");
    }


    
    
}