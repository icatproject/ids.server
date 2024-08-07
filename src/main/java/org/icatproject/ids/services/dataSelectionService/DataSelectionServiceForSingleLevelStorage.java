package org.icatproject.ids.services.dataSelectionService;

import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.models.DataInfoBase;

public class DataSelectionServiceForSingleLevelStorage extends DataSelectionService {

    protected DataSelectionServiceForSingleLevelStorage(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos,
            Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids, long length, Boolean zip, Boolean compress, RequestType requestType) {
                
        super(dsInfos, dfInfos, emptyDatasets, invids2, dsids, dfids, length, zip, compress, requestType);
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


    @Override
    public void queueDelete() throws NotImplementedException, InternalException {
        //nothing todo for single level storage
    }
    

    @Override
    public void scheduleTasks(DeferredOp operation) throws NotImplementedException, InternalException {

        throw new NotImplementedException("This operation is unavailable for single level storage");
    }


    
    
}
