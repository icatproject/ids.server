package org.icatproject.ids.v3;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.v3.models.DataFileInfo;
import org.icatproject.ids.v3.models.DataSetInfo;

public class DataSelectionForSingleLevelStorage extends DataSelectionV3Base {

    protected DataSelectionForSingleLevelStorage(Map<Long, DataSetInfo> dsInfos, Set<DataFileInfo> dfInfos,
            Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids) {
                
        super(dsInfos, dfInfos, emptyDatasets, invids2, dsids, dfids);
    }

    @Override
    public void checkOnline() throws InternalException, DataNotOnlineException {
        // nothing to do here for single level storage
    }
    
}