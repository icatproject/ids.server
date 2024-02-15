package org.icatproject.ids.v3;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.v3.models.DataFileInfo;
import org.icatproject.ids.v3.models.DataSetInfo;

public class DataSelectionForStorageUnitDatafile extends DataSelectionV3Base {

    protected DataSelectionForStorageUnitDatafile(Map<Long, DataSetInfo> dsInfos, Set<DataFileInfo> dfInfos,
            Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids) {

        super(dsInfos, dfInfos, emptyDatasets, invids2, dsids, dfids);
    }

    @Override
    public void checkOnline()throws InternalException, DataNotOnlineException {

        boolean maybeOffline = false;
        for (DataFileInfo dfInfo : dfInfos) {
            if (dfInfo.restoreIfOffline()) {
                maybeOffline = true;
            }
        }
        if (maybeOffline) {
            throw new DataNotOnlineException(
                    "Before getting a datafile, it must be restored, restoration requested automatically");
        }
    }
    
}