package org.icatproject.ids.v3;

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
import org.icatproject.ids.v3.models.DataSetInfo;

public class DataSelectionForStorageUnitDataset extends DataSelectionV3Base {

    protected DataSelectionForStorageUnitDataset(Map<Long, DataInfoBase> dsInfos, Map<Long, DataInfoBase> dfInfos,
            Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids, RequestType requestType) {

        super(dsInfos, dfInfos, emptyDatasets, invids2, dsids, dfids, requestType);
    }

    @Override
    public void checkOnline() throws InternalException, DataNotOnlineException {

        boolean maybeOffline = false;
        for (DataInfoBase dsInfo : dsInfos.values()) {
            if (this.restoreIfOffline(dsInfo, emptyDatasets)) {
                maybeOffline = true;
            }
        }
        if (maybeOffline) {
            throw new DataNotOnlineException(
                    "Before putting, getting or deleting a datafile, its dataset has to be restored, restoration requested automatically");
        }
    }


    public boolean restoreIfOffline(DataInfoBase dsInfo, Set<Long> emptyDatasets) throws InternalException {
        boolean maybeOffline = false;
        var serviceProvider = ServiceProvider.getInstance();
        if (serviceProvider.getFsm().getMaybeOffline().contains(dsInfo)) {
            maybeOffline = true;
        } else if (!emptyDatasets.contains(dsInfo.getId()) && !serviceProvider.getMainStorage().exists((DataSetInfo) dsInfo)) { //TODO: casting to DataSetInfo save?
            serviceProvider.getFsm().queue(dsInfo, DeferredOp.RESTORE);
            maybeOffline = true;
        }
        return maybeOffline;
    }

    @Override
    protected void scheduleTask(DeferredOp operation) throws NotImplementedException, InternalException {

        for (DataInfoBase dsInfo : dsInfos.values()) {
            ServiceProvider.getInstance().getFsm().queue(dsInfo, operation);
        }
    }

    @Override
    protected Collection<DataInfoBase> getDataInfosForStatusCheck() {
        return this.dsInfos.values();
    }

    @Override
    protected boolean existsInMainStorage(DataInfoBase dataInfo) throws InternalException {

        var dsInfo = (DataSetInfo) dataInfo;
        if(dsInfo == null) throw new InternalException("Could not cast DataInfoBase to DataSetInfo. Did you handed over another sub type?");
        
        return emptyDatasets.contains(dataInfo.getId()) || ServiceProvider.getInstance().getMainStorage().exists(dsInfo);
    }
      

}