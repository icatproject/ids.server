package org.icatproject.ids.service;

import javax.ejb.EJB;
import javax.ejb.Schedule;
import javax.ejb.Schedules;
import javax.ejb.Singleton;

import org.icatproject.ids2.ported.RequestHelper;


@Singleton
public class RequestClearanceService {

    @EJB
    RequestHelper requestHelper;

    // TODO adapt to the new RequestHelper
    @Schedules({
        @Schedule(hour = "*/1")
    })
    public void removeExpiredDownloadRequests() {
//        downloadRequestHelper.removeExpiredDownloadRequests();
    }

    @Schedules({
        @Schedule(dayOfWeek = "Sun")
    })
    public void removeOldLocalCacheFiles() {
//        StorageFactory sf = StorageFactory.getInstance();
//        StorageInterface si = sf.createStorageInterface(null, null);
//        si.clearUnusedFiles(PropertyHandler.getInstance().getNumberOfDaysToKeepFilesInCache()); // TODO implement clearing unused files (here or in some other class)
    }
}
