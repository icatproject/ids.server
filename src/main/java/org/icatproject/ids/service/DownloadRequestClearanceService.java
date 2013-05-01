package org.icatproject.ids.service;

import javax.ejb.EJB;
import javax.ejb.Schedule;
import javax.ejb.Schedules;
import javax.ejb.Singleton;

import org.icatproject.ids.storage.StorageFactory;
import org.icatproject.ids.storage.StorageInterface;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.DownloadRequestHelper;


@Singleton
public class DownloadRequestClearanceService {

    @EJB
    DownloadRequestHelper downloadRequestHelper;

    @Schedules({
        @Schedule(hour = "*/1")
    })
    public void removeExpiredDownloadRequests() {
        downloadRequestHelper.removeExpiredDownloadRequests();
    }

    @Schedules({
        @Schedule(dayOfWeek = "Sun")
    })
    public void removeOldLocalCacheFiles() {
        StorageFactory sf = StorageFactory.getInstance();
        StorageInterface si = sf.createStorageInterface(null, null);
        si.clearUnusedFiles(PropertyHandler.getInstance().getNumberOfDaysToKeepFilesInCache());
    }
}
