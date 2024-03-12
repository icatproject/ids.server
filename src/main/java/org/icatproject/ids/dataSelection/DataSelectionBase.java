package org.icatproject.ids.dataSelection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.models.DataFileInfo;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.services.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataSelectionBase {

    protected final static Logger logger = LoggerFactory.getLogger(DataSelectionBase.class);

    protected SortedMap<Long, DataInfoBase> dsInfos;
    protected SortedMap<Long, DataInfoBase> dfInfos;
    protected Set<Long> emptyDatasets;
    protected List<Long> invids;
    protected List<Long> dsids;
    protected List<Long> dfids;
    protected RequestType requestType;


    protected static ExecutorService threadPool;
    static { threadPool = Executors.newCachedThreadPool(); }

    private Map<String, PreparedStatus> preparedStatusMap = new ConcurrentHashMap<>();

    class PreparedStatus {
        public ReentrantLock lock = new ReentrantLock();
        public Long fromElement;
        public Future<?> future;

    }

    protected DataSelectionBase(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos, Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids, RequestType requestType) {

        this.dsInfos = dsInfos;
        this.dfInfos = dfInfos;
        this.emptyDatasets = emptyDatasets;
        this.invids = invids2;
        this.dsids = dsids;
        this.dfids = dfids;
        this.requestType = requestType;
    }

    public abstract boolean isPrepared(String preparedId) throws InternalException;

    /**
     * To get the DataInfos that is currently worked with, depending on StorageUnit
     * @return
     */
    public abstract SortedMap<Long, DataInfoBase> getPrimaryDataInfos();

    public abstract boolean existsInMainStorage(DataInfoBase dataInfo) throws InternalException;

    public abstract void queueDelete() throws NotImplementedException, InternalException;

    public abstract void scheduleTasks(DeferredOp operation) throws NotImplementedException, InternalException;


    public Map<Long, DataInfoBase> getDsInfo() {
        return dsInfos;
    }


    public Map<Long, DataInfoBase> getDfInfo() {
        return dfInfos;
    }


    public boolean mustZip() {
        return dfids.size() > 1L || !dsids.isEmpty() || !invids.isEmpty()
                || (dfids.isEmpty() && dsids.isEmpty() && invids.isEmpty());
    }

    public boolean isSingleDataset() {
        return dfids.isEmpty() && dsids.size() == 1 && invids.isEmpty();
    }


    public Set<Long> getEmptyDatasets() {
        return emptyDatasets;
    }


    /**
     * Checks to see if the investigation, dataset or datafile id list is a
     * valid comma separated list of longs. No spaces or leading 0's. Also
     * accepts null.
     */
    public static List<Long> getValidIds(String thing, String idList) throws BadRequestException {

        List<Long> result;
        if (idList == null) {
            result = Collections.emptyList();
        } else {
            String[] ids = idList.split("\\s*,\\s*");
            result = new ArrayList<>(ids.length);
            for (String id : ids) {
                try {
                    result.add(Long.parseLong(id));
                } catch (NumberFormatException e) {
                    throw new BadRequestException("The " + thing + " parameter '" + idList + "' is not a valid "
                            + "string representation of a comma separated list of longs");
                }
            }
        }
        return result;
    }


    private boolean restoreIfOffline(DataInfoBase dataInfo) throws InternalException {
        boolean maybeOffline = false;
        var serviceProvider = ServiceProvider.getInstance();
        if (serviceProvider.getFsm().getMaybeOffline().contains(dataInfo)) {
            maybeOffline = true;
        } else if (!this.existsInMainStorage(dataInfo)) {
            serviceProvider.getFsm().queue(dataInfo, DeferredOp.RESTORE);
            maybeOffline = true;
        }
        return maybeOffline;
    }


    public void checkOnline()throws InternalException, DataNotOnlineException {

        boolean maybeOffline = false;
        for (DataInfoBase dfInfo : this.getPrimaryDataInfos().values()) {
            if (this.restoreIfOffline(dfInfo)) {
                maybeOffline = true;
            }
        }
        if (maybeOffline) {
            throw new DataNotOnlineException(
                    "Before getting, putting, etc.  a datafile or dataset, it must be restored, restoration requested automatically");
        }
    }


    public void restoreDataInfos() {

        var dataInfos = this.getPrimaryDataInfos().values();
        if(!dataInfos.isEmpty()) {
            for (DataInfoBase dataInfo : dataInfos) {
                ServiceProvider.getInstance().getFsm().recordSuccess(dataInfo.getId());
            }
            threadPool.submit(new RestoreDataInfoTask(dataInfos, this, false));
        }
    }


    protected boolean areDataInfosPrepared(String preparedId) throws InternalException {
        boolean prepared = true;
        var serviceProvider = ServiceProvider.getInstance();
        PreparedStatus status = preparedStatusMap.computeIfAbsent(preparedId, k -> new PreparedStatus());

        Collection<DataInfoBase> toCheck = status.fromElement == null ? this.getPrimaryDataInfos().values()
                        : this.getPrimaryDataInfos().tailMap(status.fromElement).values();
        logger.debug("Will check online status of {} entries", toCheck.size());
        for (DataInfoBase dataInfo : toCheck) {
            serviceProvider.getFsm().checkFailure(dataInfo.getId());
            if (this.restoreIfOffline(dataInfo)) {
                prepared = false;
                status.fromElement = dataInfo.getId();
                toCheck = this.getPrimaryDataInfos().tailMap(status.fromElement).values();
                logger.debug("Will check in background status of {} entries", toCheck.size());
                status.future = threadPool.submit(new RestoreDataInfoTask(toCheck, this, true));
                break;
            }
        }
        if (prepared) {
            toCheck = status.fromElement == null ? Collections.emptySet()
                    : this.getPrimaryDataInfos().headMap(status.fromElement).values();
            logger.debug("Will check finally online status of {} entries", toCheck.size());
            for (DataInfoBase dataInfo : toCheck) {
                serviceProvider.getFsm().checkFailure(dataInfo.getId());
                if (this.restoreIfOffline(dataInfo)) {
                    prepared = false;
                }
            }
        }

        return prepared;
    }


    public void delete() throws InternalException, NotImplementedException {

        var serviceProvider = ServiceProvider.getInstance();

        /*
         * Delete the local copy directly rather than queueing it as it has
         * been removed from ICAT so will not be accessible to any
         * subsequent IDS calls.
         */
        for (DataInfoBase dataInfo : this.getDfInfo().values()) {
            var dfInfo = (DataFileInfo) dataInfo;
            String location = dataInfo.getLocation();
            try {
                if ((long) serviceProvider.getIcatReader()
                        .search("SELECT COUNT(df) FROM Datafile df WHERE df.location LIKE '" + location.replaceAll("'", "''") + "%'")
                        .get(0) == 0) {
                    if (serviceProvider.getMainStorage().exists(location)) {
                        logger.debug("Delete physical file " + location + " from main storage");
                        serviceProvider.getMainStorage().delete(location, dfInfo.getCreateId(), dfInfo.getModId());
                    }
                }
            } catch (IcatException_Exception e) {
                throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
            } catch (IOException e) {
                logger.error("I/O error " + e.getMessage() + " deleting " + dfInfo);
                throw new InternalException(e.getClass() + " " + e.getMessage());
            }
        }

        this.queueDelete();
    }


    private class RestoreDataInfoTask implements Callable<Void> {

        private Collection<DataInfoBase> dataInfos;
        private DataSelectionBase dataselection;
        private boolean checkFailure;

        public RestoreDataInfoTask(Collection<DataInfoBase> dataInfos, DataSelectionBase dataSelection, boolean checkFailure) {
            this.dataInfos = dataInfos;
            this.dataselection = dataSelection;
            this.checkFailure = checkFailure;
        }

        @Override
        public Void call() throws Exception {
            for (DataInfoBase dataInfo : dataInfos) {
                if(checkFailure)
                    ServiceProvider.getInstance().getFsm().checkFailure(dataInfo.getId());
                dataselection.restoreIfOffline(dataInfo);
            }
            return null;
        }
        
    }

}