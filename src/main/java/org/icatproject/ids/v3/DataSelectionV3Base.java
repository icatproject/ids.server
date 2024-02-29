package org.icatproject.ids.v3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

import org.icatproject.ids.IdsBean.RestoreDataInfoTask;
import org.icatproject.ids.Status;
import org.icatproject.ids.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.v3.enums.DeferredOp;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.models.DataInfoBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataSelectionV3Base {

    protected final static Logger logger = LoggerFactory.getLogger(DataSelectionV3Base.class);

    protected SortedMap<Long, DataInfoBase> dsInfos;
    protected SortedMap<Long, DataInfoBase> dfInfos;
    protected Set<Long> emptyDatasets;
    protected List<Long> invids;
    protected List<Long> dsids;
    protected List<Long> dfids;
    protected RequestType requestType;
    protected HashMap<RequestType, DeferredOp> requestTypeToDeferredOpMapping;
    public ExecutorService threadPool;

    private Map<String, PreparedStatus> preparedStatusMap = new ConcurrentHashMap<>();

    class PreparedStatus {
        public ReentrantLock lock = new ReentrantLock();
        public Long fromElement;
        public Future<?> future;

    }

    protected DataSelectionV3Base(SortedMap<Long, DataInfoBase> dsInfos, SortedMap<Long, DataInfoBase> dfInfos, Set<Long> emptyDatasets, List<Long> invids2, List<Long> dsids, List<Long> dfids, RequestType requestType) {

        this.dsInfos = dsInfos;
        this.dfInfos = dfInfos;
        this.emptyDatasets = emptyDatasets;
        this.invids = invids2;
        this.dsids = dsids;
        this.dfids = dfids;
        this.requestType = requestType;

        this.requestTypeToDeferredOpMapping = new HashMap<RequestType, DeferredOp>();
        this.requestTypeToDeferredOpMapping.put(RequestType.ARCHIVE, DeferredOp.ARCHIVE);
        this.requestTypeToDeferredOpMapping.put(RequestType.GETDATA, null);

        this.threadPool = Executors.newCachedThreadPool();
    }


    protected abstract void scheduleTask(DeferredOp operation) throws NotImplementedException, InternalException;

    public abstract boolean isPrepared(String preparedId) throws InternalException;

    /**
     * To get the DataInfos whom are primary worked on, depending on StorageUnit
     * @return
     */
    public abstract SortedMap<Long, DataInfoBase> getPrimaryDataInfos();

    protected abstract boolean existsInMainStorage(DataInfoBase dataInfo) throws InternalException;


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


    public void scheduleTask() throws NotImplementedException, InternalException {


        DeferredOp operation = this.requestTypeToDeferredOpMapping.get(this.requestType);

        if(operation == null) throw new InternalException("No DeferredOp defined for RequestType." + this.requestType);
            // ... or did you forget to add an entry for your new RequestType in this.requestTypeToDeferredOpMapping (constructor)?

        this.scheduleTask(operation);
    }

    public Status getStatus() throws InternalException {
        Status status = Status.ONLINE;
        var serviceProvider = ServiceProvider.getInstance();

        Set<DataInfoBase> restoring = serviceProvider.getFsm().getRestoring();
        Set<DataInfoBase> maybeOffline = serviceProvider.getFsm().getMaybeOffline();
        for (DataInfoBase dataInfo : this.getPrimaryDataInfos().values()) {
            serviceProvider.getFsm().checkFailure(dataInfo.getId());
            if (restoring.contains(dataInfo)) {
                status = Status.RESTORING;
            } else if (maybeOffline.contains(dataInfo)) {
                status = Status.ARCHIVED;
                break;
            } else if (!this.existsInMainStorage(dataInfo)) {
                status = Status.ARCHIVED;
                break;
            }
        }

        return status;
    }

    public boolean restoreIfOffline(DataInfoBase dataInfo) throws InternalException {
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
            this.threadPool.submit(new RestoreDataInfoTask(dataInfos, this));
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
                status.future = threadPool.submit(new RunPrepDataInfoCheck(toCheck, this));
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


    private class RunPrepDataInfoCheck implements Callable<Void> {

        private Collection<DataInfoBase> toCheck;
        private DataSelectionV3Base dataselection;

        public RunPrepDataInfoCheck(Collection<DataInfoBase> toCheck, DataSelectionV3Base dataSelection) {
            this.toCheck = toCheck;
            this.dataselection = dataSelection;
        }

        @Override
        public Void call() throws Exception {
            for(DataInfoBase dataInfo : toCheck) {
                ServiceProvider.getInstance().getFsm().checkFailure(dataInfo.getId());
                dataselection.restoreIfOffline(dataInfo);
            }
            return null;
        }
        
    }

    public class RestoreDataInfoTask implements Callable<Void> {
        private Collection<DataInfoBase> dataInfos;
        private DataSelectionV3Base dataSelection;

        public RestoreDataInfoTask(Collection<DataInfoBase> dataInfos, DataSelectionV3Base dataSelection) {
            this.dataInfos = dataInfos;
            this.dataSelection = dataSelection;
        }

        @Override
        public Void call() throws Exception {
            for (DataInfoBase dfInfo : dataInfos) {
                dataSelection.restoreIfOffline(dfInfo);
            }
            return null;
        }

    }

}