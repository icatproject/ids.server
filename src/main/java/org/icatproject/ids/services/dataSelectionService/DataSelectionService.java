package org.icatproject.ids.services.dataSelectionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
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
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.models.DataSelection;
import org.icatproject.ids.models.DatafileInfo;
import org.icatproject.ids.services.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataSelectionService {

    protected static final Logger logger = LoggerFactory.getLogger(
        DataSelectionService.class
    );

    protected DataSelection dataSelection;
    protected RequestType requestType;

    protected static ExecutorService threadPool;

    static {
        threadPool = Executors.newCachedThreadPool();
    }

    private Map<String, PreparedStatus> preparedStatusMap = new ConcurrentHashMap<>();

    class PreparedStatus {

        public ReentrantLock lock = new ReentrantLock();
        public Long fromElement;
        public Future<?> future;
    }

    protected DataSelectionService(
        SortedMap<Long, DataInfoBase> dsInfos,
        SortedMap<Long, DataInfoBase> dfInfos,
        Set<Long> emptyDatasets,
        List<Long> invids2,
        List<Long> dsids,
        List<Long> dfids,
        long length,
        Boolean zip,
        Boolean compress,
        RequestType requestType
    ) {
        this.dataSelection =
            new DataSelection(
                dsInfos,
                dfInfos,
                emptyDatasets,
                invids2,
                dsids,
                dfids,
                length,
                zip,
                compress
            );

        this.requestType = requestType;
    }

    public abstract boolean isPrepared(String preparedId)
        throws InternalException;

    /**
     * To get the DataInfos that is currently worked with, depending on StorageUnit
     * @return
     */
    public abstract SortedMap<Long, DataInfoBase> getPrimaryDataInfos();

    public abstract boolean existsInMainStorage(DataInfoBase dataInfo)
        throws InternalException;

    public abstract void queueDelete()
        throws NotImplementedException, InternalException;

    public abstract void scheduleTasks(DeferredOp operation)
        throws NotImplementedException, InternalException;

    //TODO: maybe implementing this method here whould be the better way, than making it abstract. But we will miss the NotImplementedException for SingleLevelStorage in that case.
    // should we change the tests and the behavior of the ids server here
    // public void scheduleTasks(DeferredOp operation) throws NotImplementedException, InternalException {
    //     for (DataInfoBase dataInfo : this.getPrimaryDataInfos().values()) {      // in case of SingleLevelStorage an empty map is returned and nothing will happen (even not an exception).
    //         ServiceProvider.getInstance().getFsm().queue(dataInfo, operation);
    //     }
    // }

    public SortedMap<Long, DataInfoBase> getDsInfo() {
        return this.dataSelection.getDsInfo();
    }

    public SortedMap<Long, DataInfoBase> getDfInfo() {
        return this.dataSelection.getDfInfo();
    }

    public Boolean getZip() {
        return this.dataSelection.getZip();
    }

    public Boolean getCompress() {
        return this.dataSelection.getCompress();
    }

    public long getLength() {
        return this.dataSelection.getLength();
    }

    public boolean mustZip() {
        return this.dataSelection.mustZip();
    }

    public boolean isSingleDataset() {
        return this.dataSelection.isSingleDataset();
    }

    public Set<Long> getEmptyDatasets() {
        return this.dataSelection.getEmptyDatasets();
    }

    /**
     * tries to extract a list of ids from a comma separated id string. No spaces or leading 0's. Also
     * accepts null.
     * @param thing the name of the id list - for better error message
     * @param idList a String which shoald contain long numbers seperated by commas
     * @return list of long numbers - the extracted id values
     * @throws BadRequestException
     */
    public static List<Long> getValidIds(String thing, String idList)
        throws BadRequestException {
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
                    throw new BadRequestException(
                        "The " +
                        thing +
                        " parameter '" +
                        idList +
                        "' is not a valid " +
                        "string representation of a comma separated list of longs"
                    );
                }
            }
        }
        return result;
    }

    private boolean restoreIfOffline(DataInfoBase dataInfo)
        throws InternalException {
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

    public void checkOnline() throws InternalException, DataNotOnlineException {
        boolean maybeOffline = false;
        for (DataInfoBase dfInfo : this.getPrimaryDataInfos().values()) {
            if (this.restoreIfOffline(dfInfo)) {
                maybeOffline = true;
            }
        }
        if (maybeOffline) {
            throw new DataNotOnlineException(
                "Before getting, putting, etc.  a datafile or dataset, it must be restored, restoration requested automatically"
            );
        }
    }

    public void restoreDataInfos() {
        var dataInfos = this.getPrimaryDataInfos().values();
        if (!dataInfos.isEmpty()) {
            for (DataInfoBase dataInfo : dataInfos) {
                ServiceProvider
                    .getInstance()
                    .getFsm()
                    .recordSuccess(dataInfo.getId());
            }
            threadPool.submit(new RestoreDataInfoTask(dataInfos, this, false));
        }
    }

    public OptionalLong getFileLength() {
        if (
            this.dataSelection.getDfInfo().isEmpty() ||
            this.dataSelection.mustZip()
        ) {
            return OptionalLong.empty();
        }

        return OptionalLong.of(this.dataSelection.getLength());
    }

    protected boolean areDataInfosPrepared(String preparedId)
        throws InternalException {
        boolean prepared = true;
        var serviceProvider = ServiceProvider.getInstance();
        PreparedStatus status = preparedStatusMap.computeIfAbsent(
            preparedId,
            k -> new PreparedStatus()
        );

        Collection<DataInfoBase> toCheck = status.fromElement == null
            ? this.getPrimaryDataInfos().values()
            : this.getPrimaryDataInfos().tailMap(status.fromElement).values();
        logger.debug("Will check online status of {} entries", toCheck.size());
        for (DataInfoBase dataInfo : toCheck) {
            serviceProvider.getFsm().checkFailure(dataInfo.getId());
            if (this.restoreIfOffline(dataInfo)) {
                prepared = false;
                status.fromElement = dataInfo.getId();
                toCheck =
                    this.getPrimaryDataInfos()
                        .tailMap(status.fromElement)
                        .values();
                logger.debug(
                    "Will check in background status of {} entries",
                    toCheck.size()
                );
                status.future =
                    threadPool.submit(
                        new RestoreDataInfoTask(toCheck, this, true)
                    );
                break;
            }
        }
        if (prepared) {
            toCheck =
                status.fromElement == null
                    ? Collections.emptySet()
                    : this.getPrimaryDataInfos()
                        .headMap(status.fromElement)
                        .values();
            logger.debug(
                "Will check finally online status of {} entries",
                toCheck.size()
            );
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
        for (DataInfoBase dataInfo : this.dataSelection.getDfInfo().values()) {
            var dfInfo = (DatafileInfo) dataInfo;
            String location = dataInfo.getLocation();
            try {
                if (
                    (long) serviceProvider
                        .getIcatReader()
                        .search(
                            "SELECT COUNT(df) FROM Datafile df WHERE df.location LIKE '" +
                            location.replaceAll("'", "''") +
                            "%'"
                        )
                        .get(0) ==
                    0
                ) {
                    if (serviceProvider.getMainStorage().exists(location)) {
                        logger.debug(
                            "Delete physical file " +
                            location +
                            " from main storage"
                        );
                        serviceProvider
                            .getMainStorage()
                            .delete(
                                location,
                                dfInfo.getCreateId(),
                                dfInfo.getModId()
                            );
                    }
                }
            } catch (IcatException_Exception e) {
                throw new InternalException(
                    e.getFaultInfo().getType() + " " + e.getMessage()
                );
            } catch (IOException e) {
                logger.error(
                    "I/O error " + e.getMessage() + " deleting " + dfInfo
                );
                throw new InternalException(
                    e.getClass() + " " + e.getMessage()
                );
            }
        }

        this.queueDelete();
    }

    private class RestoreDataInfoTask implements Callable<Void> {

        private Collection<DataInfoBase> dataInfos;
        private DataSelectionService dataselection;
        private boolean checkFailure;

        public RestoreDataInfoTask(
            Collection<DataInfoBase> dataInfos,
            DataSelectionService dataSelection,
            boolean checkFailure
        ) {
            this.dataInfos = dataInfos;
            this.dataselection = dataSelection;
            this.checkFailure = checkFailure;
        }

        @Override
        public Void call() throws Exception {
            for (DataInfoBase dataInfo : dataInfos) {
                if (checkFailure) ServiceProvider
                    .getInstance()
                    .getFsm()
                    .checkFailure(dataInfo.getId());
                dataselection.restoreIfOffline(dataInfo);
            }
            return null;
        }
    }
}
