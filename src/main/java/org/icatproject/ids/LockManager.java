package org.icatproject.ids;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import jakarta.annotation.PostConstruct;
import jakarta.ejb.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.icatproject.ids.plugin.AlreadyLockedException;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;

@Singleton
public class LockManager {

    public enum LockType {
        SHARED, EXCLUSIVE
    }

    public class LockInfo {
        public final Long id;
        public final LockType type;
        public final int count;

        LockInfo(LockEntry le) {
            this.id = le.id;
            this.type = le.type;
            this.count = le.count;
        }
    }

    private class LockEntry {
        final Long id;
        final LockType type;
        final AutoCloseable storageLock;
        int count;

        LockEntry(Long id, LockType type, AutoCloseable storageLock) {
            this.id = id;
            this.type = type;
            this.storageLock = storageLock;
            this.count = 0;
            lockMap.put(id, this);
        }

        void inc() {
            count += 1;
        }

        void dec() {
            assert count > 0;
            count -= 1;
            if (count == 0) {
                lockMap.remove(id);
                if (storageLock != null) {
                    try {
                        storageLock.close();
                    } catch (Exception e) {
                        logger.error("Error while closing lock on {} in the storage plugin: {}.", id, e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Define the common interface of SingleLock and LockCollection
     */
    public abstract class Lock implements AutoCloseable {
        public abstract void release();

        public void close() {
            release();
        }
    }

    private class SingleLock extends Lock {
        private final Long id;
        private boolean isValid;

        SingleLock(Long id) {
            this.id = id;
            this.isValid = true;
        }

        public void release() {
            synchronized (lockMap) {
                if (isValid) {
                    lockMap.get(id).dec();
                    isValid = false;
                    logger.debug("Released a lock on {}.", id);
                }
            }
        }
    }

    private class LockCollection extends Lock {
        private ArrayList<Lock> locks;

        LockCollection() {
            locks = new ArrayList<>();
        }

        void add(Lock l) {
            locks.add(l);
        }

        public void release() {
            for (Lock l : locks) {
                l.release();
            }
        }
    }

    private static Logger logger = LoggerFactory.getLogger(LockManager.class);
    private PropertyHandler propertyHandler;
    private MainStorageInterface mainStorage;
    private Map<Long, LockEntry> lockMap = new HashMap<>();

    @PostConstruct
    private void init() {
        propertyHandler = PropertyHandler.getInstance();
        mainStorage = propertyHandler.getMainStorage();
        logger.debug("LockManager initialized.");
    }

    public Lock lock(DsInfo ds, LockType type) throws AlreadyLockedException, IOException {
        Long id = ds.getDsId();
        assert id != null;
        synchronized (lockMap) {
            LockEntry le = lockMap.get(id);
            if (le == null) {
                AutoCloseable storageLock;
                storageLock = mainStorage.lock(ds, type == LockType.SHARED);
                le = new LockEntry(id, type, storageLock);
            } else {
                if (type == LockType.EXCLUSIVE || le.type == LockType.EXCLUSIVE) {
                    throw new AlreadyLockedException();
                }
            }
            le.inc();
            logger.debug("Acquired a {} lock on {}.", type, id);
            return new SingleLock(id);
        }
    }

    public Lock lock(Collection<DsInfo> datasets, LockType type) throws AlreadyLockedException, IOException {
        LockCollection locks = new LockCollection();
        try {
            for (DsInfo ds : datasets) {
                locks.add(lock(ds, type));
            }
        } catch (AlreadyLockedException | IOException e) {
            locks.release();
            throw e;
        }
        return locks;
    }

    public Collection<LockInfo> getLockInfo() {
        Collection<LockInfo> lockInfo = new ArrayList<>();
        synchronized (lockMap) {
            for (LockEntry le : lockMap.values()) {
                lockInfo.add(new LockInfo(le));
            }
            return lockInfo;
        }
    }

}
