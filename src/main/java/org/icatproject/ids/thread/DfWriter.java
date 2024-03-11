package org.icatproject.ids.thread;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.icatproject.ids.LockManager.Lock;
import org.icatproject.ids.finiteStateMachine.FiniteStateMachine;
import org.icatproject.ids.models.DataFileInfo;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.MainStorageInterface;

/**
 * Copies datafiles from main to archive
 */
public class DfWriter implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(DfWriter.class);

    private FiniteStateMachine fsm;
    private MainStorageInterface mainStorageInterface;
    private ArchiveStorageInterface archiveStorageInterface;
    private Path markerDir;
    private List<DataFileInfo> dataFileInfos;
    private Collection<Lock> locks;

    public DfWriter(List<DataFileInfo> dfInfos, PropertyHandler propertyHandler, FiniteStateMachine fsm, Collection<Lock> locks) {
        this.dataFileInfos = dfInfos;
        this.fsm = fsm;
        this.locks = locks;
        mainStorageInterface = propertyHandler.getMainStorage();
        archiveStorageInterface = propertyHandler.getArchiveStorage();
        markerDir = propertyHandler.getCacheDir().resolve("marker");
    }

    @Override
    public void run() {
        try {
            for (DataFileInfo dataFileInfo : dataFileInfos) {
                String dfLocation = dataFileInfo.getDfLocation();
                try (InputStream is = mainStorageInterface.get(dfLocation, dataFileInfo.getCreateId(), dataFileInfo.getModId())) {
                    archiveStorageInterface.put(is, dfLocation);
                    Path marker = markerDir.resolve(Long.toString(dataFileInfo.getDfId()));
                    Files.deleteIfExists(marker);
                    logger.debug("Removed marker " + marker);
                    logger.debug("Write of " + dataFileInfo + " completed");
                } catch (Exception e) {
                    logger.error("Write of " + dataFileInfo + " failed due to " + e.getClass() + " " + e.getMessage());
                } finally {
                    fsm.removeFromChanging(dataFileInfo);
                }
            }
        } finally {
            for (Lock l : locks) {
                l.release();
            }
        }
    }
}
