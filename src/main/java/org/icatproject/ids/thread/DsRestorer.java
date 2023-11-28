package org.icatproject.ids.thread;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.IcatReader;
import org.icatproject.ids.IdsBean;
import org.icatproject.ids.LockManager.Lock;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/*
 * Restores datafiles from the slow to the fast storage.
 */
public class DsRestorer implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(DsRestorer.class);

    private DsInfo dsInfo;

    private MainStorageInterface mainStorageInterface;
    private ArchiveStorageInterface archiveStorageInterface;
    private FiniteStateMachine fsm;

    private Path datasetCache;

    private IcatReader reader;

    private ZipMapperInterface zipMapper;
    private Lock lock;

    public DsRestorer(DsInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm, IcatReader reader, Lock lock) {
        this.dsInfo = dsInfo;
        this.fsm = fsm;
        zipMapper = propertyHandler.getZipMapper();
        mainStorageInterface = propertyHandler.getMainStorage();
        archiveStorageInterface = propertyHandler.getArchiveStorage();
        datasetCache = propertyHandler.getCacheDir().resolve("dataset");
        this.reader = reader;
        this.lock = lock;
    }

    @Override
    public void run() {
        try {
            /*
             * This code avoids unnecessary calls to restore files. It will not
             * generally do anything as pointless restores are normally filtered
             * out earlier.
             */
            if (mainStorageInterface.exists(dsInfo)) {
                return;
            }

            long size = 0;
            int n = 0;
            List<Datafile> datafiles = ((Dataset) reader.get("Dataset INCLUDE Datafile", dsInfo.getDsId()))
                    .getDatafiles();
            Map<String, String> nameToLocalMap = new HashMap<>(datafiles.size());
            for (Datafile datafile : datafiles) {
                if (datafile.getLocation() == null) {
                    continue;
                }
                nameToLocalMap.put(datafile.getName(), IdsBean.getLocation(datafile.getId(), datafile.getLocation()));
                size += datafile.getFileSize();
                n++;
            }

            logger.debug("Restoring dataset " + dsInfo.getInvId() + "/" + dsInfo.getDsId() + " with " + n
                    + " files of total size " + size);

            // Get the file into the dataset cache
            Path datasetCachePath = Files.createTempFile(datasetCache, null, null);
            archiveStorageInterface.get(dsInfo, datasetCachePath);

            // Now split file and store it locally
            logger.debug("Unpacking dataset " + dsInfo.getInvId() + "/" + dsInfo.getDsId() + " with " + n
                    + " files of total size " + size);
            ZipInputStream zis = new ZipInputStream(Files.newInputStream(datasetCachePath));
            ZipEntry ze = zis.getNextEntry();
            Set<String> seen = new HashSet<>();
            while (ze != null) {
                String dfName = zipMapper.getFileName(ze.getName());
                if (seen.contains(dfName)) {
                    throw new RuntimeException("Corrupt archive for " + dsInfo + ": duplicate entry " + dfName);
                }
                String location = nameToLocalMap.get(dfName);
                if (location == null) {
                    throw new RuntimeException("Corrupt archive for " + dsInfo + ": spurious entry " + dfName);
                }
                mainStorageInterface.put(zis, location);
                ze = zis.getNextEntry();
                seen.add(dfName);
            }
            zis.close();
            if (!seen.equals(nameToLocalMap.keySet())) {
                throw new RuntimeException("Corrupt archive for " + dsInfo + ": missing entries");
            }
            Files.delete(datasetCachePath);
            fsm.recordSuccess(dsInfo.getDsId());
            logger.debug("Restore of " + dsInfo + " completed");
        } catch (Exception e) {
            fsm.recordFailure(dsInfo.getDsId());
            logger.error("Restore of " + dsInfo + " failed due to " + e.getClass() + " " + e.getMessage());
            try {
                mainStorageInterface.delete(dsInfo);
            } catch (IOException e2) {
            }
        } finally {
            fsm.removeFromChanging(dsInfo);
            lock.release();
        }
    }
}
