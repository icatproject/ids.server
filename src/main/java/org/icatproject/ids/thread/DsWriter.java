package org.icatproject.ids.thread;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.icatproject.Datafile;
import org.icatproject.Dataset;

import org.icatproject.ids.finiteStateMachine.FiniteStateMachine;
import org.icatproject.ids.helpers.LocationHelper;
import org.icatproject.ids.models.DatafileInfo;
import org.icatproject.ids.models.DatasetInfo;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
import org.icatproject.ids.services.IcatReader;
import org.icatproject.ids.services.LockManager.Lock;
import org.icatproject.ids.services.PropertyHandler;

/**
 * Copies dataset from main to archive
 */
public class DsWriter implements Runnable {

    private final static Logger logger = LoggerFactory
            .getLogger(DsWriter.class);
    private static final int BUFSIZ = 1024;
    private DatasetInfo dsInfo;

    private FiniteStateMachine fsm;
    private MainStorageInterface mainStorageInterface;
    private ArchiveStorageInterface archiveStorageInterface;
    private Path datasetCache;
    private Path markerDir;
    private IcatReader reader;
    private ZipMapperInterface zipMapper;
    private Lock lock;

    public DsWriter(DatasetInfo dsInfo, PropertyHandler propertyHandler,
            FiniteStateMachine fsm, IcatReader reader, Lock lock) {
        this.dsInfo = dsInfo;
        this.fsm = fsm;
        this.zipMapper = propertyHandler.getZipMapper();
        mainStorageInterface = propertyHandler.getMainStorage();
        archiveStorageInterface = propertyHandler.getArchiveStorage();
        datasetCache = propertyHandler.getCacheDir().resolve("dataset");
        markerDir = propertyHandler.getCacheDir().resolve("marker");
        this.reader = reader;
        this.lock = lock;
    }

    @Override
    public void run() {
        try {
            if (!mainStorageInterface.exists(dsInfo)) {
                logger.info("No files present in main storage for " + dsInfo
                        + " - will delete archive");
                archiveStorageInterface.delete(dsInfo);
            } else {
                Path datasetCachePath = Files.createTempFile(datasetCache, null,
                        null);
                logger.debug("Creating " + datasetCachePath);
                List<Datafile> datafiles = ((Dataset) reader
                        .get("Dataset INCLUDE Datafile", dsInfo.getDsId()))
                        .getDatafiles();

                ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(
                        datasetCachePath, StandardOpenOption.CREATE));
                for (Datafile datafile : datafiles) {
                    if (datafile.getLocation() == null) {
                        continue;
                    }
                    String location = LocationHelper.getLocation(
                            datafile.getId(), datafile.getLocation());
                    InputStream is = null;
                    try {
                        zos.putNextEntry(
                                new ZipEntry(zipMapper.getFullEntryName(dsInfo,
                                        new DatafileInfo(datafile.getId(),
                                                datafile.getName(), location,
                                                datafile.getCreateId(),
                                                datafile.getModId(), 0L))));
                        is = mainStorageInterface.get(location,
                                datafile.getCreateId(), datafile.getModId());
                        int bytesRead = 0;
                        byte[] buffer = new byte[BUFSIZ];
                        while ((bytesRead = is.read(buffer)) > 0) {
                            zos.write(buffer, 0, bytesRead);
                        }
                    } catch (ZipException e) {
                        logger.debug("Skipping duplicate location " + location);
                    }
                    zos.closeEntry();
                    if (is != null) {
                        is.close();
                    }
                }
                zos.close();

                InputStream is = Files.newInputStream(datasetCachePath);
                archiveStorageInterface.put(dsInfo, is);
                Files.delete(datasetCachePath);
                is.close();
            }
            Path marker = markerDir.resolve(Long.toString(dsInfo.getDsId()));
            Files.deleteIfExists(marker);
            logger.debug("Removed marker " + marker);
            logger.debug("Write of " + dsInfo + " completed");
        } catch (Exception e) {
            logger.error("Write of " + dsInfo + " failed due to " + e.getClass()
                    + " " + e.getMessage());
        } finally {
            fsm.removeFromChanging(dsInfo);
            lock.release();
        }
    }
}
