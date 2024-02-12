package org.icatproject.ids.v3;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.DeferredOp;
import org.icatproject.ids.StorageUnit;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.v3.models.DataFileInfo;
import org.icatproject.ids.v3.models.DataSetInfo;
import org.icatproject.utils.IcatSecurity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnfinishedWorkServiceBase {


    protected final static Logger logger = LoggerFactory.getLogger(UnfinishedWorkServiceBase.class);

    public UnfinishedWorkServiceBase() {
        
    }

    public void restartUnfinishedWork(Path markerDir, String key) throws InternalException {

        try {
            var serviceProvider = ServiceProvider.getInstance();
            StorageUnit storageUnit = serviceProvider.getPropertyHandler().getStorageUnit();
            for (File file : markerDir.toFile().listFiles()) {
                if (storageUnit == StorageUnit.DATASET) {
                    long dsid = Long.parseLong(file.toPath().getFileName().toString());
                    Dataset ds = null;
                    try {
                        ds = (Dataset) serviceProvider.getIcatReader().get("Dataset ds INCLUDE ds.investigation.facility", dsid);
                        DataSetInfo dsInfo = new DataSetInfo(ds);
                        serviceProvider.getFsm().queue(dsInfo, DeferredOp.WRITE);
                        logger.info("Queued dataset with id " + dsid + " " + dsInfo
                                + " to be written as it was not written out previously by IDS");
                    } catch (IcatException_Exception e) {
                        if (e.getFaultInfo().getType() == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
                            logger.warn("Dataset with id " + dsid
                                    + " was not written out by IDS and now no longer known to ICAT");
                            Files.delete(file.toPath());
                        } else {
                            throw e;
                        }
                    }
                } else if (storageUnit == StorageUnit.DATAFILE) {
                    long dfid = Long.parseLong(file.toPath().getFileName().toString());
                    Datafile df = null;
                    try {
                        df = (Datafile) serviceProvider.getIcatReader().get("Datafile ds INCLUDE ds.dataset", dfid);
                        String location = getLocation(df.getId(), df.getLocation(), key);
                        DataFileInfo dfInfo = new DataFileInfo(dfid, df.getName(), location, df.getCreateId(),
                                df.getModId(), df.getDataset().getId());
                        serviceProvider.getFsm().queue(dfInfo, DeferredOp.WRITE);
                        logger.info("Queued datafile with id " + dfid + " " + dfInfo
                                + " to be written as it was not written out previously by IDS");
                    } catch (IcatException_Exception e) {
                        if (e.getFaultInfo().getType() == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
                            logger.warn("Datafile with id " + dfid
                                    + " was not written out by IDS and now no longer known to ICAT");
                            Files.delete(file.toPath());
                        } else {
                            throw e;
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }
    }

    public static void cleanPreparedDir(Path preparedDir) {
        for (File file : preparedDir.toFile().listFiles()) {
            Path path = file.toPath();
            String pf = path.getFileName().toString();
            if (pf.startsWith("tmp.") || pf.endsWith(".tmp")) {
                try {
                    long thisSize = 0;
                    if (Files.isDirectory(path)) {
                        for (File notZipFile : file.listFiles()) {
                            thisSize += Files.size(notZipFile.toPath());
                            Files.delete(notZipFile.toPath());
                        }
                    }
                    thisSize += Files.size(path);
                    Files.delete(path);
                    logger.debug("Deleted " + path + " to reclaim " + thisSize + " bytes");
                } catch (IOException e) {
                    logger.debug("Failed to delete " + path + e.getMessage());
                }
            }
        }
    }

    static void cleanDatasetCache(Path datasetDir) {
        for (File dsFile : datasetDir.toFile().listFiles()) {
            Path path = dsFile.toPath();
            try {
                long thisSize = Files.size(path);
                Files.delete(path);
                logger.debug("Deleted " + path + " to reclaim " + thisSize + " bytes");
            } catch (IOException e) {
                logger.debug("Failed to delete " + path + " " + e.getClass() + " " + e.getMessage());
            }
        }
    }


    public static String getLocation(long dfid, String location, String key)
            throws InsufficientPrivilegesException, InternalException {
        if (location == null) {
            throw new InternalException("location is null");
        }
        if (key == null) {
            return location;
        } else {
            return getLocationFromDigest(dfid, location, key);
        }
    }

    static String getLocationFromDigest(long id, String locationWithHash, String key)
            throws InternalException, InsufficientPrivilegesException {
        int i = locationWithHash.lastIndexOf(' ');
        try {
            String location = locationWithHash.substring(0, i);
            String hash = locationWithHash.substring(i + 1);
            if (!hash.equals(IcatSecurity.digest(id, location, key))) {
                throw new InsufficientPrivilegesException(
                        "Location \"" + locationWithHash + "\" does not contain a valid hash.");
            }
            return location;
        } catch (IndexOutOfBoundsException e) {
            throw new InsufficientPrivilegesException("Location \"" + locationWithHash + "\" does not contain hash.");
        } catch (NoSuchAlgorithmException e) {
            throw new InternalException(e.getMessage());
        }
    }
}