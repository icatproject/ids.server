package org.icatproject.ids.v3;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.enums.StorageUnit;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.v3.helper.LocationHelper;
import org.icatproject.ids.v3.models.DataFileInfo;
import org.icatproject.ids.v3.models.DataInfoBase;
import org.icatproject.ids.v3.models.DataSetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to handle not finished work.
 * It could be redesigned with sub classes depending on StorageUnit, but it isn't worth it i guess, just for one abstract method (loadDataInfo()).
 */
public class UnfinishedWorkService {


    protected final static Logger logger = LoggerFactory.getLogger(UnfinishedWorkService.class);

    public UnfinishedWorkService() {
        
    }

    public void restartUnfinishedWork(Path markerDir, String key) throws InternalException {

        try {
            var serviceProvider = ServiceProvider.getInstance();
            StorageUnit storageUnit = serviceProvider.getPropertyHandler().getStorageUnit();

            for (File file : markerDir.toFile().listFiles()) {

                if(storageUnit == null) break;
                
                long id = Long.parseLong(file.toPath().getFileName().toString());
                try {
                    DataInfoBase dataInfo = this.loadDataInfo(storageUnit, id);
                    serviceProvider.getFsm().queue(dataInfo, DeferredOp.WRITE);
                    logger.info("Queued " + storageUnit.toString().toLowerCase() + " with id " + id + " " + dataInfo
                            + " to be written as it was not written out previously by IDS");
                } catch (IcatException_Exception e) {
                    if (e.getFaultInfo().getType() == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
                        logger.warn( storageUnit.toString().toLowerCase() + " with id " + id
                                + " was not written out by IDS and now no longer known to ICAT");
                        Files.delete(file.toPath());
                    } else {
                        throw e;
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


    private DataInfoBase loadDataInfo(StorageUnit storageUnit, long id) throws IcatException_Exception, InsufficientPrivilegesException, InternalException {
        if(storageUnit == StorageUnit.DATASET) {
            Dataset ds = (Dataset) ServiceProvider.getInstance().getIcatReader().get("Dataset ds INCLUDE ds.investigation.facility", id);
            return new DataSetInfo(ds);
        }

        if(storageUnit == StorageUnit.DATAFILE) {
            Datafile df = (Datafile) ServiceProvider.getInstance().getIcatReader().get("Datafile ds INCLUDE ds.dataset", id);
            String location = LocationHelper.getLocation(df.getId(), df.getLocation());
            return new DataFileInfo(id, df.getName(), location, df.getCreateId(), df.getModId(), df.getDataset().getId());
        }

        return null;
    }
}