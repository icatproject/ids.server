package org.icatproject.ids.storage.local;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.icatproject.ids.entity.DatafileEntity;
import org.icatproject.ids.storage.StorageInterface;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.StatusInfo;


public class LocalFileStorage implements StorageInterface {
	
	private final static Logger logger = Logger.getLogger(LocalFileStorage.class.getName());

    private PropertyHandler properties = PropertyHandler.getInstance();
    
    public LocalFileStorage() {
    	logger.log(Level.INFO, "LocalFileStorage constructed");
    }

    @Override
    public HashSet<String> copyDatafiles(List<DatafileEntity> datafileList) {
        HashSet<String> datafilePathSet = new HashSet<String>();
        // Check if the file exists. if it does then it adds to the return array list
        // and sets the status of datafile as found otherwise sets error in the status
        // of datafile
        for (DatafileEntity datafileEntity : datafileList) {
            String filename = properties.getLocalStorageSystemPath() + File.separator
                    + datafileEntity.getName();
            File df = new File(filename);
            if (df.exists()) {
                datafileEntity.setStatus(StatusInfo.COMPLETED.name());
                datafilePathSet.add(filename);
            } else {
                datafileEntity.setStatus(StatusInfo.ERROR.name());
            }
        }
        return datafilePathSet;
    }

    @Override
    public String getStoragePath() {
        return properties.getLocalStorageSystemPath();
    }

    @Override
    public void clearUnusedFiles(int numberOfDays) {
        // Doesn't delete the local cache files
    }

}
