package org.icatproject.ids;

import java.util.HashMap;
import java.util.Map;

import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;

/**
 * Class to enable quick lookups of the total number of files in each restore
 * request. The file count is always available by reading the "prepared" file
 * and getting the size of the dfInfos Set but this is inefficient particularly
 * for large restore requests, so keeping them available in memory is more 
 * efficient.
 * 
 * This is particularly important for the functionality to report the progress
 * of a restore request, where regular requests are likely to be made and the 
 * total number of files is required to make the calculation each time.
 */
public class RestoreFileCountManager {

	private static RestoreFileCountManager instance = null;

    private Map<String, Integer> fileCountMap = new HashMap<>();
    
    private RestoreFileCountManager() {

    }

    public static synchronized RestoreFileCountManager getInstance() {
		if (instance == null) {
			instance = new RestoreFileCountManager();
		}
		return instance;
	}

    public void addEntryToMap(String preparedId, int fileCount) {
        fileCountMap.put(preparedId, fileCount);
    }

    /**
     * There should be an entry in the map for every preparedId but if the IDS
     * has been restarted then any entries it held previously will be missing.
     * In this case, the "prepared" file needs to be loaded to get the file 
     * count from there and create an entry in the map for it.
     * 
     * @param preparedId
     * @return the total number of files to restore for a prepared ID
     * @throws NotFoundException if no "prepared" file is found
     * @throws InternalException if there is a problem reading the "prepared" file
     */
    public int getFileCount(String preparedId) throws InternalException, NotFoundException {
        Integer fileCount = fileCountMap.get(preparedId);
        if (fileCount == null) {
            PreparedFilesManager preparedFilesManager = new PreparedFilesManager();
            Prepared prepared = preparedFilesManager.unpack(preparedId);
            fileCount = prepared.dfInfos.size();
            fileCountMap.put(preparedId, fileCount);
        }
        return fileCount.intValue();
    }

    public void removeEntryFromMap(String preparedId) {
        fileCountMap.remove(preparedId);
    }

}
