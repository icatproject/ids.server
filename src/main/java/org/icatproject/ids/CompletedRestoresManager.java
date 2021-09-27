package org.icatproject.ids;

import java.io.IOException;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompletedRestoresManager {

    private static Logger logger = LoggerFactory.getLogger(CompletedRestoresManager.class);
    private Path completedFilesDir;

    /**
     * Default constructor used within the main IDS code to identify the 
     * subdirectory of the cache where "completed" files are stored. 
     * 
     * @param idsCacheDir the IDS cache dir. See {@link PropertyHandler#cacheDir PropertyHandler}
     */
    public CompletedRestoresManager(Path idsCacheDir) {
        this(idsCacheDir, false);
    }

    /**
     * Additional constructor to allow for use by the test classes where a new
     * unique "completed" files folder will be created each time the test runs in 
     * order to ensure that the tests don't use an old folder with unknown
     * contents.
     * 
     * @param idsCacheDir the IDS cache dir. See {@link PropertyHandler#cacheDir PropertyHandler}
     * @param makeUnique set to true if the "completed" folder should be given a 
     *                   unique name
     */
    public CompletedRestoresManager(Path idsCacheDir, boolean makeUnique) {
        String dirName = Constants.COMPLETED_DIR_NAME;
        if (makeUnique) {
            dirName += "-" + System.currentTimeMillis();
        }
        completedFilesDir = idsCacheDir.resolve(dirName);
    }

    public void createCompletedFile(String preparedId) {
        try {
            completedFilesDir.resolve(preparedId).toFile().createNewFile();
        } catch (IOException e) {
            logger.error("IOException creating completed file for preparedId {}: {}", preparedId, e.getMessage());
        }
    }

    public boolean checkCompletedFileExists(String preparedId) {
        return completedFilesDir.resolve(preparedId).toFile().exists();
    }

    public void deleteCompletedFile(String preparedId) {
        boolean deleted = completedFilesDir.resolve(preparedId).toFile().delete();
        if (deleted) {
            logger.debug("Deleted completed file for preparedId {}", preparedId);
        } else {
            logger.debug("No completed file found to delete for preparedId {}", preparedId);
        }
    }

    public Path getCompletedFilesDir() {
        return completedFilesDir;
    }


}
