package org.icatproject.ids;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.icatproject.ids.exceptions.InternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to manage the "failed" files which are kept within a subdirectory of
 * the IDS cache directory, are named with the prepared ID that they relate to
 * and contain an ordered list of all of the file paths of files for that 
 * prepared ID that failed to restore from the Archive Storage.
 */
public class FailedFilesManager {

	private static Logger logger = LoggerFactory.getLogger(FailedFilesManager.class);
    private Path failedFilesDir;

    /**
     * Default constructor used within the main IDS code to identify the 
     * subdirectory of the cache where "failed" files are stored. 
     * 
     * @param idsCacheDir the IDS cache dir. See {@link PropertyHandler#cacheDir PropertyHandler}
     */
    public FailedFilesManager(Path idsCacheDir) {
        this(idsCacheDir, false);
    }

    /**
     * Additional constructor to allow for use by the test classes where a new
     * unique "failed" files folder will be created each time the test runs in 
     * order to ensure that the tests don't use an old folder with unknown
     * contents.
     * 
     * @param idsCacheDir the IDS cache dir. See {@link PropertyHandler#cacheDir PropertyHandler}
     * @param makeUnique set to true if the "failed" folder should be given a 
     *                   unique name
     */
    public FailedFilesManager(Path idsCacheDir, boolean makeUnique) {
        String dirName = Constants.FAILED_DIR_NAME;
        if (makeUnique) {
            dirName += "-" + System.currentTimeMillis();
        }
        failedFilesDir = idsCacheDir.resolve(dirName);
    }

    /**
     * Write the given list of file paths for the specified prepared ID to a
     * file. If there are already files paths written to the file then add to 
     * the list. The final list will be ordered so the file paths being added
     * may appear above, below or mixed in with any entries that were already
     * in the file.
     * 
     * @param preparedId the related prepared ID
     * @param failedFilepaths the list of file paths to write into the file
     */
    public void writeToFailedEntriesFile(String preparedId, Set<String> failedFilepaths) {
        try {
            SortedSet<String> sortedFilepathsSet = new TreeSet<>();
            sortedFilepathsSet.addAll(getFailedEntriesForPreparedId(preparedId));
            sortedFilepathsSet.addAll(failedFilepaths);
            try (FileWriter writer = new FileWriter(failedFilesDir.resolve(preparedId).toFile())) {
                for(String filepath : sortedFilepathsSet) {
                    writer.write(filepath + System.lineSeparator());
                }
            }
        } catch (InternalException | IOException e) {
            String message = String.format("%s writing failed file for prepared ID %s : %s",
                    e.getClass().getSimpleName(), preparedId, e.getMessage());
            logger.error(message);
        }
    }

    /**
     * Read an ordered list of file paths that have failed to restore for the 
     * specified prepared ID from the "failed" file.
     * 
     * @param preparedId the related prepared ID
     * @return a Set of Strings containing the failed file paths or an empty 
     *         set if there is no file for the given prepared ID
     * @throws InternalException if there is a problem reading the failed file
     */
    public Set<String> getFailedEntriesForPreparedId(String preparedId) throws InternalException {
        Path failedFilesFilePath = failedFilesDir.resolve(preparedId);
        if (Files.exists(failedFilesFilePath)) {
            Set<String> failedFilepathsSet;
            try (Stream<String> lines = Files.lines(failedFilesFilePath)) {
                failedFilepathsSet = lines.collect(Collectors.toSet());
                Set<String> sortedSet = new TreeSet<>();
                sortedSet.addAll(failedFilepathsSet);
                return sortedSet;
            } catch (IOException e) {
                String message = String.format("IOException getting failed files for prepared ID %s : %s",
                        preparedId, e.getMessage());
                logger.error(message);
                throw new InternalException(message);
            }
        } else {
            return Collections.emptySet();
        }
    }

    /**
     * Delete the failed file for the specified prepared ID.
     * 
     * @param preparedId the related prepared ID
     */
    public void deleteFailedFile(String preparedId) {
        boolean deleted = failedFilesDir.resolve(preparedId).toFile().delete();
        if (deleted) {
            logger.debug("Deleted failed file for preparedId {}", preparedId);
        } else {
            logger.debug("No failed file found to delete for preparedId {}", preparedId);
        }
    }

    /**
     * Get the path to the directory that is being used to hold the "failed" 
     * files.
     * 
     * @return the Path to the failed files directory
     */
    public Path getFailedFilesDir() {
        return failedFilesDir;
    }

}
