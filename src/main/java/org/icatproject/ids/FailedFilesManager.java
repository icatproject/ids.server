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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public void writeToFailedEntriesFile(String preparedId, Set<String> failedFilepaths) throws IOException {
        SortedSet<String> sortedFilepathsSet = new TreeSet<>();
        sortedFilepathsSet.addAll(getFailedEntriesForPreparedId(preparedId));
        sortedFilepathsSet.addAll(failedFilepaths);
        try (FileWriter writer = new FileWriter(failedFilesDir.resolve(preparedId).toFile())) { 
            for(String filepath : sortedFilepathsSet) {
                writer.write(filepath + System.lineSeparator());
            }
        }
    }

    public Set<String> getFailedEntriesForPreparedId(String preparedId) throws IOException {
        Path failedFilesFilePath = failedFilesDir.resolve(preparedId);
        if (Files.exists(failedFilesFilePath)) {
            Set<String> failedFilepathsSet;
            try (Stream<String> lines = Files.lines(failedFilesFilePath)) {
                failedFilepathsSet = lines.collect(Collectors.toSet());
                Set<String> sortedSet = new TreeSet<>();
                sortedSet.addAll(failedFilepathsSet);
                return sortedSet;
            }
        } else {
            return Collections.emptySet();
        }
    }

    public void deleteFailedFile(String preparedId) {
        boolean deleted = failedFilesDir.resolve(preparedId).toFile().delete();
        if (deleted) {
            logger.debug("Deleted failed file for preparedId {}", preparedId);
        } else {
            logger.debug("No failed file found to delete for preparedId {}", preparedId);
        }
    }

    public Path getFailedFilesDir() {
        return failedFilesDir;
    }

}
