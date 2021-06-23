package org.icatproject.ids;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;
import org.icatproject.Login.Credentials;
import org.icatproject.Login.Credentials.Entry;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtils {

	private static Logger logger = LoggerFactory.getLogger(TestUtils.class);

    /**
     * Do an ICAT login and get the session ID using a list of credentials of
     * the format (as found in the properties files):
     * "db username READER password READERpass"
     *
     * @param icatService the ICAT to log in to
     * @param credsString a String of credentials in the format described above
     * @return an ICAT session ID
     * @throws IcatException_Exception if the login fails
     */
    public static String login(ICAT icatService, String credsString) throws IcatException_Exception {
        List<String> creds = Arrays.asList(credsString.trim().split("\\s+"));
        Credentials credentials = new Credentials();
        List<Entry> entries = credentials.getEntry();
        for (int i = 1; i < creds.size(); i += 2) {
            Entry entry = new Entry();
            entry.setKey(creds.get(i));
            entry.setValue(creds.get(i + 1));
            entries.add(entry);
        }
        return icatService.login(creds.get(0), credentials);
    }

    /**
     * Recursively delete a directory and everything below it
     * 
     * @param dirToDelete the Path of the directory to delete
     * @throws IOException
     */
    public static void recursivelyDeleteDirectory(Path dirToDelete) throws IOException {
        if (dirToDelete.toFile().exists()) {
            Files.walk(dirToDelete)
                .map(Path::toFile)
                .sorted((o1, o2) -> -o1.compareTo(o2))
                .forEach(File::delete);
        }
    }

    /**
     * Check that all files from the list of DatafileInfo objects provided have
     * a corresponding file available on Main Storage (disk cache)
     * 
     * @param mainStorage an implementation of the IDS MainStorageInterface
     * @param dfInfos the list of DatafileInfo object to check
     * @throws IOException if any of the files are not found
     */
    public static void checkFilesOnMainStorage(MainStorageInterface mainStorage, 
            List<DfInfo> dfInfos) throws IOException {
        for (DfInfo dfInfo : dfInfos) {
            String filePath = dfInfo.getDfLocation();
            if (!mainStorage.exists(filePath)) {
                throw new IOException("File " + filePath + " not found on Main Storage");
            }
        }
        logger.debug("All {} files were found on Main Storage", dfInfos.size());
    }

    /**
     * Copy "unit.tests.run.properties" to "run.properties"
     * and put it in a location that is on the classpath.
     * 
     * @return the Path to the location of the run.properties file
     * @throws IOException
     */
    public static Path copyUnitTestsRunPropertiesToClasspath() throws IOException {
        Path testClassesDir = Paths.get(System.getProperty("user.dir")).resolve("target/test-classes");
        Path sourceRunPropertiesPath = testClassesDir.resolve("unit.tests.run.properties");
        Path targetRunPropertiesPath = testClassesDir.resolve(Constants.RUN_PROPERTIES_FILENAME);
        logger.debug("sourceRunPropertiesPath: {}", sourceRunPropertiesPath);
        logger.debug("targetRunPropertiesPath: {}", targetRunPropertiesPath);
        if (sourceRunPropertiesPath.toFile().exists()) {
            logger.debug("Found unit.tests.run.properties");
            Files.copy(sourceRunPropertiesPath, targetRunPropertiesPath, StandardCopyOption.REPLACE_EXISTING);    
        }
       return targetRunPropertiesPath;
    }

    /**
     * Read the "plugin.main.dir" property from the run.properties files and 
     * create the directory specified by it. This is the disk cache area used 
     * for Main Storage after files have been retrieved from Archive Storage.
     * 
     * Throws an exception if the directory already exists as a safety 
     * precaution. The user should decide if the existing contents of the 
     * directory are OK to delete and do that manually.
     * 
     * @return the path to the plugin.main.dir
     * @throws Exception if the directory already exists.
     */
    public static Path createPluginMainDir() throws Exception {
        try (InputStream is = TestUtils.class.getClassLoader().getResourceAsStream(Constants.RUN_PROPERTIES_FILENAME)) {
            Properties simpleProps = new Properties();
            simpleProps.load(is);
            Path pluginMainDirPath = Paths.get(simpleProps.getProperty("plugin.main.dir"));
            if ( pluginMainDirPath.toFile().exists() ) {
                // user should decide if this is OK to be deleted
                throw new Exception(("plugin.main.dir: " + pluginMainDirPath + " already exists"));
            } else {
                Files.createDirectories(pluginMainDirPath);
            }
            return pluginMainDirPath;
        }
    }

    /**
     * Read the "cache.dir" property from the run.properties files and 
     * create the directory specified by it. This is the disk cache area used 
     * for by the IDS to hold, for example, prepared, completed and failed 
     * files.
     * 
     * Throws an exception if the directory already exists as a safety 
     * precaution. The user should decide if the existing contents of the 
     * directory are OK to delete and do that manually.
     * 
     * @return the path to the cache.dir
     * @throws Exception if the directory already exists.
     */
    public static Path createCacheDir() throws Exception {
        try (InputStream is = TestUtils.class.getClassLoader().getResourceAsStream(Constants.RUN_PROPERTIES_FILENAME)) {
            Properties simpleProps = new Properties();
            simpleProps.load(is);
            Path cacheDirPath = Paths.get(simpleProps.getProperty("cache.dir"));
            if ( cacheDirPath.toFile().exists() ) {
                // user should decide if this is OK to be deleted
                throw new Exception(("cache.dir: " + cacheDirPath + " already exists"));
            } else {
                Files.createDirectories(cacheDirPath);
                Files.createDirectories(cacheDirPath.resolve(Constants.PREPARED_DIR_NAME));
                Files.createDirectories(cacheDirPath.resolve(Constants.COMPLETED_DIR_NAME));
                Files.createDirectories(cacheDirPath.resolve(Constants.FAILED_DIR_NAME));
            }
            return cacheDirPath;
        }
    }

}
