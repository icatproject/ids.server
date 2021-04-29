package org.icatproject.ids;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Properties;

import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.storage.ArchiveStorageDummy;
import org.icatproject.ids.thread.RestorerThreadManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RestorerThreadManagerTest {

    private static Path targetRunPropertiesPath;
    private static Path pluginMainDirPath;
    private static Path cacheDirPath;
    private static ArchiveStorageDummy archiveStorageDummy;
    private static MainStorageInterface mainStorage;
    
    @BeforeClass
    public static void setupClass() throws Exception {
        // copy "unit.tests.run.properties" to "run.properties"
        // and put it in a location that is on the classpath
        Path testClassesDir = Paths.get(System.getProperty("user.dir")).resolve("target/test-classes");
        Path sourceRunPropertiesPath = testClassesDir.resolve("unit.tests.run.properties");
        targetRunPropertiesPath = testClassesDir.resolve("run.properties");
        System.out.println("sourceRunPropertiesPath: " + sourceRunPropertiesPath);
        System.out.println("targetRunPropertiesPath: " + targetRunPropertiesPath);
        if (sourceRunPropertiesPath.toFile().exists()) {
            System.out.println("Found unit.tests.run.properties");
            Files.copy(sourceRunPropertiesPath, targetRunPropertiesPath, StandardCopyOption.REPLACE_EXISTING);    
        }

        // create directories that are specified in run.properties
        // throw an exception if they already exist so the user can decide 
        // whether they are safe to be deleted or not
        Properties properties = new Properties();
        properties.load(new FileInputStream(targetRunPropertiesPath.toFile()));
        pluginMainDirPath = Paths.get(properties.getProperty("plugin.main.dir"));
        if (pluginMainDirPath.toFile().exists()) {
            throw new Exception("plugin.main.dir: " + pluginMainDirPath + " already exists");
        } else {
            Files.createDirectories(pluginMainDirPath);
        }
        cacheDirPath = Paths.get(properties.getProperty("cache.dir"));
        if (cacheDirPath.toFile().exists()) {
            throw new Exception("cache.dir: " + cacheDirPath + " already exists");
        } else {
            Files.createDirectories(cacheDirPath);
            Files.createDirectories(cacheDirPath.resolve(Constants.COMPLETED_DIR_NAME));
            Files.createDirectories(cacheDirPath.resolve(Constants.FAILED_DIR_NAME));
        }

        // run.properties will now be used to initialise the PropertyHandler
        // and the directories defined within it will now exist
        mainStorage = PropertyHandler.getInstance().getMainStorage();

        // initialise the dummy archive storage used for testing
        archiveStorageDummy = new ArchiveStorageDummy(null);
    }

    @Test
    public void testMultiThreadedRestoreIncludingFailure() throws Exception {
        // set a system property to cause the first of the threads to be 
        // created to fail part way through and be resubmitted 
        System.setProperty(ArchiveStorageDummy.RESTORE_FAIL_PROPERTY, "true");

        RestorerThreadManager restorerThreadManager = new RestorerThreadManager();
        // with the lists below this test takes about 30 seconds
        List<DfInfo> dfInfos1 = archiveStorageDummy.createDfInfosList(0, 5);
        List<DfInfo> dfInfos2 = archiveStorageDummy.createDfInfosList(5, 12);
        List<DfInfo> dfInfos3 = archiveStorageDummy.createDfInfosList(12, 30);
        List<DfInfo> dfInfos4 = archiveStorageDummy.createDfInfosList(30, 60);
        List<DfInfo> dfInfos5 = archiveStorageDummy.createDfInfosList(60, 90);
        restorerThreadManager.createRestorerThread("preparedId1", dfInfos1);
        restorerThreadManager.createRestorerThread("preparedId2", dfInfos2);
        restorerThreadManager.createRestorerThread("preparedId3", dfInfos3);
        restorerThreadManager.createRestorerThread("preparedId4", dfInfos4);
        restorerThreadManager.createRestorerThread("preparedId5", dfInfos5);
        int numFilesRemaining = dfInfos1.size() + dfInfos2.size() + dfInfos3.size() + dfInfos4.size() + dfInfos5.size();
        while (numFilesRemaining > 0) {
            Thread.sleep(1000);
            int numFilesRemaining1 = restorerThreadManager.getTotalNumFilesRemaining("preparedId1");
            int numFilesRemaining2 = restorerThreadManager.getTotalNumFilesRemaining("preparedId2");
            int numFilesRemaining3 = restorerThreadManager.getTotalNumFilesRemaining("preparedId3");
            int numFilesRemaining4 = restorerThreadManager.getTotalNumFilesRemaining("preparedId4");
            int numFilesRemaining5 = restorerThreadManager.getTotalNumFilesRemaining("preparedId5");
            System.out.println("preparedId1 numFilesRemaining: " + numFilesRemaining1 + " (" + System.currentTimeMillis() + ")");
            System.out.println("preparedId2 numFilesRemaining: " + numFilesRemaining2 + " (" + System.currentTimeMillis() + ")");
            System.out.println("preparedId3 numFilesRemaining: " + numFilesRemaining3 + " (" + System.currentTimeMillis() + ")");
            System.out.println("preparedId4 numFilesRemaining: " + numFilesRemaining4 + " (" + System.currentTimeMillis() + ")");
            System.out.println("preparedId5 numFilesRemaining: " + numFilesRemaining5 + " (" + System.currentTimeMillis() + ")");
            numFilesRemaining = numFilesRemaining1 + numFilesRemaining2 + numFilesRemaining3 + numFilesRemaining4 + numFilesRemaining5;
        }
        // just wait a few secs to get any further reporting from threads
        Thread.sleep(5000);
        // check all the files were restored to main storage
        TestUtils.checkFilesOnMainStorage(mainStorage, dfInfos1);
        TestUtils.checkFilesOnMainStorage(mainStorage, dfInfos2);
        TestUtils.checkFilesOnMainStorage(mainStorage, dfInfos3);
        TestUtils.checkFilesOnMainStorage(mainStorage, dfInfos4);
        TestUtils.checkFilesOnMainStorage(mainStorage, dfInfos5);
        System.out.println("Finishing testMultiThreadedRestoreIncludingFailure");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        System.out.println("Running tearDownClass");
        Files.deleteIfExists(targetRunPropertiesPath);
        if (pluginMainDirPath != null) {
            TestUtils.recursivelyDeleteDirectory(pluginMainDirPath);
        }
        if (cacheDirPath != null) {
            TestUtils.recursivelyDeleteDirectory(cacheDirPath);
        }
    }

}
