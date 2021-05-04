package org.icatproject.ids;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.storage.ArchiveStorageDummy;
import org.icatproject.ids.thread.RestorerThreadManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.stfc.storaged.DfInfoWithLocation;

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

    @Before
    public void setup() throws Exception {
        Files.createDirectories(pluginMainDirPath);
        Files.createDirectories(cacheDirPath);
        Files.createDirectories(cacheDirPath.resolve(Constants.COMPLETED_DIR_NAME));
        Files.createDirectories(cacheDirPath.resolve(Constants.FAILED_DIR_NAME));
    }

    @Test
    public void testMultiThreadedRestore() throws Exception {
        RestorerThreadManager restorerThreadManager = new RestorerThreadManager();
        // request 44 files: with maxRestoresPerThread set to 10 this should
        // result in 4 threads restoring 9 files and 1 restoring 8 files
        // (check logging output to confirm this, if required)
        List<DfInfo> dfInfos1 = archiveStorageDummy.createDfInfosList(0, 44);
        String preparedId = "preparedId1";
        restorerThreadManager.submitFilesForRestore(preparedId, dfInfos1);
        int numFilesRemaining = dfInfos1.size();
        while (numFilesRemaining > 0) {
            Thread.sleep(1000);
            numFilesRemaining = restorerThreadManager.getTotalNumFilesRemaining(preparedId);
        }
        // just wait a few secs to get any further reporting from threads
        Thread.sleep(5000);
        // check all the files were restored to main storage
        TestUtils.checkFilesOnMainStorage(mainStorage, dfInfos1);
        assertTrue("Problem with completed file and/or failed files", 
                checkForCompletedFileAndEmptyFailedFile(preparedId));
    }

    @Test
    public void testMultiThreadedRestoreIncludingFailure() throws Exception {
        // set a system property to cause the first of the threads to be 
        // created to fail part way through and be resubmitted 
        System.setProperty(ArchiveStorageDummy.RESTORE_FAIL_PROPERTY, "true");

        RestorerThreadManager restorerThreadManager = new RestorerThreadManager();
        List<DfInfo> dfInfos1 = archiveStorageDummy.createDfInfosList(0, 5);
        List<DfInfo> dfInfos2 = archiveStorageDummy.createDfInfosList(5, 12);
        List<DfInfo> dfInfos3 = archiveStorageDummy.createDfInfosList(12, 30);
        List<DfInfo> dfInfos4 = archiveStorageDummy.createDfInfosList(30, 60);
        List<DfInfo> dfInfos5 = archiveStorageDummy.createDfInfosList(60, 90);
        restorerThreadManager.createRestorerThread("preparedId1", dfInfos1, false);
        restorerThreadManager.createRestorerThread("preparedId2", dfInfos2, false);
        restorerThreadManager.createRestorerThread("preparedId3", dfInfos3, false);
        restorerThreadManager.createRestorerThread("preparedId4", dfInfos4, false);
        restorerThreadManager.createRestorerThread("preparedId5", dfInfos5, false);
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
        assertTrue("Problem with completed file and/or failed files", 
                checkForCompletedFileAndEmptyFailedFile("preparedId1"));
        assertTrue("Problem with completed file and/or failed files", 
                checkForCompletedFileAndEmptyFailedFile("preparedId2"));
        assertTrue("Problem with completed file and/or failed files", 
                checkForCompletedFileAndEmptyFailedFile("preparedId3"));
        assertTrue("Problem with completed file and/or failed files", 
                checkForCompletedFileAndEmptyFailedFile("preparedId4"));
        assertTrue("Problem with completed file and/or failed files", 
                checkForCompletedFileAndEmptyFailedFile("preparedId5"));
    }

    @Test
    public void testRestoreFilesSomeAlreadyOnCache() throws Exception {
        // restore the first 10 files from the dummy archive storage list
        RestorerThreadManager restorerThreadManager = new RestorerThreadManager();
        List<DfInfo> dfInfos1 = archiveStorageDummy.createDfInfosList(0, 10);
        restorerThreadManager.submitFilesForRestore("preparedId1", dfInfos1);
        int numFilesRemaining = dfInfos1.size();
        while (numFilesRemaining > 0) {
            Thread.sleep(1000);
            numFilesRemaining = restorerThreadManager.getTotalNumFilesRemaining("preparedId1");
            System.out.println("preparedId1 numFilesRemaining: " + numFilesRemaining + " (" + System.currentTimeMillis() + ")");
        }
        // just wait a few secs to get any further reporting from threads
        Thread.sleep(5000);
        // check all the files were restored to main storage
        TestUtils.checkFilesOnMainStorage(mainStorage, dfInfos1);

        // restore the first 20 files from the dummy archive storage list
        // the first 10 of these should already be on the cache so will not be restored again
        List<DfInfo> dfInfos2 = archiveStorageDummy.createDfInfosList(0, 20);
        restorerThreadManager.submitFilesForRestore("preparedId2", dfInfos2);
        numFilesRemaining = dfInfos2.size();
        while (numFilesRemaining > 0) {
            Thread.sleep(1000);
            numFilesRemaining = restorerThreadManager.getTotalNumFilesRemaining("preparedId2");
            System.out.println("preparedId2 numFilesRemaining: " + numFilesRemaining + " (" + System.currentTimeMillis() + ")");
        }
        // just wait a few secs to get any further reporting from threads
        Thread.sleep(5000);
        // check all the files were restored to main storage
        TestUtils.checkFilesOnMainStorage(mainStorage, dfInfos2);
        assertTrue("Problem with completed file and/or failed files", 
                checkForCompletedFileAndEmptyFailedFile("preparedId1"));
        assertTrue("Problem with completed file and/or failed files", 
                checkForCompletedFileAndEmptyFailedFile("preparedId2"));
    }

    @Test
    public void testMultiThreadedRestoreMultipleMissingFiles() throws Exception {
        // create a list of files to request with non-existent files at the 
        // start and at the end such that the list will get split into two
        // threads with some non-existent files in each
        List<DfInfo> dfInfosToRestore = new ArrayList<>();

        Set<String> nonExistentFiles1 = new HashSet<>();
        nonExistentFiles1.add("/non/existent/file3.txt");
        nonExistentFiles1.add("/non/existent/file1.txt");
        nonExistentFiles1.add("/non/existent/file2.txt");
        for (String filePath : nonExistentFiles1) {
            dfInfosToRestore.add(new DfInfoWithLocation(Paths.get(filePath)));
        }

        List<DfInfo> dfInfosExistingFiles = archiveStorageDummy.createDfInfosList(0, 10);
        dfInfosToRestore.addAll(dfInfosExistingFiles); 

        Set<String> nonExistentFiles2 = new HashSet<>();
        nonExistentFiles2.add("/another/non/existent/file5.txt");
        nonExistentFiles2.add("/another/non/existent/file4.txt");
        for (String filePath : nonExistentFiles2) {
            dfInfosToRestore.add(new DfInfoWithLocation(Paths.get(filePath)));
        }

        String preparedId = "preparedId1";
        RestorerThreadManager restorerThreadManager = new RestorerThreadManager();
        restorerThreadManager.submitFilesForRestore(preparedId, dfInfosToRestore);
        int numFilesRemaining = dfInfosToRestore.size();
        while (numFilesRemaining > 0) {
            Thread.sleep(1000);
            numFilesRemaining = restorerThreadManager.getTotalNumFilesRemaining(preparedId);
            System.out.println(preparedId + " numFilesRemaining: " + numFilesRemaining + " (" + System.currentTimeMillis() + ")");
        }
        // just wait a few secs to get any further reporting from threads
        Thread.sleep(5000);

        Set<String> expectedFailedFilesSet = new TreeSet<>();
        expectedFailedFilesSet.addAll(nonExistentFiles1);
        expectedFailedFilesSet.addAll(nonExistentFiles2);

        // check a completed file has been created
        CompletedRestoresManager completedRestoresManager = new CompletedRestoresManager(cacheDirPath);
        assertTrue("No completed file found", completedRestoresManager.checkCompletedFileExists(preparedId));
        // check the failed file contains the entries we expect in the correct order
        FailedFilesManager failedFilesManager = new FailedFilesManager(cacheDirPath);
        Set<String> actualfailedFilesSet = failedFilesManager.getFailedEntriesForPreparedId(preparedId);
        assertEquals("Returned Set of failed files was different from expected", 
            expectedFailedFilesSet.toString(), actualfailedFilesSet.toString());

        TestUtils.checkFilesOnMainStorage(mainStorage, dfInfosExistingFiles);
    }

    private boolean checkForCompletedFileAndEmptyFailedFile(String preparedId) throws IOException {
        CompletedRestoresManager completedRestoresManager = new CompletedRestoresManager(cacheDirPath);
        boolean completedFileExists = completedRestoresManager.checkCompletedFileExists(preparedId);
        System.out.println("Found completed file for " + preparedId + " : " + completedFileExists);
        FailedFilesManager failedFilesManager = new FailedFilesManager(cacheDirPath);
        Set<String> failedFiles = failedFilesManager.getFailedEntriesForPreparedId(preparedId);
        System.out.println("Failed files for " + preparedId + " contains " + failedFiles.size() + " entries");
        return completedFileExists && failedFiles.size()==0;
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.recursivelyDeleteDirectory(pluginMainDirPath);
        TestUtils.recursivelyDeleteDirectory(cacheDirPath);
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
