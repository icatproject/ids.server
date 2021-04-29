package org.icatproject.ids;

import static org.junit.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class FailedFilesManagerTest {
    
    private static FailedFilesManager failedFilesManager;

    private static Set<String> failedFilepathsSet1;
    private static Set<String> failedFilepathsSet2;

    @BeforeClass
	public static void setupClass() throws Exception {
		Path idsCacheDir = Paths.get(System.getProperty("java.io.tmpdir"));
        failedFilesManager = new FailedFilesManager(idsCacheDir, true);
        Files.createDirectory(failedFilesManager.getFailedFilesDir());

        failedFilepathsSet1 = new HashSet<>();
        failedFilepathsSet1.add("/tmp/subdir5/myfile5");
        failedFilepathsSet1.add("/tmp/subdir2/myfile2");
        failedFilepathsSet1.add("/tmp/subdir1/myfile1");
        failedFilepathsSet1.add("/tmp/subdir4/myfile4");
        failedFilepathsSet1.add("/tmp/subdir3/myfile3");
        
        failedFilepathsSet2 = new HashSet<>();
        failedFilepathsSet2.add("/tmp/mysubdir5/myfile5");
        failedFilepathsSet2.add("/tmp/subdir1/myfile2");
        failedFilepathsSet2.add("/tmp/subdir6/myfile6");
    }

    @Test
    public void testCreateInitialFailedFilesFile() throws Exception {
        String preparedId = "preparedId1";
        failedFilesManager.writeToFailedEntriesFile(preparedId, failedFilepathsSet1);
        Set<String> sortedFailedFilepathsSet = failedFilesManager.getFailedEntriesForPreparedId(preparedId);
        Set<String> expectedFilepathSet = new TreeSet<>();
        expectedFilepathSet.addAll(failedFilepathsSet1); 
        assertEquals("Set of failed file paths were not as expected", expectedFilepathSet.toString(), sortedFailedFilepathsSet.toString());
        failedFilesManager.deleteFailedFile(preparedId);
    }

    @Test
    public void testCreateExtendedFailedFilesFile() throws Exception {
        String preparedId = "preparedId2";
        failedFilesManager.writeToFailedEntriesFile(preparedId, failedFilepathsSet1);
        failedFilesManager.writeToFailedEntriesFile(preparedId, failedFilepathsSet2);
        Set<String> sortedFailedFilepathsSet = failedFilesManager.getFailedEntriesForPreparedId(preparedId);
        Set<String> expectedFilepathSet = new TreeSet<>();
        expectedFilepathSet.addAll(failedFilepathsSet1); 
        expectedFilepathSet.addAll(failedFilepathsSet2); 
        assertEquals("Set of failed file paths were not as expected", expectedFilepathSet.toString(), sortedFailedFilepathsSet.toString());
        failedFilesManager.deleteFailedFile(preparedId);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        // there should just be an empty directory to clean up
        failedFilesManager.getFailedFilesDir().toFile().delete();
    }

}
