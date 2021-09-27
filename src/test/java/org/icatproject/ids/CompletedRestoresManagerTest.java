package org.icatproject.ids;

import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CompletedRestoresManagerTest {
    
    private static CompletedRestoresManager completedRestoresManager;

    @BeforeClass
	public static void setupClass() throws Exception {
		Path idsCacheDir = Paths.get(System.getProperty("java.io.tmpdir"));
        completedRestoresManager = new CompletedRestoresManager(idsCacheDir, true);
        Files.createDirectory(completedRestoresManager.getCompletedFilesDir());
    }

    @Test
    public void testCreateCompletedFile() throws Exception {
        String preparedId = "preparedId1";
        completedRestoresManager.createCompletedFile(preparedId);
        assertTrue("Could not find completed file", 
                completedRestoresManager.checkCompletedFileExists(preparedId));
        completedRestoresManager.deleteCompletedFile(preparedId);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        // there should just be an empty directory to clean up
        completedRestoresManager.getCompletedFilesDir().toFile().delete();
    }
}
