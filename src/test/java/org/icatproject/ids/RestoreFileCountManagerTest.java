package org.icatproject.ids;

import static org.junit.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.icatproject.ids.exceptions.NotFoundException;
import org.junit.Test;

public class RestoreFileCountManagerTest {
    
    @Test
    public void testAddEntryAndRemove() throws Exception {
        // note that for this test the preparedDir does not need to exist
        // it just needs to not be null
		Path preparedDir = Paths.get(System.getProperty("java.io.tmpdir")).resolve(
                this.getClass().getSimpleName() + "-" + System.currentTimeMillis());
        RestoreFileCountManager restoreFileCountManager = RestoreFileCountManager.getTestInstance(preparedDir);
        String preparedId = "preparedId1";
        int fileCount = 1000;
        restoreFileCountManager.addEntryToMap(preparedId, fileCount);
        assertEquals("Value returned from map was not as expected", fileCount, restoreFileCountManager.getFileCount(preparedId));
        restoreFileCountManager.removeEntryFromMap(preparedId);
        try{
            restoreFileCountManager.getFileCount(preparedId);
        } catch (Exception e) {
            System.out.println(e);
            assertEquals("Exception type was not as expected", e.getClass(), NotFoundException.class);
        }
    }

    @Test
    public void testGetFileCountFromPreparedFile() throws Exception {
		Path preparedDir = Paths.get(System.getProperty("java.io.tmpdir")).resolve(
                this.getClass().getSimpleName() + "-" + System.currentTimeMillis());
        Files.createDirectories(preparedDir);
        String preparedId = "a70cd70c-083a-4cad-a0cd-b80276ed433b";
        // copy the prepared file into the temporary preparedDir
        Path sourcePreparedFilePath = Paths.get(System.getProperty("user.dir")).resolve("src/test/resources/" + preparedId);
        Path targetPreparedFilePath = preparedDir.resolve(preparedId);
        if (sourcePreparedFilePath.toFile().exists()) {
            Files.copy(sourcePreparedFilePath, targetPreparedFilePath, StandardCopyOption.REPLACE_EXISTING);    
        }
        RestoreFileCountManager restoreFileCountManager = RestoreFileCountManager.getTestInstance(preparedDir);
        int fileCount = restoreFileCountManager.getFileCount(preparedId);
        assertEquals("File count from prepared file was not as expected", 30, fileCount);
        TestUtils.recursivelyDeleteDirectory(preparedDir);
    }

}
