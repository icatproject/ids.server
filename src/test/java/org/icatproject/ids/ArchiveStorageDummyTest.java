package org.icatproject.ids;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.storage.ArchiveStorageDummy;
import org.junit.Test;

import uk.ac.stfc.storaged.DfInfoWithLocation;
import uk.ac.stfc.storaged.MainSDStorage;

public class ArchiveStorageDummyTest {

    @Test
    public void testRestoreFilesAllAvailable() throws Exception {
		Path pluginMainDirPath = Paths.get(System.getProperty("java.io.tmpdir")).resolve(
                this.getClass().getSimpleName() + "-" + System.currentTimeMillis());
		Files.createDirectories(pluginMainDirPath);
        ArchiveStorageDummy archiveStorageDummy = new ArchiveStorageDummy(null);
        Properties props = new Properties();
        props.setProperty("plugin.main.dir", pluginMainDirPath.toString());
        MainSDStorage mainStorage = new MainSDStorage(props);
        List<DfInfo> dfInfos = archiveStorageDummy.createDfInfosList(0, 10);
        Set<DfInfo> failedFiles = archiveStorageDummy.restore(mainStorage, dfInfos);
        assertEquals(0, failedFiles.size());
        TestUtils.checkFilesOnMainStorage(mainStorage, dfInfos);
        TestUtils.recursivelyDeleteDirectory(pluginMainDirPath);
    }

    @Test
    public void testRestoreFilesNotAllAvailable() throws Exception {
		Path pluginMainDirPath = Paths.get(System.getProperty("java.io.tmpdir")).resolve(
                this.getClass().getSimpleName() + "-" + System.currentTimeMillis());
		Files.createDirectories(pluginMainDirPath);
        ArchiveStorageDummy archiveStorageDummy = new ArchiveStorageDummy(null);
        Properties props = new Properties();
        props.setProperty("plugin.main.dir", pluginMainDirPath.toString());
        MainSDStorage mainStorage = new MainSDStorage(props);
        List<DfInfo> dfInfosExistingFiles = archiveStorageDummy.createDfInfosList(0, 10);
        Set<String> nonExistentFiles = new HashSet<>();
        nonExistentFiles.add("/non/existent/file1.txt");
        nonExistentFiles.add("/non/existent/file2.txt");
        nonExistentFiles.add("/non/existent/file3.txt");
        List<DfInfo> dfInfosToRequest = new ArrayList<>();
        dfInfosToRequest.addAll(dfInfosExistingFiles); 
        for (String filePath : nonExistentFiles) {
            dfInfosToRequest.add(new DfInfoWithLocation(Paths.get(filePath)));
        }
        Set<DfInfo> failedFiles = archiveStorageDummy.restore(mainStorage, dfInfosToRequest);
        Set<String> failedFilesSet = new HashSet<>();
        for (DfInfo dfInfo : failedFiles) {
            failedFilesSet.add(dfInfo.getDfLocation());
        }
        assertTrue("Returned Set of failed files was different from expected", 
                failedFilesSet.containsAll(nonExistentFiles));
        TestUtils.checkFilesOnMainStorage(mainStorage, dfInfosExistingFiles);
        TestUtils.recursivelyDeleteDirectory(pluginMainDirPath);
    }

}
