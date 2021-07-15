package org.icatproject.ids;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.icatproject.ids.plugin.DsInfo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdsStreamingOutputTest {

    private static final Logger logger = LoggerFactory.getLogger(IdsStreamingOutputTest.class);

    private static Path targetRunPropertiesPath;
    private static Path pluginMainDirPath;
    private static Path cacheDirPath;
	private static Path tmpDirPath = Paths.get(System.getProperty("java.io.tmpdir"));

    private File tempZipFile;

    private static List<String> filePaths;

	@BeforeClass
	public static void setupClass() throws Exception {
        targetRunPropertiesPath = TestUtils.copyUnitTestsRunPropertiesToClasspath();

        pluginMainDirPath = TestUtils.createPluginMainDir();

        cacheDirPath = TestUtils.createCacheDir();

       // create a temporary test directory structure in plugin.main.dir
		int numFiles = 5;
		filePaths = new ArrayList<>();
		for (int i=1; i<=numFiles; i++) {
            String relativeFilePath = "subdir" + i + "/" + "file" + i + ".txt";
			Path path = pluginMainDirPath.resolve(relativeFilePath);
			filePaths.add("/" + relativeFilePath);
			Files.createDirectories(path.getParent());
			OutputStream out = Files.newOutputStream(path);
			byte[] fileBytes = new String("Contents of " + path.getFileName()).getBytes();
			out.write(fileBytes);
			out.close();
		}
	}

	@Test
	public void testZipCreationAllFilesPresent() throws Exception {
		testZipCreation(false);
		assertTrue(checkZipContents(false));
	}

	@Test
	public void testZipCreationSomeFilesMissing() throws Exception {
		testZipCreation(true);
		checkZipContents(true); 
	}

	private void testZipCreation(boolean addMissingFile) throws Exception {
		tempZipFile = tmpDirPath.resolve(IdsStreamingOutputTest.class.getSimpleName() +
            "-" + System.currentTimeMillis() + ".zip").toFile();
		// create a dummy dsInfo for the dsInfos to reference (by ID)
		DsInfo dsInfo1 = new DsInfoImpl(1L, "dsName", "dsLocation", 1L, "invName", "visitId", 1L, "facilityName");
		Map<Long, DsInfo> dsInfos = new HashMap<>();
		dsInfos.put(dsInfo1.getDsId(), dsInfo1);
		// create a set of dfInfos with each referencing the dsInfo created
		Set<DfInfoImpl> dfInfos = new HashSet<>();
		Long dfId = 0L;
		for (String filePath : filePaths) {
			dfId++;
			DfInfoImpl dfInfo = new DfInfoImpl(dfId, null, filePath, null, null, 1L);
			dfInfos.add(dfInfo);		
		}
		if (addMissingFile) {
			// add an extra file path that does not exist to the list of files to be zipped
			dfId++;
			Path missingFilePath = pluginMainDirPath.resolve("non_existent_file.txt");
			DfInfoImpl dfInfo = new DfInfoImpl(dfId, null, missingFilePath.toString(), null, null, 1L);
			dfInfos.add(dfInfo);		
		}
		// create the IdsStreamingOutput class
		IdsStreamingOutput so = new IdsStreamingOutput(dsInfos, dfInfos, 0L, true, true, 0L);
		// write the streamed zip file to the system temp dir
		FileOutputStream fos = new FileOutputStream(tempZipFile);
		so.write(fos);
		fos.close();
	}

	private boolean checkZipContents(boolean shouldContainMissingFilesList) throws IOException {
		boolean missingFilesListFound = false;
		int numZipEntries = 0;
		try (ZipInputStream zis = new ZipInputStream(new FileInputStream(tempZipFile))) {
			ZipEntry entry;
			while ((entry = zis.getNextEntry()) != null) {
				String entryName = "/" + entry.getName();
				if (filePaths.contains(entryName)) {
					logger.debug("Found expected file: " + entryName);
					numZipEntries++;
				} else if (shouldContainMissingFilesList && 
						entryName.equals("/" + PropertyHandler.getInstance().getMissingFilesZipEntryName())) {
					logger.debug("Found missing files listing: " + entryName);
					missingFilesListFound = true;
				} else {
					throw new IOException("Unexpected entry found in zip file: " + entryName);
				}
			}
		}
        if (filePaths.size() == numZipEntries) {
            if (shouldContainMissingFilesList) {
				if (missingFilesListFound) {
                	return true;
				} else {
					return false;
				}
            }
            return true;
        } else {
        	return false;
		}
	}

	@After
	public void tearDown() throws Exception {
		tempZipFile.delete();
	}

	@AfterClass
	public static void tearDownClass() throws Exception {
        Files.deleteIfExists(targetRunPropertiesPath);
        TestUtils.recursivelyDeleteDirectory(pluginMainDirPath);
        TestUtils.recursivelyDeleteDirectory(cacheDirPath);
	}
}