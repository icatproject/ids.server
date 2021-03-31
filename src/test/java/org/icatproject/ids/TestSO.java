package org.icatproject.ids;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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

import org.icatproject.ids.IdsBean.SO;
import org.icatproject.ids.LockManager.Lock;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

/**
 * Class to test the implementation of the SO (StreamingOutput) inner class in 
 * IdsBean which creates the zip files that are downloaded from the IDS 
 * containing the files which the user requested.
 *  
 * The tests aim to check two things:
 * 
 * 1) that the original download functionality of the IDS is not altered by the
 *    addition of the allowRestoreFailures option. The zip file containing the 
 *    files requested should be returned if they all exist on main storage, or
 *    an IOException should be thrown if any are missing. 
 * 2) that the allowRestoreFailures functionality works as intended when 
 *    enabled. If all files requested are present on the main storage then a 
 *    zip file containing them should be returned. If any files were not found
 *    on the main storage, then those files should be listed in an additional
 *    file which also gets added to the zip. The default name of the file is
 *    MISSING_FILES.txt and will be placed in the root of the zip file but the
 *    property missingFilesZipEntryName can be set to another filename, with
 *    path if desired.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestSO {

	@Mock
	private PropertyHandler propertyHandler;
	@Mock
	private MainStorageInterface mainStorage;
	@Mock
	private ZipMapperInterface zipMapper;
	@Mock
	private Transmitter transmitter;
	@Mock
	private Lock lock;
	@InjectMocks
	private IdsBean idsBean;

	private static Path tmpDirPath = Paths.get(System.getProperty("java.io.tmpdir"));
	private static Path tmpTestDirPath = tmpDirPath.resolve("IDS_TestSO_testing");
	private File tempZipFile;

	@BeforeClass
	public static void setupClass() throws Exception {
		// create a temporary test directory structure containing a few files
		int numFiles = 5;
		List<Path> pathList = new ArrayList<>();
		for (int i=1; i<=numFiles; i++) {
			Path path = tmpTestDirPath.resolve("subdir" + i + "/" + "file" + i + ".txt");
			pathList.add(path);
			Files.createDirectories(path.getParent());
			OutputStream out = Files.newOutputStream(path);
			byte[] fileBytes = new String("Contents of " + path.getFileName()).getBytes();
			out.write(fileBytes);
			out.close();
		}
	}

	@Before
	public void setup() throws Exception {
		when(mainStorage.get(any(), any(), any())).thenAnswer(new Answer<InputStream>() {
			public InputStream answer(InvocationOnMock invocation) throws Throwable {
				String location = invocation.getArgument(0);
				return new FileInputStream(location);
			}
		});

		when(zipMapper.getFullEntryName(any(), any())).thenAnswer(new Answer<String>() {
			public String answer(InvocationOnMock invocation) throws Throwable {
				DfInfo dfInfo = invocation.getArgument(1);
				return dfInfo.getDfLocation();
			}
		});

		when(propertyHandler.getMissingFilesZipEntryName()).thenReturn("MISSING_FILES.txt");
	}

	// test that the pre-getAllowRestoreFailures behaviour still works
	// ie. a zip file is created when all files are present
	@Test
	public void testSuccessfulNormalZipCreation() throws Exception {
		when(propertyHandler.getAllowRestoreFailures()).thenReturn(false);
		List<Path> pathList = testZipCreation(false);
		assertTrue(checkZipContents(pathList, false));
	}

	// test that the pre-getAllowRestoreFailures behaviour still works
	// ie. an exception is thrown if any files are not found on main storage
	@Test(expected = IOException.class)
	public void testFailedNormalZipCreation() throws Exception {
		when(propertyHandler.getAllowRestoreFailures()).thenReturn(false);
		testZipCreation(true);
	}

	// test that the getAllowRestoreFailures functionality works if there are no files missing
	@Test
	public void testMissingFilesZipCreationNoneMissing() throws Exception {
		when(propertyHandler.getAllowRestoreFailures()).thenReturn(true);
		List<Path> pathList = testZipCreation(false);
		assertTrue(checkZipContents(pathList, false)); 
	}

	// test that the getAllowRestoreFailures functionality works when there are files missing
	@Test
	public void testMissingFilesZipCreationFilesMissing() throws Exception {
		when(propertyHandler.getAllowRestoreFailures()).thenReturn(true);
		List<Path> pathList = testZipCreation(true);
		assertTrue(checkZipContents(pathList, true)); 
	}

	// test that the getAllowRestoreFailures functionality works when there are files missing
	// and additionally check that a file path to the missing files listing can be used
	@Test
	public void testMissingFilesZipCreationFilesMissingWithPath() throws Exception {
		when(propertyHandler.getAllowRestoreFailures()).thenReturn(true);
		when(propertyHandler.getMissingFilesZipEntryName()).thenReturn("path/to/MISSING_FILES2.txt");
		List<Path> pathList = testZipCreation(true);
		assertTrue(checkZipContents(pathList, true)); 
	}

	private List<Path> testZipCreation(boolean addMissingFile) throws Exception {
		tempZipFile = tmpDirPath.resolve("IDS_TestSO_" + System.currentTimeMillis() + ".zip").toFile();
		// create a list of the files created in the class setup 
		List<Path> pathList = new ArrayList<>();
		Files.walk(tmpTestDirPath)
        	.filter(Files::isRegularFile)
        	.forEach(pathList::add);
		// create a dummy dsInfo for the dsInfos to reference (by ID)
		DsInfo dsInfo1 = new DsInfoImpl(1L, "dsName", "dsLocation", 1L, "invName", "visitId", 1L, "facilityName");
		Map<Long, DsInfo> dsInfos = new HashMap<>();
		dsInfos.put(dsInfo1.getDsId(), dsInfo1);
		// create a set of dfInfos with each referencing the dsInfo created
		Set<DfInfoImpl> dfInfos = new HashSet<>();
		Long dfId = 0L;
		for (Path path : pathList) {
			dfId++;
			DfInfoImpl dfInfo = new DfInfoImpl(dfId, null, path.toString(), null, null, 1L);
			dfInfos.add(dfInfo);		
		}
		if (addMissingFile) {
			// add an extra file path that does not exist to the list of files to be zipped
			dfId++;
			Path missingFilePath = tmpTestDirPath.resolve("non_existent_file.txt");
			DfInfoImpl dfInfo = new DfInfoImpl(dfId, null, missingFilePath.toString(), null, null, 1L);
			dfInfos.add(dfInfo);		
		}
		// create the IdsBean SO (StreamingOutput) inner class
		SO so = idsBean.new SO(dsInfos, dfInfos, 0L, true, true, lock, 0L, "ip", 0L);
		// write the streamed zip file to the system temp dir
		FileOutputStream fos = new FileOutputStream(tempZipFile);
		so.write(fos);
		fos.close();
		return pathList;
	}

	private boolean checkZipContents(List<Path> pathList, boolean shouldContainMissingFilesList) throws IOException {
		if (shouldContainMissingFilesList) {
			pathList.add(Paths.get(propertyHandler.getMissingFilesZipEntryName()));
		}
		int origPathListSize = pathList.size();
		ZipInputStream zis = new ZipInputStream(new FileInputStream(tempZipFile));
		int numZipEntries = 0;
		ZipEntry entry;
		while ((entry = zis.getNextEntry()) != null) {
			numZipEntries++;
			pathList.remove(Paths.get(entry.getName()));
		}
		zis.close();
		if (origPathListSize == numZipEntries && pathList.size() == 0) {
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
		// delete the test directory structure
		// note the sorting is important to ensure that the 
		// parent directory is empty when it gets deleted
		Files.walk(tmpTestDirPath)
                .map(Path::toFile)
                .sorted((o1, o2) -> -o1.compareTo(o2))
                .forEach(File::delete);
	}
}