package org.icatproject.ids.integration;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;

import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.TreeDeleteVisitor;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.icatproject.ids.integration.util.client.TestingClient;
import org.icatproject.ids.integration.util.client.TestingClient.Method;
import org.icatproject.ids.integration.util.client.TestingClient.ParmPos;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ArchiveTest {

	private static Setup setup;
	private static ICAT icat;
	private TestingClient testingClient;
	private String sessionId;
	private Map<String, String> parameters;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup();
		final ICATService icatService = new ICATService(setup.getIcatUrl(), new QName(
				"http://icatproject.org", "ICATService"));
		icat = icatService.getICATPort();
	}

	@Before
	public void clearFastStorage() throws Exception {
		Path storageDir = FileSystems.getDefault().getPath(setup.getStorageDir());
		Path storageZipDir = FileSystems.getDefault().getPath(setup.getStorageZipDir());
		TreeDeleteVisitor treeDeleteVisitor = new TreeDeleteVisitor();
		Files.walkFileTree(storageDir, treeDeleteVisitor);
		Files.walkFileTree(storageZipDir, treeDeleteVisitor);
		Files.createDirectories(storageDir);
		Files.createDirectories(storageZipDir);
		testingClient = new TestingClient(setup.getIdsUrl());
		parameters = new HashMap<>();
		sessionId = setup.getGoodSessionId();
	}

	@Test
	public void restoreThenArchiveDataset() throws Exception {
		Dataset icatDs = (Dataset) icat.get(sessionId, "Dataset", setup.getDatasetIds().get(0));
		File dirOnFastStorage = new File(setup.getStorageDir(), icatDs.getLocation());
		File zipOnFastStorage = new File(new File(setup.getStorageZipDir(), icatDs.getLocation()),
				"files.zip");

		testingClient.restore(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)), 200);
		do {
			Thread.sleep(1000);
		} while (!dirOnFastStorage.exists() || !zipOnFastStorage.exists());
		assertTrue("File " + dirOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", dirOnFastStorage.exists());
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been restored, but doesn't exist", zipOnFastStorage.exists());

		testingClient.archive(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)), 200);
		while (dirOnFastStorage.listFiles().length > 0 || zipOnFastStorage.exists()) {
			Thread.sleep(1000);
		}
		assertTrue("Directory " + dirOnFastStorage.getAbsolutePath()
				+ " should have been cleaned, but still contains files",
				dirOnFastStorage.listFiles().length == 0);
		assertTrue("Zip in " + zipOnFastStorage.getAbsolutePath()
				+ " should have been archived, but still exists", !zipOnFastStorage.exists());
	}

	@Test(expected = BadRequestException.class)
	public void badSessionIdFormatTest() throws Exception {
		testingClient.archive("bad sessionId format",
				new DataSelection().addDatafiles(Arrays.asList(1L, 2L)), 400);
	}

	@Test(expected = BadRequestException.class)
	public void badDatafileIdFormatTest() throws Exception {
		parameters.put("sessionId", sessionId);
		parameters.put("datafileIds", "1,2,a");
		testingClient.process("restore", parameters, Method.POST, ParmPos.BODY, null, 400);
	}

	@Test(expected = BadRequestException.class)
	public void badDatasetIdFormatTest() throws Exception {

		parameters.put("sessionId", sessionId);
		parameters.put("datafileIds", "");
		testingClient.process("restore", parameters, Method.POST, ParmPos.BODY, null, 400);
	}

	@Test(expected = BadRequestException.class)
	public void noIdsTest() throws Exception {
		testingClient.archive(sessionId, new DataSelection(), 400);
	}

	@Test(expected = InsufficientPrivilegesException.class)
	public void nonExistingSessionIdTest() throws Exception {
		testingClient.archive("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
				new DataSelection().addDatafiles(Arrays.asList(1L, 2L)), 403);
	}

}
