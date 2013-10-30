package org.icatproject.ids.integration;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;

import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.TreeDeleteVisitor;
import org.icatproject.ids.integration.util.client.TestingClient;
import org.junit.Before;

public class BaseTest {

	protected static Setup setup = null;

	protected TestingClient testingClient;
	protected Map<String, String> parameters;
	protected String sessionId;
	protected static ICAT icat;

	public static void icatsetup() throws Exception {
		ICATService icatService = new ICATService(setup.getIcatUrl(), new QName(
				"http://icatproject.org", "ICATService"));
		icat = icatService.getICATPort();
	}

	@Before
	public void clearFastStorage() throws Exception {
		Path storageDir = FileSystems.getDefault().getPath(setup.getStorageDir());
		Path storageZipDir = FileSystems.getDefault().getPath(setup.getStorageZipDir());
		TreeDeleteVisitor treeDeleteVisitor = new TreeDeleteVisitor();
		if (Files.exists(storageDir)) {
			Files.walkFileTree(storageDir, treeDeleteVisitor);
		}
		Files.createDirectories(storageDir);
		if (storageZipDir != null) {
			if (Files.exists(storageZipDir)) {
				Files.walkFileTree(storageZipDir, treeDeleteVisitor);
			}
			Files.createDirectories(storageZipDir);
		}
		testingClient = new TestingClient(setup.getIdsUrl());
		parameters = new HashMap<>();
		sessionId = setup.getGoodSessionId();
	}

}
