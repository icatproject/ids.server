package org.icatproject.ids.integration;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import javax.xml.namespace.QName;

import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.TestingUtils;
import org.icatproject.ids.integration.util.TreeDeleteVisitor;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.NotFoundException;
import org.icatproject.ids.integration.util.client.TestingClient;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.icatproject.ids.integration.util.client.TestingClient.Status;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class GetDataForPreparedIdTest {

	private static Setup setup = null;
	@SuppressWarnings("unused")
	private static ICAT icat;
	private TestingClient testingClient;

	final int goodOffset = 20;
	private String sessionId;

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
		// parameters = new HashMap<>();
		sessionId = setup.getGoodSessionId();
	}

	@Test(expected = BadRequestException.class)
	public void badPreparedIdFormatTest() throws Exception {
		testingClient.getData("bad preparedId format", null, 0L, 400);
	}

	@Test(expected = BadRequestException.class)
	public void badFileNameFormatTest() throws Exception {
		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(setup.getDatafileIds().get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		testingClient.getData(preparedId, "this/is/a/bad/file/name", 0L, 400);
	}

	@Test(expected = BadRequestException.class)
	public void badOffsetFormatTest() throws Exception {

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(setup.getDatafileIds().get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		testingClient.getData(preparedId, null, -10L, 400);

	}

	@Test(expected = NotFoundException.class)
	public void nonExistentPreparedIdTest() throws Exception {
		testingClient.getData("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", null, 0L, 404);
	}

	@Ignore
	@Test
	public void offsetTooBigTest() throws Exception {

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(setup.getDatafileIds().get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		InputStream stream = testingClient.getData(preparedId, null, 99999999, 400);
		Map<String, String> map = TestingUtils.filenameMD5Map(stream);
		assertEquals(0, map.size());
		TestingUtils.checkMD5Values(map, setup);
	}

	@Test
	public void correctBehaviourNoOffsetTest() throws Exception {
		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(setup.getDatafileIds().get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		InputStream stream = testingClient.getData(preparedId, null, 0, 200);

		Map<String, String> map = TestingUtils.filenameMD5Map(stream);
		assertEquals(1, map.size());
		TestingUtils.checkMD5Values(map, setup);
	}

	@Test
	public void correctBehaviourNoOffsetMultipleDatafilesTest() throws Exception {
		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafiles(setup.getDatafileIds()), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		InputStream stream = testingClient.getData(preparedId, null, 0, 200);

		Map<String, String> map = TestingUtils.filenameMD5Map(stream);
		assertEquals(4, map.size());
		TestingUtils.checkMD5Values(map, setup);

	}

	@Test
	public void correctBehaviourNoOffsetWithDatasetTest() throws Exception {

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		InputStream stream = testingClient.getData(preparedId, null, 0, 200);

		Map<String, String> map = TestingUtils.filenameMD5Map(stream);
		assertEquals(2, map.size());
		TestingUtils.checkMD5Values(map, setup);

	}

	@Test
	public void correctBehaviourNoOffsetWithDatasetAndDatafileTest() throws Exception {

		String preparedId = testingClient.prepareData(
				sessionId,
				new DataSelection().addDataset(setup.getDatasetIds().get(0)).addDatafiles(
						setup.getDatafileIds()), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		InputStream stream = testingClient.getData(preparedId, null, 0, 200);

		Map<String, String> map = TestingUtils.filenameMD5Map(stream);
		assertEquals(6, map.size());
		TestingUtils.checkMD5Values(map, setup);
	}

	@Test
	public void correctBehaviourWithOffsetTest() throws Exception {

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(setup.getDatafileIds().get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		// request the zip file twice, with and without an offset
		byte[] zip = TestingUtils.getOutput(testingClient.getData(preparedId, null, 0, 200));
		byte[] zipoffset = TestingUtils.getOutput(testingClient.getData(preparedId, null,
				goodOffset, 200));

		// compare the two zip files byte by byte taking into account the offset
		System.out.println(zip.length + " " + zipoffset.length);
		for (int i = 0; i < zipoffset.length; i++) {
			assertEquals("Byte offset: " + i, (byte) zipoffset[i], (byte) zip[i + goodOffset]);
		}
	}

}