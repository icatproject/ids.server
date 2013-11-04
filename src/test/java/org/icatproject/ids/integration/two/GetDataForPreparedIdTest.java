package org.icatproject.ids.integration.two;

import static org.junit.Assert.fail;

import java.io.InputStream;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.NotFoundException;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.icatproject.ids.integration.util.client.TestingClient.Status;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetDataForPreparedIdTest extends BaseTest {

	final int goodOffset = 20;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
	}

	@Test(expected = BadRequestException.class)
	public void badPreparedIdFormatTest() throws Exception {
		testingClient.getData("bad preparedId format", null, 0L, 400);
	}

	@Test(expected = BadRequestException.class)
	public void badOffsetFormatTest() throws Exception {

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.NONE, 200);

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

	@Test
	public void correctBehaviourNoOffsetTest() throws Exception {
		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		InputStream stream = testingClient.getData(preparedId, null, 0, 200);

		// TODO Map<String, String> map = TestingUtils.filenameMD5Map(stream);
		// assertEquals(1, map.size());
		// TestingUtils.checkMD5Values(map, setup);
		fail();
	}

	@Test
	public void correctBehaviourNoOffsetMultipleDatafilesTest() throws Exception {
		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafiles(datafileIds), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		InputStream stream = testingClient.getData(preparedId, null, 0, 200);

		// TODO Map<String, String> map = TestingUtils.filenameMD5Map(stream);
		// assertEquals(1, map.size());
		// TestingUtils.checkMD5Values(map, setup);
		fail();

	}

	@Test
	public void correctBehaviourNoOffsetWithDatasetTest() throws Exception {

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		InputStream stream = testingClient.getData(preparedId, null, 0, 200);

		// TODO Map<String, String> map = TestingUtils.filenameMD5Map(stream);
		// assertEquals(1, map.size());
		// TestingUtils.checkMD5Values(map, setup);
		fail();

	}

	@Test
	public void correctBehaviourNoOffsetWithDatasetAndDatafileTest() throws Exception {

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDatafiles(datafileIds),
				Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		InputStream stream = testingClient.getData(preparedId, null, 0, 200);

		// TODO Map<String, String> map = TestingUtils.filenameMD5Map(stream);
		// assertEquals(1, map.size());
		// TestingUtils.checkMD5Values(map, setup);
		fail();
	}

	@Test
	public void correctBehaviourWithOffsetTest() throws Exception {

		String preparedId = testingClient.prepareData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.NONE, 200);

		Status status = null;
		do {
			Thread.sleep(1000);
			status = testingClient.getStatus(preparedId, 200);
		} while (Status.RESTORING.equals(status));

		// // request the zip file twice, with and without an offset
		// TODO byte[] zip = TestingUtils.getOutput(testingClient.getData(preparedId, null, 0,
		// 200));
		// byte[] zipoffset = TestingUtils.getOutput(testingClient.getData(preparedId, null,
		// goodOffset, 206));
		//
		// // compare the two zip files byte by byte taking into account the offset
		// System.out.println(zip.length + " " + zipoffset.length);
		// for (int i = 0; i < zipoffset.length; i++) {
		// assertEquals("Byte offset: " + i, (byte) zipoffset[i], (byte) zip[i + goodOffset]);
		// }
		fail();
	}

}