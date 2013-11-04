package org.icatproject.ids.integration.one;

import static org.junit.Assert.assertEquals;

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
		setup = new Setup("one.properties");
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
			Thread.sleep(500);
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

		for (Flag flag : Flag.values()) {

			String preparedId = testingClient.prepareData(sessionId,
					new DataSelection().addDatafile(datafileIds.get(0)), flag, 200);

			Status status = null;
			do {
				Thread.sleep(1000);
				status = testingClient.getStatus(preparedId, 200);
			} while (Status.RESTORING.equals(status));

			InputStream stream = testingClient.getData(preparedId, null, 0, 200);

			if (flag == Flag.NONE || flag == Flag.COMPRESS) {
				checkStream(stream, datafileIds.get(0));
			} else if (flag == Flag.ZIP) {
				checkZipStream(stream, datafileIds.subList(0, 1), 57);
			} else {
				checkZipStream(stream, datafileIds.subList(0, 1), 36);
			}
		}
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
		checkZipStream(stream, datafileIds, 57);
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
		checkZipStream(stream, datafileIds.subList(0, 2), 57);
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
		checkZipStream(stream, datafileIds, 57);
	}

	@Test
	public void correctBehaviourWithOffsetTest() throws Exception {

		for (Flag flag : Flag.values()) {

			String preparedId = testingClient.prepareData(sessionId,
					new DataSelection().addDatafile(datafileIds.get(0)), flag, 200);

			Status status = null;
			do {
				Thread.sleep(1000);
				status = testingClient.getStatus(preparedId, 200);
			} while (Status.RESTORING.equals(status));

			// request the file twice, with and without an offset
			byte[] out = getOutput(testingClient.getData(preparedId, null, 0, 200));
			byte[] outOffset = getOutput(testingClient.getData(preparedId, null, goodOffset, 206));

			// compare the two zip files byte by byte taking into account the offset
			System.out.println(flag + ": " + out.length + " " + outOffset.length);
			for (int i = 0; i < outOffset.length; i++) {
				assertEquals("Byte offset: " + i, (byte) outOffset[i], (byte) out[i + goodOffset]);
			}
		}

	}

}