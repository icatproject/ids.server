package org.icatproject.ids.integration.twodf;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.NotFoundException;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetDataForPreparedIdTest extends BaseTest {

	final int goodOffset = 20;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("twodf.properties");
		icatsetup();
	}

	@Test(expected = BadRequestException.class)
	public void badPreparedIdFormatTest() throws Exception {
		try (InputStream z = testingClient.getData("bad preparedId format", 0L, 400)) {
		}
	}

	@Test(expected = BadRequestException.class)
	public void badOffsetFormatTest() throws Exception {

		String preparedId = testingClient.prepareData(sessionId, new DataSelection().addDatafile(datafileIds.get(0)),
				Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		try (InputStream z = testingClient.getData(preparedId, -10L, 400)) {
		}
	}

	@Test(expected = NotFoundException.class)
	public void nonExistentPreparedIdTest() throws Exception {
		try (InputStream z = testingClient.getData("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", 0L, 404)) {
		}
	}

	@Test
	public void correctBehaviourNoOffsetTest() throws Exception {
		String preparedId = testingClient.prepareData(sessionId, new DataSelection().addDatafile(datafileIds.get(0)),
				Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		try (InputStream stream = testingClient.getData(preparedId, 0, 200)) {
			checkStream(stream, datafileIds.get(0));
		}
	}

	@Test
	public void correctBehaviourNoOffsetMultipleDatafilesTest() throws Exception {
		String preparedId = testingClient.prepareData(sessionId, new DataSelection().addDatafiles(datafileIds),
				Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		try (InputStream stream = testingClient.getData(preparedId, 0, 200)) {
			checkZipStream(stream, datafileIds, 57, 0);
		}

	}

	@Test
	public void correctBehaviourNoOffsetWithDatasetTest() throws Exception {

		String preparedId = testingClient.prepareData(sessionId, new DataSelection().addDataset(datasetIds.get(0)),
				Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		try (InputStream stream = testingClient.getData(preparedId, 0, 200)) {
			checkZipStream(stream, datafileIds.subList(0, 2), 57, 0);
		}
	}

	@Test
	public void correctBehaviourNoOffsetWithDatasetAndDatafileTest() throws Exception {

		String preparedId = testingClient.prepareData(sessionId, new DataSelection().addDataset(datasetIds.get(0))
				.addDatafiles(datafileIds), Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		try (InputStream stream = testingClient.getData(preparedId, 0, 200)) {
			checkZipStream(stream, datafileIds, 57, 0);
		}
	}

	@Test
	public void correctBehaviourWithOffsetTest() throws Exception {

		String preparedId = testingClient.prepareData(sessionId, new DataSelection().addDatafile(datafileIds.get(0)),
				Flag.NONE, 200);

		while (!testingClient.isPrepared(preparedId, 200)) {
			Thread.sleep(1000);
		}

		// request the zip file twice, with and without an offset
		byte[] zip = getOutput(testingClient.getData(preparedId, 0, 200));
		byte[] zipoffset = getOutput(testingClient.getData(preparedId, goodOffset, 206));

		// compare the two zip files byte by byte taking into account the offset
		System.out.println(zip.length + " " + zipoffset.length);
		for (int i = 0; i < zipoffset.length; i++) {
			assertEquals("Byte offset: " + i, (byte) zipoffset[i], (byte) zip[i + goodOffset]);
		}
	}

}