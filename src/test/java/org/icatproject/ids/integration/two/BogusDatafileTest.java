package org.icatproject.ids.integration.two;

import java.io.InputStream;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.NotFoundException;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.icatproject.ids.integration.util.client.TestingClient.Status;

/*
 * Issue #63: Internal error is raised trying to restore a dataset
 * with datafiles not uploaded to IDS
 *
 * ids.server gets confused if datafiles do not exist in the storage,
 * e.g. they are created in ICAT without having location set.
 *
 * Desired behavior: such bogus datafiles should be ignored.
 */

public class BogusDatafileTest extends BaseTest {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
	}

	@Before
	public void createBogusFiles() throws Exception {
		long timestamp = System.currentTimeMillis();

		Dataset ds1 = (Dataset) icatWS.get(sessionId, "Dataset", datasetIds.get(0));
		Datafile dfb1 = new Datafile();
		dfb1.setName("dfbogus1_" + timestamp);
		dfb1.setFileSize(42L);
		dfb1.setDataset(ds1);
		dfb1.setId(icatWS.create(sessionId, dfb1));

		Dataset ds2 = (Dataset) icatWS.get(sessionId, "Dataset", datasetIds.get(1));
		Datafile dfb2 = new Datafile();
		dfb2.setName("dfbogus2_" + timestamp);
		dfb2.setFileSize(42L);
		dfb2.setDataset(ds2);
		dfb2.setId(icatWS.create(sessionId, dfb2));

		Dataset ds3 = (Dataset) icatWS.get(sessionId, "Dataset", datasetIds.get(2));
		Datafile dfb3 = new Datafile();
		dfb3.setName("dfbogus3_" + timestamp);
		dfb3.setFileSize(42L);
		dfb3.setDataset(ds3);
		dfb3.setId(icatWS.create(sessionId, dfb3));

		datafileIds.add(dfb1.getId());
		datafileIds.add(dfb2.getId());
		datafileIds.add(dfb3.getId());
	}

	@Test
	public void getEmptyDataset() throws Exception {

		DataSelection selection = new DataSelection().addDataset(datasetIds.get(2));

		try (InputStream stream = testingClient.getData(sessionId, selection, Flag.NONE, 0, null)) {
			checkZipStream(stream, Collections.<Long>emptyList(), 57, 0);
		}

	}

	@Test
	public void getSizeEmptyDataset() throws Exception {

		DataSelection selection = new DataSelection().addDataset(datasetIds.get(2));
		assertEquals(0L, testingClient.getSize(sessionId, selection, 200));

	}

	@Test
	public void getNonEmptyDataset() throws Exception {

		DataSelection selection = new DataSelection().addDataset(datasetIds.get(0));

		testingClient.restore(sessionId, selection, 204);

		waitForIds();
		assertEquals(Status.ONLINE, testingClient.getStatus(sessionId, selection, null));

		try (InputStream stream = testingClient.getData(sessionId, selection, Flag.NONE, 0, null)) {
			checkZipStream(stream, datafileIds.subList(0, 2), 57, 0);
		}

	}

	@Test
	public void getSizeNonEmptyDataset() throws Exception {

		DataSelection selection = new DataSelection().addDataset(datasetIds.get(0));
		assertEquals(104L, testingClient.getSize(sessionId, selection, 200));

	}

	@Test(expected = NotFoundException.class)
	public void getBogusFile() throws Exception {

		DataSelection selection = new DataSelection().addDatafile(datafileIds.get(5));

		testingClient.restore(sessionId, selection, 404);

		waitForIds();
		assertEquals(Status.ONLINE, testingClient.getStatus(sessionId, selection, 404));

		try (InputStream stream = testingClient.getData(sessionId, selection, Flag.NONE, 0, 404)) {
			checkZipStream(stream, Collections.<Long>emptyList(), 57, 0);
		}

	}

	@Test(expected = NotFoundException.class)
	public void getSizeBogusFile() throws Exception {

		DataSelection selection = new DataSelection().addDatafile(datafileIds.get(5));
		testingClient.getSize(sessionId, selection, 404);

	}

}
