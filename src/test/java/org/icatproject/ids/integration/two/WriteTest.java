package org.icatproject.ids.integration.two;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.DatasetType;
import org.icatproject.Investigation;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataNotOnlineException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.junit.BeforeClass;
import org.junit.Test;

public class WriteTest extends BaseTest {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
	}

	/**
	 * In principle, it's always possible to do a write call on
	 * existing datasets, but it will have no visible effect.
	 */
	@Test
	public void restoreThenWriteDataset() throws Exception {

		Long dsId = datasetIds.get(0);
		Path dirOnFastStorage = getDirOnFastStorage(dsId);
		DataSelection selection = new DataSelection().addDataset(dsId);

		testingClient.restore(sessionId, selection, 204);
		waitForIds();
		checkPresent(dirOnFastStorage);

		testingClient.write(sessionId, selection, 204);

	}

	/**
	 * Create a dataset in ICAT, store the files in main storage,
	 * then do a write call to IDS to get the dataset written to
	 * archive storage.
	 */
	@Test
	public void storeThenWrite() throws Exception {

		long timestamp = System.currentTimeMillis();

		Investigation inv = (Investigation) icatWS.get(sessionId, "Investigation INCLUDE Facility", investigationId);
		String invLoc = inv.getId() + "/";
		DatasetType dsType = (DatasetType) icatWS.search(sessionId, "DatasetType").get(0);

		Dataset ds = new Dataset();
		ds.setName("dsWrite_" + timestamp);
		ds.setComplete(false);
		ds.setType(dsType);
		ds.setInvestigation(inv);
		ds.setId(icatWS.create(sessionId, ds));
		String dsLoc = invLoc + ds.getId() + "/";

		Datafile df = new Datafile();
		df.setName("dfWrite_" + timestamp);
		df.setLocation(dsLoc + UUID.randomUUID());
		df.setDataset(ds);
		writeToFile(df, "some really boring datafile test content", setup.getKey());

		Long dsId = ds.getId();
		Path dirOnFastStorage = getDirOnFastStorage(dsId);
		Path fileOnArchiveStorage = getFileOnArchiveStorage(dsId);
		DataSelection selection = new DataSelection().addDataset(dsId);

		checkPresent(dirOnFastStorage);

		testingClient.write(sessionId, selection, 204);
		waitForIds();

		ArrayList<Long> list = new ArrayList<Long>();
		list.add(df.getId());
		checkZipFile(fileOnArchiveStorage, list, 42);

	}

	/**
	 * Write fails if the dataset is not online.
	 */
	@Test(expected = DataNotOnlineException.class)
	public void notOnlineTest() throws Exception {
		Long dsId = datasetIds.get(0);
		Path dirOnFastStorage = getDirOnFastStorage(dsId);
		DataSelection selection = new DataSelection().addDataset(dsId);
		testingClient.archive(sessionId, selection, 204);
		waitForIds();
		checkAbsent(dirOnFastStorage);
		testingClient.write(sessionId, selection, 404);
	}

	@Test(expected = BadRequestException.class)
	public void badSessionIdFormatTest() throws Exception {
		testingClient.write("bad sessionId format",
				new DataSelection().addDatafiles(Arrays.asList(1L, 2L)), 400);
	}

	@Test
	public void noIdsTest() throws Exception {
		testingClient.write(sessionId, new DataSelection(), 204);
	}

	@Test(expected = InsufficientPrivilegesException.class)
	public void nonExistingSessionIdTest() throws Exception {
		testingClient.write("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
				new DataSelection().addDatafiles(Arrays.asList(1L, 2L)), 403);
	}

}
