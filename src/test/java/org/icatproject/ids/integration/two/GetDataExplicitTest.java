package org.icatproject.ids.integration.two;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.InputStream;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataNotOnlineException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.IdsException;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.icatproject.ids.integration.util.client.TestingClient.Method;
import org.icatproject.ids.integration.util.client.TestingClient.ParmPos;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetDataExplicitTest extends BaseTest {

	// value of the offset in bytes
	final Integer goodOffset = 20;
	final Integer badOffset = 99999999;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
	}

	@Test(expected = BadRequestException.class)
	public void badPreparedIdFormatTest() throws Exception {
		parameters.put("preparedId", "bad preparedId format");
		testingClient.process("getData", parameters, Method.GET, ParmPos.URL, null, null, 400);
	}

	@Test(expected = BadRequestException.class)
	public void badDatafileIdFormatTest() throws Exception {
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("datafileIds", "notADatafile");
		testingClient.process("getData", parameters, Method.GET, ParmPos.URL, null, null, 400);
	}

	@Test(expected = InsufficientPrivilegesException.class)
	public void forbiddenTest() throws Exception {
		parameters.put("sessionId", setup.getForbiddenSessionId());
		parameters.put("datafileIds", datafileIds.toString().replace("[", "").replace("]", "")
				.replace(" ", ""));
		testingClient.process("getData", parameters, Method.GET, ParmPos.URL, null, null, 403);
	}

	@Test
	public void correctBehaviourTest() throws Exception {

		try {
			testingClient.getData(sessionId, new DataSelection().addDatafiles(datafileIds),
					Flag.NONE, null, 0, 404);
			fail("Should have thrown exception");
		} catch (IdsException e) {
			assertEquals(DataNotOnlineException.class, e.getClass());
		}

		InputStream stream;
		while (true) {
			try {
				stream = testingClient.getData(sessionId,
						new DataSelection().addDatafiles(datafileIds), Flag.NONE, null, 0, null);
				break;
			} catch (IdsException e) {
				assertEquals(DataNotOnlineException.class, e.getClass());
				Thread.sleep(1000);
			}
		}

		checkZipStream(stream, datafileIds, 57);
	}

	@Test
	public void gettingDatafileRestoresItsDatasetTest() throws Exception {

		try {
			testingClient.getData(sessionId, new DataSelection().addDatafile(datafileIds.get(2)),
					Flag.NONE, null, 0, null);
			fail("Should have thrown an exception");
		} catch (DataNotOnlineException e) {
			// All is well
		}

		waitForIds();
		InputStream stream = testingClient.getData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(3)), Flag.NONE, null, 0, 200);
		checkStream(stream, datafileIds.get(3));

	}

	@Test
	public void gettingDatafileAndDatasetShouldRestoreBothDatasetsTest() throws Exception {

		try {
			testingClient.getData(sessionId, new DataSelection().addDatafile(datafileIds.get(2))
					.addDataset(datasetIds.get(0)), Flag.NONE, null, 0, 404);
			fail("Should throw exception");
		} catch (DataNotOnlineException e) {
			// All is well
		}

		waitForIds();
		InputStream stream = testingClient.getData(sessionId,
				new DataSelection().addDatasets(datasetIds), Flag.NONE, null, 0, 200);
		checkZipStream(stream, datafileIds, 57);
	}

}
