package org.icatproject.ids.integration.one;

import java.io.InputStream;
import java.util.Arrays;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.icatproject.ids.integration.util.client.TestingClient.Method;
import org.icatproject.ids.integration.util.client.TestingClient.ParmPos;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetDataExplicitTest extends BaseTest {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("one.properties");
		icatsetup();
	}

	@Test(expected = BadRequestException.class)
	public void badPreparedIdFormatTest() throws Exception {
		testingClient.getData("bad preparedId format", null, 0, 400);
	}

	@Test(expected = BadRequestException.class)
	public void badDatafileIdFormatTest() throws Exception {
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("datafileIds", "notADatafile");
		testingClient.process("getData", parameters, Method.GET, ParmPos.URL, null, null, 400);
	}

	@Test(expected = InsufficientPrivilegesException.class)
	public void forbiddenTest() throws Exception {
		testingClient.getData(setup.getForbiddenSessionId(),
				new DataSelection().addDatafiles(datafileIds), Flag.NONE, null, 0, 403);
	}

	@Test
	public void correctBehaviourTestNone() throws Exception {
		InputStream stream = testingClient.getData(sessionId,
				new DataSelection().addDatafiles(datafileIds), Flag.NONE, null, 0, 200);
		checkZipStream(stream, datafileIds, 57L);

		stream = testingClient.getData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.NONE, null, 0, 200);
		checkStream(stream, datafileIds.get(0));
	}

	@Test
	public void correctBehaviourTestCompress() throws Exception {
		InputStream stream = testingClient.getData(sessionId,
				new DataSelection().addDatafiles(datafileIds), Flag.COMPRESS, null, 0, 200);
		checkZipStream(stream, datafileIds, 36L);

		stream = testingClient.getData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.COMPRESS, null, 0, 200);
		checkStream(stream, datafileIds.get(0));
	}

	@Test
	public void correctBehaviourTestZip() throws Exception {
		InputStream stream = testingClient.getData(sessionId,
				new DataSelection().addDatafiles(datafileIds), Flag.ZIP, null, 0, 200);
		checkZipStream(stream, datafileIds, 57L);

		stream = testingClient.getData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.ZIP, null, 0, 200);
		checkZipStream(stream, datafileIds.subList(0, 1), 57L);
	}

	@Test
	public void correctBehaviourTestZipAndCompress() throws Exception {
		InputStream stream = testingClient.getData(sessionId,
				new DataSelection().addDatafiles(datafileIds), Flag.ZIP_AND_COMPRESS, null, 0, 200);
		checkZipStream(stream, datafileIds, 36L);

		stream = testingClient.getData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.ZIP_AND_COMPRESS, null,
				0, 200);
		checkZipStream(stream, datafileIds.subList(0, 1), 36L);
	}

	@Test
	public void correctBehaviourInvestigation() throws Exception {
		InputStream stream = testingClient.getData(sessionId,
				new DataSelection().addInvestigation(investigationId), Flag.NONE, null, 0, 200);
		checkZipStream(stream, datafileIds, 57L);

		stream = testingClient.getData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.ZIP, null, 0, 200);
		checkZipStream(stream, datafileIds.subList(0, 1), 57L);
	}

	@Test
	public void correctBehaviourInvestigations() throws Exception {
		InputStream stream = testingClient.getData(sessionId,
				new DataSelection().addInvestigations(Arrays.asList(investigationId)), Flag.NONE,
				null, 0, 200);
		checkZipStream(stream, datafileIds, 57L);

		stream = testingClient.getData(sessionId,
				new DataSelection().addDatafile(datafileIds.get(0)), Flag.ZIP, null, 0, 200);
		checkZipStream(stream, datafileIds.subList(0, 1), 57L);
	}

}
