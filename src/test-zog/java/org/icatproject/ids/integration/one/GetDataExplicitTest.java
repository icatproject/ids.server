package org.icatproject.ids.integration.one;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GetDataExplicitTest extends BaseTest {

	// value of the offset in bytes
	final Integer goodOffset = 20;
	final Integer badOffset = 99999999;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("one.properties");
		icatsetup();
	}

	@Test(expected = BadRequestException.class)
	public void badPreparedIdFormatTest() throws Exception {
		testingClient.getData("bad preparedId format", null, 0, 403);
	}

	@Test(expected = BadRequestException.class)
	public void badDatafileIdFormatTest() throws Exception {
		parameters.put("sessionId", setup.getGoodSessionId());
		parameters.put("datafileIds", "notADatafile");
		testingClient.process("getData", parameters, Method.GET, ParmPos.URL, null, null, 400);
	}

	@Test(expected = InsufficientPrivilegesException.class)
	public void forbiddenTest() throws Exception {
		populateStorage();
		testingClient.getData(setup.getForbiddenSessionId(),
				new DataSelection().addDatafiles(datafileIds), Flag.NONE, null, 0, 403);
	}

	@Test
	public void correctBehaviourTest() throws Exception {
		populateStorage();
		InputStream stream = testingClient.getData(sessionId,
				new DataSelection().addDatafiles(datafileIds), Flag.NONE, null, 0, 200);
		checkZipStream(stream, datafileIds);

	}



}
