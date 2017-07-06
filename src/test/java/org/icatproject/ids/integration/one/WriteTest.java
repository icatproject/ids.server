package org.icatproject.ids.integration.one;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.junit.BeforeClass;
import org.junit.Test;

public class WriteTest extends BaseTest {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("one.properties");
		icatsetup();
	}

	/**
	 * For one level storage, the write call is basically a noop.
	 * Just verify that it does not throw an error.
	 */
	@Test
	public void writeDataset() throws Exception {
		testingClient.write(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 204);
	}

}
