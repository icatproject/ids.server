package org.icatproject.ids.integration.two;

import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.junit.BeforeClass;
import org.junit.Test;

public class LinkTest extends BaseTest {

	private static Path here;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
		here = Paths.get("").toAbsolutePath();
	}

	@Test
	public void getLink() throws Exception {

		Path link = here.resolve("alink");
		try {
			testingClient.getLink(sessionId, datafileIds.get(0), link, "glassfish", 403);
			fail("Should have thrown an exception");
		} catch (InsufficientPrivilegesException e) {
			// All is well
		}
	}
}
