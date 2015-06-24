package org.icatproject.ids.integration.two;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.junit.BeforeClass;
import org.junit.Test;

public class BaseTests extends BaseTest {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
	}

	@Test
	public void raceTest() throws Exception {
		super.raceTest();
	}

	@Test
	public void getIcatUrlTest() throws Exception {
		super.getIcatUrlTest();
	}

	@Test
	public void apiVersionTest() throws Exception {
		super.apiVersionTest();
	}

}