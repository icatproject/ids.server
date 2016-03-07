package org.icatproject.ids.integration.one;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.junit.BeforeClass;
import org.junit.Test;

public class BaseTests extends BaseTest {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("one.properties");
		icatsetup();
	}

	@Test
	public void getIcatUrlTest() throws Exception {
		super.getIcatUrlTest();
	}

	@Test
	public void apiVersionTest() throws Exception {
		super.apiVersionTest();
	}

	@Test
	public void getDatafileIdsTest() throws Exception {
		super.getDatafileIdsTest();
	}
	
	@Test
	public void reliabilityTest() throws Exception {
		super.reliabilityTest();
	}

}