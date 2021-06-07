package org.icatproject.ids.integration;

import static org.junit.Assert.assertTrue;

import org.icatproject.ids.integration.util.Setup;
import org.junit.BeforeClass;
import org.junit.Test;

public class IntegrationTest extends BaseIntegrationTest {

    @BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("integration.properties");
//        icatsetup();
//        cleanIcat();
//        populateIcat();
	}
    
    @Test
    public void testSomething() throws Exception {
        assertTrue(true);
    }
}
