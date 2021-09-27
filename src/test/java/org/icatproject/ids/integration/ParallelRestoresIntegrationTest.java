package org.icatproject.ids.integration;

import org.junit.BeforeClass;

/**
 * Integration test class that causes all of the tests in IntegrationTest to be
 * re-run but with the IDS re-installed using a different run.properties file.
 * 
 * The difference in this run.properties file is that the maxRestoresPerThread
 * property has been set low to cause multiple restore threads to be created  
 * for some of the tests. 
 * 
 * The result should be the same apart from this test runs quicker due to more 
 * threads working on restoring the files from Archive Storage.
 */
public class ParallelRestoresIntegrationTest extends IntegrationTest {

    @BeforeClass
	public static void setupClass() throws Exception {
        doSetup("integration.parallel.run.properties");
        setLoggerClass(ParallelRestoresIntegrationTest.class);
	}

}
