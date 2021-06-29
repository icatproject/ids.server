package org.icatproject.ids.integration;

import java.util.ArrayList;
import java.util.List;

import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.TestingClient;

/**
 * Class to provide common functionality needed by multiple integration tests
 */
public class BaseIntegrationTest {

	protected static Setup setup;
    protected static org.icatproject.ICAT icatSoapClient;
    protected static String rootSessionId;
    protected static TestingClient testingClient;

	protected static void doSetup(String runPropertiesFile) throws Exception {
		setup = new Setup(runPropertiesFile);
		setupIcatClientsEtc();
	}

	protected static void doSetup(String runPropertiesFile, boolean skipIcatRepopulate) throws Exception {
		setup = new Setup(runPropertiesFile, skipIcatRepopulate);
		setupIcatClientsEtc();
	}

	private static void setupIcatClientsEtc() throws Exception {
		icatSoapClient = setup.getIcatSoapClient();
		rootSessionId = setup.getRootSessionId();
		testingClient = new TestingClient(setup.getIdsUrl());
	}

	protected List<String> getStringsFromQuery(String query) throws Exception {
		List<Object> objList = getIDsFromQuery(query);
		List<String> stringList = new ArrayList<>();
		for (Object obj : objList) {
			stringList.add((String)obj);
		}
		return stringList;
	}

	protected List<Long> getLongsFromQuery(String query) throws Exception {
		List<Object> objList = getIDsFromQuery(query);
		List<Long> longList = new ArrayList<>();
		for (Object obj : objList) {
			longList.add((Long)obj);
		}
		return longList;
	}

	private List<Object> getIDsFromQuery(String query) throws Exception {
		String rootSessionId = setup.getRootSessionId();
		List<Object> objList = icatSoapClient.search(rootSessionId, query);
		return objList;
	}

}
