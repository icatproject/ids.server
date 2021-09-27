package org.icatproject.ids.integration.util;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.icatproject.ICAT;
import org.icatproject.ids.ICATGetter;
import org.icatproject.ids.TestUtils;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.ShellCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Setup the test environment for the IDS. 
 * 
 * This is done by reading property values from test.properties and 
 * run.properties and calling a script to (re)install the IDS. 
 * 
 * IcatSetup is then called to ensure that the ICAT being used is populated 
 * with the necessary metadata.
 */
public class Setup {

	private static final Logger logger = LoggerFactory.getLogger(Setup.class);

	private static boolean skipIcatRepopulate = false;

	private URL icatUrl;
	private URL idsUrl;

	private String rootSessionId;
	private String forbiddenSessionId;

	private ICAT icatSoapClient;

	private Path home;
	private Path pluginMainDir;


	public Setup(String runPropertyFile) throws Exception {
		doSetup(runPropertyFile);
	}

	public Setup(String runPropertyFile, boolean skipIcatRepopulate) throws Exception {
		Setup.skipIcatRepopulate = skipIcatRepopulate;
		doSetup(runPropertyFile);
	}

	private void doSetup(String runPropertyFile) throws Exception {
		// Test home directory
		String testHome = System.getProperty("testHome");
		if (testHome == null) {
			testHome = System.getProperty("user.home");
		}
		home = Paths.get(testHome);

		// Start by reading the test properties
		Properties testProps = new Properties();
		InputStream is = Setup.class.getClassLoader().getResourceAsStream("test.properties");
		try {
			testProps.load(is);
		} catch (Exception e) {
			System.err.println("Problem loading test.properties: " + e.getClass() + " " + e.getMessage());
		}

		String serverUrl = System.getProperty("serverUrl");

		idsUrl = new URL(serverUrl + "/ids");

		String containerHome = System.getProperty("containerHome");
		if (containerHome == null) {
			System.err.println("containerHome is not defined as a system property");
		}

		long time = System.currentTimeMillis();

		String prepareScript = "prepare_test.py";
		ShellCommand sc = new ShellCommand("src/test/scripts/" + prepareScript, 
				"src/test/resources/" + runPropertyFile,
				home.toString(), containerHome, serverUrl);
		logger.debug("{} exit code is: {}", prepareScript, sc.getExitValue());
		logger.debug("{} std out is: {}", prepareScript, sc.getStdout());
		logger.debug("{} std err is: {}", prepareScript, sc.getStderr());
		if (sc.getExitValue() != 0) {
			throw new Exception(prepareScript + " failed with message: " + sc.getMessage());
		}
		logger.info("Setting up {} took {} seconds", runPropertyFile, (System.currentTimeMillis() - time) / 1000.);

		// Having set up the run.properties file read it find other things
		CheckedProperties runProperties = new CheckedProperties();
		runProperties.loadFromFile("src/test/install/run.properties");
		icatUrl = runProperties.getURL("icat.url");
		ICAT icat = ICATGetter.getService(icatUrl.toString());
		rootSessionId = TestUtils.login(icat, testProps.getProperty("login.root"));
		forbiddenSessionId = TestUtils.login(icat, testProps.getProperty("login.unauthorized"));

		pluginMainDir = runProperties.getPath("plugin.main.dir");
		logger.info("plugin.main.dir is {}", pluginMainDir);
		if (pluginMainDir.toFile().list().length != 0) {
			logger.error("plugin.main.dir is not empty");
			// force the user to delete the contents manually in case the wrong directory
			// has been specified because it will be emptied at the end of each test
			throw new Exception("plugin.main.dir " + pluginMainDir + " must be empty before tests are run");
		}

		icatSoapClient = ICATGetter.getService(icatUrl.toString());

		if (skipIcatRepopulate) {
			logger.warn("Skipping ICAT re-populate");
		} else {
			IcatSetup icatSetup = new IcatSetup(icatSoapClient, rootSessionId);
			icatSetup.cleanIcat();
			icatSetup.populateIcat();
			// set the flag so that the ICAT doesn't 
			// get repopulated for subsequent tests
			skipIcatRepopulate = true;
		}

	}


	public String getRootSessionId() {
		return rootSessionId;
	}

	public String getForbiddenSessionId() {
		return forbiddenSessionId;
	}

	public URL getIcatUrl() {
		return icatUrl;
	}

	public URL getIdsUrl() {
		return idsUrl;
	}

	public ICAT getIcatSoapClient() {
		return icatSoapClient;
	}

	public Path getPluginMainDir() {
		return pluginMainDir;
	}

}
