package org.icatproject.ids.integration.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;
import org.icatproject.Login.Credentials;
import org.icatproject.Login.Credentials.Entry;
import org.icatproject.ids.ICATGetter;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.ShellCommand;

/*
 * Setup the test environment for the IDS. This is done by reading property
 * values from the test.properties file and calling this class before 
 * running the tests.
 */
public class Setup {

	private URL icatUrl = null;
	private URL idsUrl = null;

	private String goodSessionId = null;
	private String forbiddenSessionId = null;

	private Path home;
	private Path storageArchiveDir;
	private Path storageDir;
	private Path preparedCacheDir;
	private Path updownDir;

	public boolean isTwoLevel() {
		return twoLevel;
	}

	public String getStorageUnit() {
		return storageUnit;
	}

	public Path getErrorLog() {
		return errorLog;
	}

	private Path errorLog;
	private String storageUnit;
	private boolean twoLevel;
	private String key;

	public Setup(String runPropertyFile) throws Exception {

		home = Paths.get(System.getProperty("user.home"));

		// Start by reading the test properties
		Properties testProps = new Properties();
		InputStream is = Setup.class.getClassLoader().getResourceAsStream("test.properties");
		try {
			testProps.load(is);
		} catch (Exception e) {
			System.err.println("Problem loading test.properties: " + e.getClass() + " " + e.getMessage());
		}

		setReliability(1.);

		String serverUrl = System.getProperty("serverUrl");

		idsUrl = new URL(serverUrl + "/ids");

		String containerHome = System.getProperty("containerHome");
		if (containerHome == null) {
			System.err.println("containerHome is not defined as a system property");
		}

		long time = System.currentTimeMillis();

		ShellCommand sc = new ShellCommand("src/test/scripts/prepare_test.py", "src/test/resources/" + runPropertyFile,
				home.toString(), containerHome, serverUrl);
		System.out.println(sc.getStdout() + " " + sc.getStderr());
		System.out.println(
				"Setting up " + runPropertyFile + " took " + (System.currentTimeMillis() - time) / 1000. + "seconds");

		// Having set up the ids.properties file read it find other things
		CheckedProperties runProperties = new CheckedProperties();
		runProperties.loadFromFile("src/test/install/run.properties");
		if (runProperties.has("key")) {
			key = runProperties.getString("key");
		}
		updownDir = home.resolve(testProps.getProperty("updownDir"));
		icatUrl = runProperties.getURL("icat.url");
		goodSessionId = login(testProps.getProperty("authorizedIcatUsername"),
				testProps.getProperty("authorizedIcatPassword"));
		forbiddenSessionId = login(testProps.getProperty("unauthorizedIcatUsername"),
				testProps.getProperty("unauthorizedIcatPassword"));

		storageDir = runProperties.getPath("plugin.main.dir");

		if (runProperties.has("plugin.archive.dir")) {
			storageArchiveDir = runProperties.getPath("plugin.archive.dir");
		}

		Path cacheDir = runProperties.getPath("cache.dir");
		preparedCacheDir = cacheDir.resolve("prepared");
		twoLevel = runProperties.has("plugin.archive.class");
		if (twoLevel) {
			String storageUnitString = runProperties.getString("storageUnit");
			if (storageUnitString == null) {
				throw new Exception("storageUnit not set");
			}
			storageUnit = storageUnitString.toUpperCase();
		}

		errorLog = runProperties.getPath("filesCheck.errorLog");

	}

	public void setReliability(double d) throws IOException {
		Path p = home.resolve("reliability");
		try (BufferedWriter out = Files.newBufferedWriter(p)) {
			out.write(d + "\n");
		}
	}

	public String login(String username, String password) throws IcatException_Exception, MalformedURLException {
		ICAT icat = ICATGetter.getService(icatUrl.toString());

		Credentials credentials = new Credentials();
		List<Entry> entries = credentials.getEntry();

		Entry u = new Entry();
		u.setKey("username");
		u.setValue(username);
		entries.add(u);

		Entry p = new Entry();
		p.setKey("password");
		p.setValue(password);
		entries.add(p);
		String sessionId = icat.login("db", credentials);
		return sessionId;
	}

	public String getForbiddenSessionId() {
		return forbiddenSessionId;
	}

	public String getGoodSessionId() {
		return goodSessionId;
	}

	public URL getIdsUrl() {
		return idsUrl;
	}

	public Path getStorageArchiveDir() {
		return storageArchiveDir;
	}

	public Path getStorageDir() {
		return storageDir;
	}

	public Path getPreparedCacheDir() {
		return preparedCacheDir;
	}

	public Path getUpdownDir() {
		return updownDir;
	}

	public URL getIcatUrl() {
		return icatUrl;
	}

	public String getKey() {
		return key;
	}

}
