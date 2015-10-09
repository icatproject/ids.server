package org.icatproject.ids.integration.util;

import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Properties;

import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;
import org.icatproject.Login.Credentials;
import org.icatproject.Login.Credentials.Entry;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.ICATGetter;
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

	public Setup(String idsPropertyFile) throws Exception {

		// Start by reading the test properties
		Properties testProps = new Properties();
		InputStream is = Setup.class.getClassLoader().getResourceAsStream("test.properties");
		try {
			testProps.load(is);
		} catch (Exception e) {
			System.out.println("Problem loading test.properties: " + e.getClass() + " " + e.getMessage());
		}

		idsUrl = new URL(testProps.getProperty("ids.url") + "/ids");

		String glassfish = testProps.getProperty("glassfish");

		String appName = testProps.getProperty("appName");

		ShellCommand sc = new ShellCommand("asadmin", "get", "property.administrative.domain.name");
		String domain = sc.getStdout().split("[\r\n]+")[0].split("=")[1];
		Path config = new File(glassfish).toPath().resolve("glassfish").resolve("domains").resolve(domain)
				.resolve("config");

		sc = new ShellCommand("cmp", config.resolve("ids.properties").toString(),
				"src/test/resources/" + idsPropertyFile);
		if (sc.getExitValue() == 1) {
			System.out.println("Moving " + idsPropertyFile + " to " + config);
			sc = new ShellCommand("asadmin", "disable", appName);
			if (sc.getExitValue() != 0) {
				System.out.println(sc.getMessage());
			}
			Files.copy(new File("src/test/resources/" + idsPropertyFile).toPath(), config.resolve("ids.properties"),
					StandardCopyOption.REPLACE_EXISTING);
			sc = new ShellCommand("asadmin", "enable", appName);
			if (sc.getExitValue() != 0) {
				System.out.println(sc.getMessage());
			} else {
				System.out.println(sc.getStdout());
			}
		} else if (sc.getExitValue() == 2) {
			System.out.println(sc.getMessage());
		}

		// Having set up the ids.properties file read it find other things
		CheckedProperties idsProperties = new CheckedProperties();
		idsProperties.loadFromFile(config.resolve("ids.properties").toString());
		if (idsProperties.has("key")) {
			key = idsProperties.getString("key");
		}
		updownDir = new File(testProps.getProperty("updownDir")).toPath();
		icatUrl = idsProperties.getURL("icat.url");
		goodSessionId = login(testProps.getProperty("authorizedIcatUsername"),
				testProps.getProperty("authorizedIcatPassword"));
		forbiddenSessionId = login(testProps.getProperty("unauthorizedIcatUsername"),
				testProps.getProperty("unauthorizedIcatPassword"));

		// Lookup up the plugin config files and read those to find where they
		// will store data
		String mainProps = idsProperties.getString("plugin.main.properties");
		Properties storageProps = new Properties();
		storageProps.load(Files.newInputStream(config.resolve(mainProps)));
		storageDir = new File(storageProps.getProperty("dir")).toPath();

		if (idsProperties.has("plugin.archive.properties")) {
			String archiveProps = idsProperties.getString("plugin.archive.properties");
			storageProps.load(Files.newInputStream(config.resolve(archiveProps)));
			storageArchiveDir = new File(storageProps.getProperty("dir")).toPath();
		}

		Path cacheDir = new File(idsProperties.getString("cache.dir")).toPath();
		preparedCacheDir = cacheDir.resolve("prepared");
		twoLevel = idsProperties.has("plugin.archive.class");
		if (twoLevel) {
			String storageUnitString = idsProperties.getString("storageUnit");
			if (storageUnitString == null) {
				throw new Exception("storageUnit not set");
			}
			storageUnit = storageUnitString.toUpperCase();
		}

		errorLog = config.resolve(idsProperties.getString("filesCheck.errorLog"));

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
