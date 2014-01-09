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

import javax.xml.namespace.QName;

import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.IcatException_Exception;
import org.icatproject.Login.Credentials;
import org.icatproject.Login.Credentials.Entry;
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
	private Path datasetCacheDir;
	private Path storageDir;
	private Path preparedCacheDir;
	private Path updownDir;

	public boolean isTwoLevel() {
		return twoLevel;
	}

	private boolean twoLevel;

	public Setup(String idsPropertyFile) throws Exception {

		// Start by reading the test properties
		Properties testProps = new Properties();

		InputStream is = getClass().getResourceAsStream("/test.properties");
		try {
			testProps.load(is);
		} catch (Exception e) {
			System.out.println("Problem loading test.properties\n" + e.getMessage());
		}

		idsUrl = new URL(testProps.getProperty("ids.url") + "/ids");

		String glassfish = testProps.getProperty("glassfish");

		ShellCommand sc = new ShellCommand("asadmin", "get", "property.administrative.domain.name");
		String domain = sc.getStdout().split("[\r\n]+")[0].split("=")[1];
		Path config = new File(glassfish).toPath().resolve("glassfish").resolve("domains")
				.resolve(domain).resolve("config");

		sc = new ShellCommand("cmp", config.resolve("ids.properties").toString(),
				"src/test/resources/" + idsPropertyFile);
		if (sc.getExitValue() == 1) {
			System.out.println("Moving " + idsPropertyFile + " to " + config);
			sc = new ShellCommand("asadmin", "disable", "ids.server-1.0.0-SNAPSHOT");
			if (sc.getExitValue() != 0) {
				System.out.println(sc.getMessage());
			}
			Files.copy(new File("src/test/resources/" + idsPropertyFile).toPath(),
					config.resolve("ids.properties"), StandardCopyOption.REPLACE_EXISTING);
			sc = new ShellCommand("asadmin", "enable", "ids.server-1.0.0-SNAPSHOT");
			if (sc.getExitValue() != 0) {
				System.out.println(sc.getMessage());
			} else {
				System.out.println(sc.getStdout());
			}
		} else if (sc.getExitValue() == 2) {
			System.out.println(sc.getMessage());
		}

		// Having set up the ids.properties file read it find other things
		Properties idsProperties = new Properties();
		idsProperties.load(Files.newInputStream(config.resolve("ids.properties")));

		updownDir = new File(testProps.getProperty("updownDir")).toPath();

		String icatUrlString = idsProperties.getProperty("icat.url");
		if (!icatUrlString.endsWith("ICATService/ICAT?wsdl")) {
			if (icatUrlString.charAt(icatUrlString.length() - 1) == '/') {
				icatUrlString = icatUrlString + "ICATService/ICAT?wsdl";
			} else {
				icatUrlString = icatUrlString + "/ICATService/ICAT?wsdl";
			}
		}
		icatUrl = new URL(icatUrlString);
		goodSessionId = login(testProps.getProperty("authorizedIcatUsername"),
				testProps.getProperty("authorizedIcatPassword"));
		forbiddenSessionId = login(testProps.getProperty("unauthorizedIcatUsername"),
				testProps.getProperty("unauthorizedIcatPassword"));

		// Lookup up the plugin config files and read those to find where they will store data
		String mainProps = idsProperties.getProperty("plugin.main.properties");
		Properties storageProps = new Properties();
		storageProps.load(Files.newInputStream(config.resolve(mainProps)));
		storageDir = new File(storageProps.getProperty("dir")).toPath();

		String archiveProps = idsProperties.getProperty("plugin.archive.properties");
		if (archiveProps != null) {
			storageProps.load(Files.newInputStream(config.resolve(archiveProps)));
			storageArchiveDir = new File(storageProps.getProperty("dir")).toPath();
		}

		Path cacheDir = new File(idsProperties.getProperty("cache.dir")).toPath();
		preparedCacheDir = cacheDir.resolve("prepared");
		datasetCacheDir = cacheDir.resolve("dataset");
		twoLevel = idsProperties.getProperty("plugin.archive.class") != null;

	}

	public String login(String username, String password) throws IcatException_Exception,
			MalformedURLException {
		ICAT icat = new ICATService(icatUrl, new QName("http://icatproject.org", "ICATService"))
				.getICATPort();

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

	public Path getDatasetCacheDir() {
		return datasetCacheDir;
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

}
