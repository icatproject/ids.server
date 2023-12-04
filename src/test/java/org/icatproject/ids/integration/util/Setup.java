package org.icatproject.ids.integration.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.icatproject.ICAT;
import org.icatproject.ids.ICATGetter;
import org.icatproject.ids.TestUtils;
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

    private String rootSessionId = null;
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
        ICAT icat = ICATGetter.getService(icatUrl.toString());
        rootSessionId = TestUtils.login(icat, testProps.getProperty("login.root"));
        forbiddenSessionId = TestUtils.login(icat, testProps.getProperty("login.unauthorized"));

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

    public String getForbiddenSessionId() {
        return forbiddenSessionId;
    }

    public String getRootSessionId() {
        return rootSessionId;
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
