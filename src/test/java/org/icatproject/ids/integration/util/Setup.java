package org.icatproject.ids.integration.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import javax.xml.namespace.QName;

import org.apache.commons.codec.digest.DigestUtils;
import org.icatproject.Datafile;
import org.icatproject.DatafileFormat;
import org.icatproject.Dataset;
import org.icatproject.DatasetType;
import org.icatproject.EntityBaseBean;
import org.icatproject.Facility;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.IcatException_Exception;
import org.icatproject.Investigation;
import org.icatproject.InvestigationType;
import org.icatproject.Login.Credentials;
import org.icatproject.Login.Credentials.Entry;

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

	private Map<String, String> filenameMD5 = new HashMap<String, String>();
	private ArrayList<Long> datasetIds = new ArrayList<>();
	private ArrayList<Long> datafileIds = new ArrayList<>();
	private String newFileLocation;

	private String storageArchiveDir;
	private String storageZipDir;
	private String storageDir;
	private String storagePreparedDir;
	private String userLocalDir;

	private ICAT icat;
	private Facility fac;
	private DatasetType dsType;
	private DatafileFormat supportedDatafileFormat;

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

		userLocalDir = testProps.getProperty("userLocalDir");

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
		storageDir = storageProps.getProperty("dir");

		String archiveProps = idsProperties.getProperty("plugin.archive.properties");
		if (archiveProps != null) {
			storageProps.load(Files.newInputStream(config.resolve(archiveProps)));
			storageArchiveDir = storageProps.getProperty("dir");
		}

		Path cacheDir = new File(idsProperties.getProperty("cache.dir")).toPath();
		storagePreparedDir = cacheDir.resolve("prepared").toString();
		storageZipDir = cacheDir.resolve("dataset").toString();

		long timestamp = System.currentTimeMillis();

		try {
			final ICATService icatService = new ICATService(icatUrl, new QName(
					"http://icatproject.org", "ICATService"));
			icat = icatService.getICATPort();

			List<Object> objects = icat.search(goodSessionId, "Facility");
			List<EntityBaseBean> facilities = new ArrayList<>();
			for (Object o : objects) {
				facilities.add((Facility) o);
			}

			icat.deleteMany(goodSessionId, facilities);

			fac = new Facility();
			fac.setName("Facility_" + timestamp);
			fac.setId(icat.create(goodSessionId, fac));

			dsType = new DatasetType();
			dsType.setFacility(fac);
			dsType.setName("DatasetType_" + timestamp);
			dsType.setId(icat.create(goodSessionId, dsType));

			supportedDatafileFormat = new DatafileFormat();
			supportedDatafileFormat.setFacility(fac);
			supportedDatafileFormat.setName("test_format");
			supportedDatafileFormat.setVersion("42.0.0");
			supportedDatafileFormat.setId(icat.create(goodSessionId, supportedDatafileFormat));

			InvestigationType invType = new InvestigationType();
			invType.setName("Not null");
			invType.setFacility(fac);
			invType.setId(icat.create(goodSessionId, invType));

			Investigation inv = new Investigation();
			inv.setName("Investigation_" + timestamp);
			inv.setType(invType);
			inv.setTitle("Not null");
			inv.setFacility(fac);
			inv.setVisitId("A visit");
			inv.setId(icat.create(goodSessionId, inv));

			String invLoc = fac.getName() + "/" + inv.getName() + "/" + inv.getVisitId();

			Dataset ds1 = new Dataset();
			ds1.setName("ds1_" + timestamp);
			ds1.setLocation(invLoc + "/" + ds1.getName());
			ds1.setType(dsType);
			ds1.setInvestigation(inv);
			ds1.setId(icat.create(goodSessionId, ds1));

			Dataset ds2 = new Dataset();
			ds2.setName("ds2_" + timestamp);
			ds1.setLocation(invLoc + "/" + ds2.getName());
			ds2.setType(dsType);
			ds2.setInvestigation(inv);
			ds2.setId(icat.create(goodSessionId, ds2));

			Datafile df1 = new Datafile();
			df1.setName("df1_" + timestamp);
			df1.setLocation(ds1.getLocation() + "/df1_" + timestamp);
			df1.setDataset(ds1);
			df1.setId(icat.create(goodSessionId, df1));

			Datafile df2 = new Datafile();
			df2.setName("df2_" + timestamp);
			df2.setLocation(ds1.getLocation() + "/df2_" + timestamp);
			df2.setDataset(ds1);
			df2.setId(icat.create(goodSessionId, df2));

			System.out.println("ds " + ds1.getId() + " holds dfs " + df1.getId() + " and "
					+ df2.getId());

			Datafile df3 = new Datafile();
			df3.setName("df3_" + timestamp);
			df3.setLocation(ds2.getLocation() + "/df3_" + timestamp);
			df3.setDataset(ds2);
			df3.setId(icat.create(goodSessionId, df3));

			Datafile df4 = new Datafile();
			df4.setName("df4_" + timestamp);
			df4.setLocation(ds2.getLocation() + "/df4_" + timestamp);
			df4.setDataset(ds2);
			df4.setId(icat.create(goodSessionId, df4));

			System.out.println("ds " + ds2.getId() + " holds dfs " + df3.getId() + " and "
					+ df4.getId());

			// update the datasets, so they contains references to their datafiles
			ds1 = (Dataset) icat.get(goodSessionId, "Dataset INCLUDE Datafile", ds1.getId());
			ds2 = (Dataset) icat.get(goodSessionId, "Dataset INCLUDE Datafile", ds2.getId());

			datasetIds.add(ds1.getId());
			datasetIds.add(ds2.getId());

			datafileIds.add(df1.getId());
			datafileIds.add(df2.getId());
			datafileIds.add(df3.getId());
			datafileIds.add(df4.getId());

			writeToFile(new File(storageDir, df1.getLocation()), "df1 test content");
			writeToFile(new File(storageDir, df2.getLocation()), "df2 test content");
			writeToFile(new File(storageDir, df3.getLocation()), "df3 test content");
			writeToFile(new File(storageDir, df4.getLocation()), "df4 test content");

			addMD5s(df1, computeMd5(df1));
			addMD5s(df2, computeMd5(df2));
			addMD5s(df3, computeMd5(df3));
			addMD5s(df4, computeMd5(df4));

			if (storageArchiveDir != null) {
				zipDatasetToArchive(ds1);
				zipDatasetToArchive(ds2);
			}

			newFileLocation = new File(userLocalDir, "new_file_" + timestamp).getAbsolutePath();
			writeToFile(new File(newFileLocation), "new_file test content");
		} catch (Exception e) {
			System.err.println("Could not prepare ICAT db for testing: " + e.getMessage());
			e.printStackTrace();
			throw e;
		}

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

	/*
	 * May be used in @AfterClass annotated method in tests to clean the test data from the disk and
	 * the ICAT database
	 */
	public void cleanArchiveAndDb() throws NumberFormatException, IcatException_Exception,
			IOException {
		for (Long id : datafileIds) {
			Datafile df = (Datafile) icat.get(goodSessionId, "Datafile", id);
			icat.delete(goodSessionId, df);
		}
		for (Long id : datasetIds) {
			Dataset ds = (Dataset) icat.get(goodSessionId, "Dataset", id);
			icat.delete(goodSessionId, ds);
		}
		icat.delete(goodSessionId, dsType);
		icat.delete(goodSessionId, fac);

		TreeDeleteVisitor treeDeleteVisitor = new TreeDeleteVisitor();

		Files.walkFileTree(FileSystems.getDefault().getPath(storageArchiveDir, "test_dss"),
				treeDeleteVisitor);

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

	public Map<String, String> getFilenameMD5() {
		return filenameMD5;
	}

	public ArrayList<Long> getDatasetIds() {
		return datasetIds;
	}

	public ArrayList<Long> getDatafileIds() {
		return datafileIds;
	}

	public String getStorageArchiveDir() {
		return storageArchiveDir;
	}

	public String getStorageZipDir() {
		return storageZipDir;
	}

	public String getStorageDir() {
		return storageDir;
	}

	public String getStoragePreparedDir() {
		return storagePreparedDir;
	}

	public String getUserLocalDir() {
		return userLocalDir;
	}

	public String getNewFileLocation() {
		return newFileLocation;
	}

	public URL getIcatUrl() {
		return icatUrl;
	}

	public DatafileFormat getSupportedDatafileFormat() {
		return supportedDatafileFormat;
	}

	private void writeToFile(File dst, String content) throws IOException {
		PrintWriter out = null;
		try {
			dst.getParentFile().mkdirs();
			dst.createNewFile();
			out = new PrintWriter(dst);
			out.println(content);
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	private String computeMd5(Datafile df) throws IOException {
		FileInputStream in = null;
		String res;
		try {
			File file = new File(storageDir, df.getLocation());
			in = new FileInputStream(file);
			res = DigestUtils.md5Hex(in);
		} finally {
			if (in != null) {
				in.close();
			}
		}
		return res;
	}

	/*
	 * Adds an MD5 sum to the map for three possible locations that a Datafile can have in a zip
	 * file
	 */
	private void addMD5s(Datafile df, String md5) {
		filenameMD5.put(df.getLocation(), md5);
		filenameMD5.put("Datafile-" + df.getId(), md5);
		filenameMD5.put(
				String.format("Dataset-%s/Datafile-%s", df.getDataset().getId(), df.getId()), md5);
	}

	private void zipDatasetToArchive(Dataset ds) throws IOException {
		File zipFile = new File(new File(storageArchiveDir, ds.getLocation()), "files.zip");
		zipFile.getParentFile().mkdirs();
		zipFile.createNewFile();
		zipDataset(zipFile, ds, storageDir, false);
	}

	private void zipDataset(File zipFile, Dataset dataset, String relativePath, boolean compress) {
		if (dataset.getDatafiles().isEmpty()) {
			// Create empty file
			try {
				zipFile.createNewFile();
			} catch (IOException ex) {
				System.err.println("writeZipFileFromDatafiles" + ex);
			}
			return;
		}

		try {
			FileOutputStream fos = new FileOutputStream(zipFile);
			ZipOutputStream zos = new ZipOutputStream(fos);

			// set whether to compress the zip file or not
			if (compress == true) {
				zos.setMethod(ZipOutputStream.DEFLATED);
			} else {
				// using compress with level 0 instead of archive (STORED) because
				// STORED requires you to set CRC, size and compressed size
				// TODO: find efficient way of calculating CRC
				zos.setMethod(ZipOutputStream.DEFLATED);
				zos.setLevel(0);
			}
			for (Datafile file : dataset.getDatafiles()) {
				addToZip(file.getName(), zos,
						new File(relativePath, dataset.getLocation()).getAbsolutePath());
			}

			zos.close();
			fos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void addToZip(String fileName, ZipOutputStream zos, String relativePath) {
		try {
			File file = new File(relativePath, fileName);
			FileInputStream fis = new FileInputStream(file);
			// to the directory being zipped, so chop off the rest of the path
			String zipFilePath = file.getCanonicalPath().substring(relativePath.length(),
					file.getCanonicalPath().length());
			if (zipFilePath.startsWith(File.separator)) {
				zipFilePath = zipFilePath.substring(1);
			}
			System.out.println("Writing '" + zipFilePath + "' to zip file");
			ZipEntry zipEntry = new ZipEntry(zipFilePath);
			try {
				zos.putNextEntry(zipEntry);
				byte[] bytes = new byte[1024];
				int length;
				while ((length = fis.read(bytes)) >= 0) {
					zos.write(bytes, 0, length);
				}
				zos.closeEntry();
				fis.close();
			} catch (ZipException ex) {
				System.out.println("Skipping the file" + ex);
				fis.close();
			}
		} catch (IOException ex) {
			System.err.println("addToZip" + ex);
		}
	}
}
