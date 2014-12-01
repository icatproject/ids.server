package org.icatproject.ids.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.xml.namespace.QName;

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
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.TestingClient;
import org.icatproject.ids.integration.util.client.TestingClient.ServiceStatus;
import org.icatproject.ids.integration.util.client.TestingClient.Status;
import org.junit.Before;

public class BaseTest {

	public class DeleteVisitor extends SimpleFileVisitor<Path> {

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			Files.delete(file);
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
			if (e == null) {
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			} else {
				// directory iteration failed
				throw e;
			}
		}

	}

	protected Path getDirOnFastStorage(Long dsId) throws IcatException_Exception {
		Dataset icatDs = (Dataset) icat.get(sessionId, "Dataset INCLUDE Investigation", dsId);
		return setup.getStorageDir().resolve(Long.toString(icatDs.getInvestigation().getId()))
				.resolve(Long.toString(icatDs.getId()));
	}

	protected Path getFileOnArchiveStorage(Long dsId) throws IcatException_Exception {
		Dataset icatDs = (Dataset) icat.get(sessionId, "Dataset INCLUDE Investigation", dsId);
		return setup.getStorageArchiveDir()
				.resolve(Long.toString(icatDs.getInvestigation().getId()))
				.resolve(Long.toString(icatDs.getId()));
	}

	protected static ICAT icat;
	protected static Setup setup = null;

	public static void icatsetup() throws Exception {
		ICATService icatService = new ICATService(setup.getIcatUrl(), new QName(
				"http://icatproject.org", "ICATService"));
		icat = icatService.getICATPort();
	}

	protected ArrayList<Long> datafileIds = new ArrayList<>();
	protected ArrayList<Long> datasetIds = new ArrayList<>();
	protected String sessionId;

	protected TestingClient testingClient;
	protected Path newFileLocation;
	protected DatafileFormat supportedDatafileFormat;
	protected long investigationId;
	private long time;

	@Before
	public void before() throws Exception {
		testingClient = new TestingClient(setup.getIdsUrl());
		sessionId = setup.getGoodSessionId();
		waitForIds();
		populateStorage(setup.isTwoLevel(), setup.getStorageUnit(), setup.getKey());
	}

	protected void checkAbsent(Path file) {
		assertFalse(file.toString(), Files.exists(file));
	}

	protected void checkPresent(Path file) {
		assertTrue(file.toString(), Files.exists(file));
	}

	protected void waitForIds() throws Exception {
		waitForIds(20);
	}

	protected void waitForIds(int nmax) throws Exception {
		int n = 0;
		while (true) {
			n++;
			if (n > nmax) {
				throw new Exception("Got bored waiting");
			}
			ServiceStatus stat = testingClient.getServiceStatus(sessionId, 200);

			if (stat.getOpItems().isEmpty()) {
				break;
			}

			if (n % 10 == 0) {
				if (!stat.getOpItems().isEmpty()) {
					System.out.println("Ops " + stat.getOpItems());
				}
			}
			Thread.sleep(1000);
		}
	}

	protected void logTime(String msg) {
		long now = System.currentTimeMillis();
		if (time != 0) {
			System.out.println(msg + " took " + (now - time) + "ms.");
		} else {
			System.out.println(msg);
		}
		time = now;
	}

	protected void checkZipFile(Path file, List<Long> ids, int compressedSize) throws IOException {
		checkZipStream(Files.newInputStream(file), ids, compressedSize);
	}

	private void clearStorage() throws IOException {
		cleanDir(setup.getStorageDir());
		cleanDir(setup.getStorageArchiveDir());
		cleanDir(setup.getUpdownDir());
		cleanDir(setup.getPreparedCacheDir());
		cleanDir(setup.getStorageDir());
	}

	private void cleanDir(Path dir) throws IOException {
		if (dir != null) {
			DeleteVisitor treeDeleteVisitor = new DeleteVisitor();
			if (Files.exists(dir)) {
				Files.walkFileTree(dir, treeDeleteVisitor);
			}
			Files.createDirectories(dir);
		}
	}

	protected byte[] getOutput(InputStream stream) throws IOException {
		ByteArrayOutputStream os = null;
		try {
			os = new ByteArrayOutputStream();
			int len;
			byte[] buffer = new byte[1024];
			while ((len = stream.read(buffer)) != -1) {
				os.write(buffer, 0, len);
			}
			return os.toByteArray();
		} finally {
			if (stream != null) {
				stream.close();
			}
		}
	}

	private void populateStorage(boolean twoLevel, String storageUnit, String key)
			throws IOException, IcatException_Exception, NoSuchAlgorithmException {
		clearStorage();
		long timestamp = System.currentTimeMillis();

		try {

			List<Object> objects = icat.search(sessionId, "Facility");
			List<EntityBaseBean> facilities = new ArrayList<>();
			for (Object o : objects) {
				facilities.add((Facility) o);
			}

			icat.deleteMany(sessionId, facilities);

			Facility fac = new Facility();
			fac.setName("Facility_" + timestamp);
			fac.setId(icat.create(sessionId, fac));

			DatasetType dsType = new DatasetType();
			dsType.setFacility(fac);
			dsType.setName("DatasetType_" + timestamp);
			dsType.setId(icat.create(sessionId, dsType));

			supportedDatafileFormat = new DatafileFormat();
			supportedDatafileFormat.setFacility(fac);
			supportedDatafileFormat.setName("test_format");
			supportedDatafileFormat.setVersion("42.0.0");
			supportedDatafileFormat.setId(icat.create(sessionId, supportedDatafileFormat));

			InvestigationType invType = new InvestigationType();
			invType.setName("Not null");
			invType.setFacility(fac);
			invType.setId(icat.create(sessionId, invType));

			Investigation inv = new Investigation();
			inv.setName("Investigation_" + timestamp);
			inv.setType(invType);
			inv.setTitle("Not null");
			inv.setFacility(fac);
			inv.setVisitId("N/A");
			inv.setId(icat.create(sessionId, inv));
			investigationId = inv.getId();
			String invLoc = inv.getId() + "/";

			Dataset ds1 = new Dataset();
			ds1.setName("ds1_" + timestamp);
			ds1.setLocation(invLoc + ds1.getId());
			ds1.setType(dsType);
			ds1.setInvestigation(inv);
			ds1.setId(icat.create(sessionId, ds1));
			String ds1Loc = invLoc + ds1.getId() + "/";

			Dataset ds2 = new Dataset();
			ds2.setName("ds2_" + timestamp);
			ds2.setLocation(invLoc + ds2.getId());
			ds2.setType(dsType);
			ds2.setInvestigation(inv);
			ds2.setId(icat.create(sessionId, ds2));
			String ds2Loc = invLoc + ds2.getId() + "/";

			Datafile df1 = new Datafile();
			df1.setName("a/df1_" + timestamp);
			df1.setLocation(ds1Loc + UUID.randomUUID());
			df1.setDataset(ds1);
			writeToFile(df1, "df1 test content very compressible very compressible", key);

			Datafile df2 = new Datafile();
			df2.setName("df2_" + timestamp);
			df2.setLocation(ds1Loc + UUID.randomUUID());
			df2.setDataset(ds1);
			writeToFile(df2, "df2 test content very compressible very compressible", key);

			// System.out.println("ds " + ds1.getId() + " holds dfs " + df1.getId() + " and "
			// + df2.getId());

			Datafile df3 = new Datafile();
			df3.setName("df3_" + timestamp);
			df3.setLocation(ds2Loc + UUID.randomUUID());
			df3.setDataset(ds2);
			writeToFile(df3, "df3 test content very compressible very compressible", key);

			Datafile df4 = new Datafile();
			df4.setName("df4_" + timestamp);
			df4.setLocation(ds2Loc + UUID.randomUUID());
			df4.setDataset(ds2);
			writeToFile(df4, "df4 test content very compressible very compressible", key);

			// System.out.println("ds " + ds2.getId() + " holds dfs " + df3.getId() + " and "
			// + df4.getId());

			datasetIds.add(ds1.getId());
			datasetIds.add(ds2.getId());

			datafileIds.add(df1.getId());
			datafileIds.add(df2.getId());
			datafileIds.add(df3.getId());
			datafileIds.add(df4.getId());

			if (twoLevel) {
				moveDatasetToArchive(storageUnit, ds1, ds1Loc, fac, inv, key);
				moveDatasetToArchive(storageUnit, ds2, ds2Loc, fac, inv, key);
			}

			newFileLocation = setup.getUpdownDir().resolve("new_file_" + timestamp);
			Files.createDirectories(newFileLocation.getParent());
			byte[] bytes = "new_file test content".getBytes();
			OutputStream out = Files.newOutputStream(newFileLocation);
			out.write(bytes);
			out.close();
		} catch (IllegalAccessError e) {
			System.err.println("Could not prepare ICAT db for testing: " + e.getMessage());
			e.printStackTrace();
			throw e;
		}

	}

	protected Map<String, String> crcs = new HashMap<>();
	protected Map<String, Long> fsizes = new HashMap<>();
	protected Map<String, String> contents = new HashMap<>();
	protected Map<String, Long> ids = new HashMap<>();
	protected Map<String, String> paths = new HashMap<>();

	protected void checkZipStream(InputStream stream, List<Long> datafileIds, long compressedSize)
			throws IOException {
		ZipInputStream zis = new ZipInputStream(stream);
		ZipEntry ze = zis.getNextEntry();
		Set<Long> idsNeeded = new HashSet<>(datafileIds);
		while (ze != null) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] bytes = new byte[1024];
			int length;
			long n = 0;
			while ((length = zis.read(bytes)) >= 0) {
				baos.write(bytes, 0, length);
				n = n + length;
			}
			String key = paths.get(ze.getName());
			assertEquals(compressedSize, ze.getCompressedSize());
			assertEquals(crcs.get(key), Long.toHexString(ze.getCrc()));
			assertEquals(contents.get(key), baos.toString());
			assertEquals(fsizes.get(key), (Long) n);
			assertTrue(idsNeeded.remove(ids.get(key)));
			ze = zis.getNextEntry();
		}
		zis.close();
		assertTrue(idsNeeded.isEmpty());

	}

	protected void checkStream(InputStream stream, long id) throws IOException {
		boolean found = false;
		for (Entry<String, Long> e : ids.entrySet()) {
			if (id == e.getValue()) {
				assertEquals(contents.get(e.getKey()), new String(getOutput(stream)));
				found = true;
				break;
			}
		}
		assertTrue(found);
	}

	private void writeToFile(Datafile df, String content, String key) throws IOException,
			IcatException_Exception, NoSuchAlgorithmException {
		Path path = setup.getStorageDir().resolve(df.getLocation());
		Files.createDirectories(path.getParent());
		byte[] bytes = content.getBytes();
		CRC32 crc = new CRC32();
		crc.update(bytes);
		OutputStream out = Files.newOutputStream(path);
		out.write(bytes);
		out.close();
		df.setChecksum(Long.toHexString(crc.getValue()));
		df.setFileSize((long) bytes.length);
		long dfId = icat.create(sessionId, df);
		df.setId(dfId);
		if (key != null) {
			String location = df.getLocation();
			df.setLocation(location + " " + digest(dfId, location, key));
		}
		icat.update(sessionId, df);

		crcs.put(df.getLocation(), df.getChecksum());
		fsizes.put(df.getLocation(), df.getFileSize());
		contents.put(df.getLocation(), content);
		ids.put(df.getLocation(), df.getId());
		Dataset ds = df.getDataset();
		Investigation inv = ds.getInvestigation();
		Facility fac = inv.getFacility();
		paths.put(
				"ids/" + fac.getName() + "/" + inv.getName() + "/"
						+ URLEncoder.encode(inv.getVisitId(), "UTF-8") + "/" + ds.getName() + "/"
						+ df.getName(), df.getLocation());
	}

	private static final char[] HEX_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'A', 'B', 'C', 'D', 'E', 'F' };

	private String digest(Long id, String location, String key) throws NoSuchAlgorithmException {
		byte[] pattern = (id + location + key).getBytes();
		MessageDigest digest = null;
		digest = MessageDigest.getInstance("SHA-256");
		byte[] bytes = digest.digest(pattern);
		char[] hexChars = new char[bytes.length * 2];
		int v;
		for (int j = 0; j < bytes.length; j++) {
			v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_CHARS[v >>> 4];
			hexChars[j * 2 + 1] = HEX_CHARS[v & 0x0F];
		}
		return new String(hexChars);
	}

	private void moveDatasetToArchive(String storageUnit, Dataset ds, String dsLoc, Facility fac,
			Investigation inv, String key) throws IOException, IcatException_Exception {
		ds = (Dataset) icat.get(sessionId, "Dataset INCLUDE Datafile", ds.getId());
		Path top = setup.getStorageDir();

		if (storageUnit.equals("DATASET")) {
			Path zipFile = setup.getStorageArchiveDir().resolve(dsLoc);
			Files.createDirectories(zipFile.getParent());

			ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipFile));
			zos.setLevel(0);

			for (Datafile df : ds.getDatafiles()) {
				Path file;
				if (key == null) {
					file = top.resolve(df.getLocation());
				} else {
					file = top.resolve(getLocationFromDigest(df.getId(), df.getLocation(), key));
				}
				InputStream fis = Files.newInputStream(file);

				zos.putNextEntry(new ZipEntry("ids/" + fac.getName() + "/" + inv.getName() + "/"
						+ URLEncoder.encode(inv.getVisitId(), "UTF-8") + "/" + ds.getName() + "/"
						+ df.getName()));
				byte[] bytes = new byte[1024];
				int length;
				while ((length = fis.read(bytes)) >= 0) {
					zos.write(bytes, 0, length);
				}
				zos.closeEntry();
				fis.close();

			}
			zos.close();
		} else if (storageUnit.equals("DATAFILE")) {
			Path archive = setup.getStorageArchiveDir().resolve(dsLoc);
			Files.createDirectories(archive);
			for (Datafile df : ds.getDatafiles()) {
				Path p;
				if (key == null) {
					p = top.resolve(df.getLocation());
				} else {
					p = top.resolve(getLocationFromDigest(df.getId(), df.getLocation(), key));
				}
				Files.move(p, archive.resolve(p.getFileName()), StandardCopyOption.REPLACE_EXISTING);
			}
		}
		for (Datafile df : ds.getDatafiles()) {
			Path file;
			if (key == null) {
				file = top.resolve(df.getLocation());
			} else {
				file = top.resolve(getLocationFromDigest(df.getId(), df.getLocation(), key));
			}
			Files.deleteIfExists(file);
			Path parent = file.getParent();
			while (!parent.equals(top)) {
				try {
					Files.delete(parent);
					parent = parent.getParent();
				} catch (IOException e) {
					break;
				}
			}
		}

	}

	static String getLocationFromDigest(Long id, String locationWithHash, String key) {
		int i = locationWithHash.lastIndexOf(' ');
		return locationWithHash.substring(0, i);
	}

	protected void raceTest() throws Exception {
		logTime("Starting");

		Path path = Files.createTempFile(null, null);
		try (OutputStream st = Files.newOutputStream(path)) {
			byte[] bytes = new byte[1000];
			for (int i = 0; i < 100000; i++) {
				st.write(bytes);
			}
		}
		logTime("File created");

		Investigation inv = (Investigation) icat.search(sessionId, "Investigation").get(0);
		DatasetType dst = (DatasetType) icat.search(sessionId, "DatasetType").get(0);

		Dataset ds = new Dataset();
		ds.setName("Big ds");
		ds.setType(dst);
		ds.setInvestigation(inv);
		long dsid = icat.create(sessionId, ds);
		ds.setId(dsid);

		for (int i = 0; i < 30; i++) {
			testingClient.put(sessionId, Files.newInputStream(path), "uploaded_file" + i, dsid,
					supportedDatafileFormat.getId(), "A rather splendid datafile", 201);
		}
		testingClient.archive(sessionId, new DataSelection().addDataset(dsid), 204);
		logTime("Put and archive calls");

		waitForIds(300);

		logTime("Archive complete");
		testingClient.restore(sessionId, new DataSelection().addDataset(dsid), 204);
		while (testingClient.getStatus(sessionId, new DataSelection().addDataset(dsid), 200) != Status.ONLINE) {
			Thread.sleep(1000);
		}
		logTime("Marked online");

		ServiceStatus stat = testingClient.getServiceStatus(sessionId, 200);

		assertTrue(stat.getOpItems().toString(), stat.getOpItems().isEmpty());

		testingClient.delete(sessionId, new DataSelection().addDataset(dsid), 204);
		icat.delete(sessionId, ds);

		waitForIds(300);
		logTime("Deleted");

		Files.delete(path);

	}

}
