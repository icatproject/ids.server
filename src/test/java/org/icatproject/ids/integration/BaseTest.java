package org.icatproject.ids.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.json.Json;
import javax.json.JsonReader;

import org.icatproject.Datafile;
import org.icatproject.DatafileFormat;
import org.icatproject.Dataset;
import org.icatproject.DatasetType;
import org.icatproject.EntityBaseBean;
import org.icatproject.Facility;
import org.icatproject.IcatException_Exception;
import org.icatproject.Investigation;
import org.icatproject.InvestigationType;
import org.icatproject.icat.client.ICAT;
import org.icatproject.icat.client.Session;
import org.icatproject.ids.ICATGetter;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.TestingClient;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
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

	private static long timestamp = System.currentTimeMillis();

	protected Path getDirOnFastStorage(Long dsId) throws IcatException_Exception {
		Dataset icatDs = (Dataset) icatWS.get(sessionId, "Dataset INCLUDE Investigation", dsId);
		return setup.getStorageDir().resolve(Long.toString(icatDs.getInvestigation().getId()))
				.resolve(Long.toString(icatDs.getId()));
	}

	protected Path getFileOnArchiveStorage(Long dsId) throws IcatException_Exception {
		Dataset icatDs = (Dataset) icatWS.get(sessionId, "Dataset INCLUDE Investigation", dsId);
		return setup.getStorageArchiveDir().resolve(Long.toString(icatDs.getInvestigation().getId()))
				.resolve(Long.toString(icatDs.getId()));
	}

	protected static org.icatproject.ICAT icatWS;
	protected static org.icatproject.icat.client.ICAT icat;
	protected static Setup setup = null;

	public static void icatsetup() throws Exception {
		String icatUrl = setup.getIcatUrl().toString();
		icatWS = ICATGetter.getService(icatUrl);
		icat = new org.icatproject.icat.client.ICAT(icatUrl);
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
		if (msg != null) {
			if (time != 0) {
				System.out.println(msg + " took " + (now - time) / 1000. + "s.");
			} else {
				System.out.println(msg);
			}
		}
		time = now;
	}

	protected void checkZipFile(Path file, List<Long> ids, int compressedSize) throws IOException {
		checkZipStream(Files.newInputStream(file), ids, compressedSize, 0);
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

			List<Object> objects = icatWS.search(sessionId, "Facility");
			List<EntityBaseBean> facilities = new ArrayList<>();
			for (Object o : objects) {
				facilities.add((Facility) o);
			}

			icatWS.deleteMany(sessionId, facilities);

			Facility fac = new Facility();
			fac.setName("Facility_" + timestamp);
			fac.setId(icatWS.create(sessionId, fac));

			DatasetType dsType = new DatasetType();
			dsType.setFacility(fac);
			dsType.setName("DatasetType_" + timestamp);
			dsType.setId(icatWS.create(sessionId, dsType));

			supportedDatafileFormat = new DatafileFormat();
			supportedDatafileFormat.setFacility(fac);
			supportedDatafileFormat.setName("test_format");
			supportedDatafileFormat.setVersion("42.0.0");
			supportedDatafileFormat.setId(icatWS.create(sessionId, supportedDatafileFormat));

			InvestigationType invType = new InvestigationType();
			invType.setName("Not null");
			invType.setFacility(fac);
			invType.setId(icatWS.create(sessionId, invType));

			Investigation inv = new Investigation();
			inv.setName("Investigation_" + timestamp);
			inv.setType(invType);
			inv.setTitle("Not null");
			inv.setFacility(fac);
			inv.setVisitId("N/A");
			inv.setId(icatWS.create(sessionId, inv));
			investigationId = inv.getId();
			String invLoc = inv.getId() + "/";

			Dataset ds1 = new Dataset();
			ds1.setName("ds1_" + timestamp);
			ds1.setLocation(invLoc + ds1.getId());
			ds1.setType(dsType);
			ds1.setInvestigation(inv);
			ds1.setId(icatWS.create(sessionId, ds1));
			String ds1Loc = invLoc + ds1.getId() + "/";

			Dataset ds2 = new Dataset();
			ds2.setName("ds2_" + timestamp);
			ds2.setLocation(invLoc + ds2.getId());
			ds2.setType(dsType);
			ds2.setInvestigation(inv);
			ds2.setId(icatWS.create(sessionId, ds2));
			String ds2Loc = invLoc + ds2.getId() + "/";

			Dataset ds3 = new Dataset();
			ds3.setName("ds3_" + timestamp);
			ds3.setLocation(invLoc + ds3.getId());
			ds3.setType(dsType);
			ds3.setInvestigation(inv);
			ds3.setId(icatWS.create(sessionId, ds3));

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

			datasetIds.add(ds1.getId());
			datasetIds.add(ds2.getId());
			datasetIds.add(ds3.getId());

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

	protected void checkZipStream(InputStream stream, List<Long> datafileIdsIn, long compressedSize, int numLeft)
			throws IOException {
		ZipInputStream zis = new ZipInputStream(stream);
		ZipEntry ze = zis.getNextEntry();
		List<Long> idsNeeded = new ArrayList<>(datafileIdsIn);
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
		assertEquals(numLeft, idsNeeded.size());
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

	private void writeToFile(Datafile df, String content, String key)
			throws IOException, IcatException_Exception, NoSuchAlgorithmException {
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
		long dfId = icatWS.create(sessionId, df);
		df.setId(dfId);
		if (key != null) {
			String location = df.getLocation();
			df.setLocation(location + " " + digest(dfId, location, key));
		}
		icatWS.update(sessionId, df);

		crcs.put(df.getLocation(), df.getChecksum());
		fsizes.put(df.getLocation(), df.getFileSize());
		contents.put(df.getLocation(), content);
		ids.put(df.getLocation(), df.getId());
		Dataset ds = df.getDataset();
		Investigation inv = ds.getInvestigation();
		Facility fac = inv.getFacility();
		paths.put("ids/" + fac.getName() + "/" + inv.getName() + "/" + URLEncoder.encode(inv.getVisitId(), "UTF-8")
				+ "/" + ds.getName() + "/" + df.getName(), df.getLocation());
	}

	private static final char[] HEX_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E',
			'F' };

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

	private void moveDatasetToArchive(String storageUnit, Dataset ds, String dsLoc, Facility fac, Investigation inv,
			String key) throws IOException, IcatException_Exception {
		ds = (Dataset) icatWS.get(sessionId, "Dataset INCLUDE Datafile", ds.getId());
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
					file = top.resolve(getLocationFromDigest(df.getId(), df.getLocation()));
				}
				InputStream fis = Files.newInputStream(file);

				zos.putNextEntry(new ZipEntry("ids/" + fac.getName() + "/" + inv.getName() + "/"
						+ URLEncoder.encode(inv.getVisitId(), "UTF-8") + "/" + ds.getName() + "/" + df.getName()));
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
					p = top.resolve(getLocationFromDigest(df.getId(), df.getLocation()));
				}
				Files.move(p, archive.resolve(p.getFileName()), StandardCopyOption.REPLACE_EXISTING);
			}
		}
		for (Datafile df : ds.getDatafiles()) {
			Path file;
			if (key == null) {
				file = top.resolve(df.getLocation());
			} else {
				file = top.resolve(getLocationFromDigest(df.getId(), df.getLocation()));
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

	protected String getLocationFromDigest(Long id, String locationWithHash) {
		int i = locationWithHash.lastIndexOf(' ');
		return locationWithHash.substring(0, i);
	}

	protected void getIcatUrlTest() throws Exception {
		System.out.println(testingClient.getIcatUrl(200));
	}

	protected void apiVersionTest() throws Exception {
		assertTrue(testingClient.getApiVersion(200).startsWith("1.8."));
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

		Investigation inv = (Investigation) icatWS.search(sessionId, "Investigation").get(0);
		DatasetType dst = (DatasetType) icatWS.search(sessionId, "DatasetType").get(0);

		Dataset ds = new Dataset();
		ds.setName("Big ds");
		ds.setType(dst);
		ds.setInvestigation(inv);
		long dsid = icatWS.create(sessionId, ds);
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
		icatWS.delete(sessionId, ds);

		waitForIds(300);
		logTime("Deleted");

		Files.delete(path);

	}

	public void getDatafileIdsTest() throws Exception {

		List<Long> ids = testingClient.getDatafileIds(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDatafile(datafileIds.get(0)), 200);
		assertEquals(2, ids.size());
		assertTrue(ids.contains(datafileIds.get(0)));
		assertTrue(ids.contains(datafileIds.get(1)));

		ids = testingClient.getDatafileIds(sessionId, new DataSelection().addDatafile(datafileIds.get(0)), 200);
		assertEquals(1, ids.size());
		assertTrue(ids.contains(datafileIds.get(0)));

		ids = testingClient.getDatafileIds(sessionId, new DataSelection().addInvestigation(investigationId), 200);
		assertEquals(4, ids.size());
		for (Long id : datafileIds) {
			assertTrue(ids.contains(id));
		}

	}

	public void bigDataSelectionTest() throws Exception {
		String icatUrl = testingClient.getIcatUrl(200).toExternalForm();
		ICAT restIcat = new org.icatproject.icat.client.ICAT(icatUrl);
		try (JsonReader parser = Json.createReader(new ByteArrayInputStream(restIcat.getProperties().getBytes()))) {
			assertEquals("maxEntities must have a fixed value in the icat.server for test to be useful", 20,
					parser.readObject().getInt("maxEntities"));
		}

		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 204);

		waitForIds();
		List<Long> idList = new ArrayList<>();
		for (int i = 0; i < 45; i++) {
			Long dfid = testingClient.put(sessionId, Files.newInputStream(newFileLocation),
					"uploaded_file3_" + timestamp + i, datasetIds.get(0), supportedDatafileFormat.getId(),
					"A rather splendid datafile", 201);
			idList.add(dfid);
		}
		waitForIds();
		List<Long> idList2 = testingClient.getDatafileIds(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDatafile(datafileIds.get(0)), 200);
		assertEquals(47, idList2.size());
		for (Long id : idList) {
			assertTrue(idList2.contains(id));
		}

		DataSelection dsel = new DataSelection();
		for (int i = 0; i < 30; i++) {
			dsel.addDatafile(idList.get(i));
		}
		idList2 = testingClient.getDatafileIds(sessionId, dsel, 200);

		assertEquals(30, idList2.size());
		for (int i = 0; i < 30; i++) {
			assertTrue(idList2.contains(idList.get(i)));
		}

		idList2 = testingClient.getDatafileIds(sessionId, new DataSelection().addInvestigation(investigationId), 200);
		assertEquals(49, idList2.size());
	}

	public void cloningTest() throws Exception {
		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 204);
		waitForIds();
		assertEquals(104, testingClient.getSize(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 200));
		Session s = icat.getSession(sessionId);
		Map<String, String> m = new HashMap<>();
		m.put("name", "newOne");
		long dfid = datafileIds.get(0);
		long ndfid = s.cloneEntity("Datafile", dfid, m);
		Datafile df = (Datafile) icatWS.get(sessionId, "Datafile df INCLUDE df.dataset.investigation.facility", ndfid);
		Dataset ds = df.getDataset();
		Investigation inv = ds.getInvestigation();
		Facility fac = inv.getFacility();
		paths.put("ids/" + fac.getName() + "/" + inv.getName() + "/" + URLEncoder.encode(inv.getVisitId(), "UTF-8")
				+ "/" + ds.getName() + "/" + df.getName(), df.getLocation());
		crcs.put(df.getLocation(), df.getChecksum());
		fsizes.put(df.getLocation(), df.getFileSize());
		contents.put(df.getLocation(), "df1 test content very compressible very compressible");
		ids.put(df.getLocation(), df.getId());
		assertEquals(156, testingClient.getSize(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 200));
		try (InputStream stream = testingClient.getData(sessionId, new DataSelection().addDataset(datasetIds.get(0)),
				Flag.NONE, 0, 200)) {
			List<Long> dfids = new ArrayList<>(datafileIds.subList(0, 2));
			dfids.add(ndfid);
			checkZipStream(stream, dfids, 57, 0);
		}
		long dsid = datasetIds.get(0);
		assertEquals(2, getDirOnFastStorage(dsid).toFile().list().length);
		assertEquals("[3]", s.search("SELECT COUNT(df) from Datafile df WHERE df.dataset.id = " + dsid));
		testingClient.delete(sessionId, new DataSelection().addDatafile(datafileIds.get(0)), 204);
		assertEquals(2, getDirOnFastStorage(datasetIds.get(0)).toFile().list().length);
		assertEquals("[2]", s.search("SELECT COUNT(df) from Datafile df WHERE df.dataset.id = " + dsid));
		testingClient.delete(sessionId, new DataSelection().addDatafile(datafileIds.get(1)), 204);
		assertEquals(1, getDirOnFastStorage(datasetIds.get(0)).toFile().list().length);
		assertEquals("[1]", s.search("SELECT COUNT(df) from Datafile df WHERE df.dataset.id = " + dsid));
		testingClient.delete(sessionId, new DataSelection().addDatafile(ndfid), 204);
		assertFalse(getDirOnFastStorage(datasetIds.get(0)).toFile().exists());
		assertEquals("[0]", s.search("SELECT COUNT(df) from Datafile df WHERE df.dataset.id = " + dsid));
	}

	public void reliabilityTest() throws Exception {

		testingClient.restore(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDataset(datasetIds.get(1)), 204);
		waitForIds();

		Long dfid1 = testingClient.put(sessionId, Files.newInputStream(newFileLocation), "uploaded_file2_" + timestamp,
				datasetIds.get(0), supportedDatafileFormat.getId(), "A rather splendid datafile", 201);

		testingClient.archive(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDataset(datasetIds.get(1)), 204);
		waitForIds();

		assertTrue(testingClient.getServiceStatus(sessionId, 200).getFailures().isEmpty());

		setup.setReliability(0.);

		String preparedId = testingClient.prepareData(sessionId, new DataSelection().addDatafile(dfid1), Flag.NONE,
				200);

		try {
			while (!testingClient.isPrepared(preparedId, null)) {
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			System.out.println(e);
			assertEquals("Restore failed", e.getMessage());
			Set<Long> failures = testingClient.getServiceStatus(sessionId, 200).getFailures();
			assertEquals(1, failures.size());
			setup.setReliability(1.);
			testingClient.reset(preparedId, 204);
			assertTrue(testingClient.getServiceStatus(sessionId, 200).getFailures().isEmpty());
			while (!testingClient.isPrepared(preparedId, null)) {
				Thread.sleep(1000);
			}
		}

		setup.setReliability(0.);
		Long dfid2 = testingClient.put(sessionId, Files.newInputStream(newFileLocation), "uploaded_file3_" + timestamp,
				datasetIds.get(0), supportedDatafileFormat.getId(), "An even better datafile", "7.1.3",
				new Date(420000), new Date(42000), 201);
		waitForIds();
		System.out.println(testingClient.getStatus(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDatafile(dfid2), 200));
		setup.setReliability(1.);
		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0)).addDatafile(dfid2), 204);
		waitForIds();
		System.out.println(testingClient.getStatus(sessionId,
				new DataSelection().addDataset(datasetIds.get(0)).addDatafile(dfid2), 200));

	}

	protected void reliabilityTest2() throws Exception {
		DataSelection dsel = new DataSelection().addDatafile(datafileIds.get(0));
		testingClient.archive(sessionId, dsel, 204);
		waitForIds();

		setup.setReliability(0.);
		String preparedId = testingClient.prepareData(sessionId, dsel, Flag.NONE, 200);

		try {
			while (!testingClient.isPrepared(preparedId, null)) {
				Thread.sleep(1000);
			}
			fail("Should throw an error");
		} catch (Exception e) {
			assertEquals("Restore failed", e.getMessage());
		}

		setup.setReliability(1.);
		preparedId = testingClient.prepareData(sessionId, dsel, Flag.NONE, 200);
		while (!testingClient.isPrepared(preparedId, null)) {
			Thread.sleep(1000);
		}

		preparedId = testingClient.prepareData(sessionId, dsel, Flag.NONE, 200);
		while (!testingClient.isPrepared(preparedId, null)) {
			Thread.sleep(1000);
		}

	}

	protected void reliabilityTest3() throws Exception {
		DataSelection dsel = new DataSelection().addDatafile(datafileIds.get(0));
		testingClient.archive(sessionId, dsel, 204);
		waitForIds();

		setup.setReliability(0.);
		testingClient.restore(sessionId, dsel, 204);

		try {
			while (testingClient.getStatus(sessionId, dsel, null) != Status.ONLINE) {
				Thread.sleep(1000);
			}
			fail("Should throw an error");
		} catch (Exception e) {
			assertEquals("Restore failed", e.getMessage());
		}

		setup.setReliability(1.);
		testingClient.restore(sessionId, dsel, 204);
		try {
			while (testingClient.getStatus(sessionId, dsel, null) != Status.ONLINE) {
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			assertEquals("Restore failed", e.getMessage());
		}

		testingClient.reset(sessionId, dsel, 204);
		testingClient.restore(sessionId, dsel, 204);
		while (testingClient.getStatus(sessionId, dsel, null) != Status.ONLINE) {
			Thread.sleep(1000);
		}
	}

	protected void isPreparedTest() throws Exception {

		int numDs = 30;
		int numDf = 19;

		Investigation inv = (Investigation) icatWS.search(sessionId, "Investigation INCLUDE Facility").get(0);
		String invLoc = inv.getId() + "/";
		DatasetType dsType = (DatasetType) icatWS.search(sessionId, "DatasetType").get(0);
		Facility fac = inv.getFacility();

		String key = setup.getKey();
		boolean twoLevel = setup.isTwoLevel();
		String storageUnit = setup.getStorageUnit();

		logTime("Starting");
		DataSelection dsel = new DataSelection();
		long dsid = 0;
		for (int i = 0; i < numDs; i++) {
			Dataset ds = new Dataset();
			ds.setName("ds1_" + i);

			ds.setLocation(invLoc + ds.getId());
			ds.setType(dsType);
			ds.setInvestigation(inv);

			dsid = icatWS.create(sessionId, ds);
			String dsLoc = invLoc + dsid + "/";
			ds.setId(dsid);
			for (int j = 0; j < numDf; j++) {

				Datafile df = new Datafile();
				df.setName("a/df1_" + j);
				df.setLocation(dsLoc + UUID.randomUUID());

				df.setDataset(ds);
				writeToFile(df, "42", key);
			}
			dsel.addDataset(dsid);
			if (twoLevel) {
				moveDatasetToArchive(storageUnit, ds, dsLoc, fac, inv, key);
			}
		}
		waitForIds(300);

		logTime("Put calls");

		String preparedId = testingClient.prepareData(sessionId, dsel, Flag.ZIP, 200);
		logTime("Data prepared");

		while (!testingClient.isPrepared(preparedId, 200)) {
			logTime("Not ready");
			Thread.sleep(500);
			logTime(null);
		}
		logTime("Prepared");

		testingClient.isPrepared(preparedId, 200);
		logTime("isPrepared after prepared");

		try (InputStream stream = testingClient.getData(preparedId, 0, 200)) {
			int l = getOutput(stream).length;
			logTime("Read " + l + " bytes");
		}

		testingClient.archive(sessionId, new DataSelection().addDataset(dsid), 204);
		waitForIds(300);
		logTime(null);

		try (InputStream stream = testingClient.getData(preparedId, 0, 404)) {
			fail("Should have thrown an error");
		} catch (Exception e) {
			// Expected
		}

		while (!testingClient.isPrepared(preparedId, 200)) {
			logTime("Not ready");
			Thread.sleep(500);
			logTime(null);
		}

		logTime("Prepared");
		testingClient.isPrepared(preparedId, 200);
		logTime("isPrepared after prepared");

		try (InputStream stream = testingClient.getData(preparedId, 0, 200)) {
			int l = getOutput(stream).length;
			logTime("Read " + l + " bytes");
		}
	}

}
