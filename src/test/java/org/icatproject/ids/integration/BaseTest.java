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
import java.nio.file.attribute.BasicFileAttributes;
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
import org.icatproject.ids.integration.util.client.TestingClient;
import org.icatproject.ids.integration.util.client.TestingClient.ServiceStatus;
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

	protected Path getDatasetCacheFile(Long dsId) throws IcatException_Exception {
		Dataset icatDs = (Dataset) icat.get(sessionId, "Dataset INCLUDE Investigation", dsId);
		return setup.getDatasetCacheDir().resolve(Long.toString(icatDs.getInvestigation().getId()))
				.resolve(Long.toString(icatDs.getId()));
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

	@Before
	public void before() throws Exception {
		testingClient = new TestingClient(setup.getIdsUrl());
		sessionId = setup.getGoodSessionId();
		waitForIds();
		populateStorage(setup.isTwoLevel());
	}

	protected void checkAbsent(Path file) {
		assertFalse(file.toString(), Files.exists(file));
	}

	protected void checkPresent(Path file) {
		assertTrue(file.toString(), Files.exists(file));
	}

	protected void waitForIds() throws Exception {
		int n = 0;
		while (true) {
			n++;
			if (n > 20) {
				throw new Exception("Got bored waiting");
			}
			ServiceStatus stat = testingClient.getServiceStatus(sessionId, 200);
			if (stat.getOpItems().isEmpty() && stat.getPrepItems().isEmpty()) {
				break;
			}
			if (n % 10 == 0) {
				if (!stat.getOpItems().isEmpty()) {
					System.out.println("Ops " + stat.getOpItems());
				}
				if (!stat.getPrepItems().isEmpty()) {
					System.out.println("Preps " + stat.getPrepItems());
				}
			}
			Thread.sleep(1000);
		}
	}

	protected void checkZipFile(Path file, List<Long> ids, int compressedSize) throws IOException {
		checkZipStream(Files.newInputStream(file), ids, compressedSize);
	}

	private void clearStorage() throws IOException {
		cleanDir(setup.getStorageDir());
		cleanDir(setup.getStorageArchiveDir());
		cleanDir(setup.getDatasetCacheDir());
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

	private void populateStorage(boolean twoLevel) throws IOException, IcatException_Exception {
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
			writeToFile(df1, "df1 test content very compressible very compressible");

			Datafile df2 = new Datafile();
			df2.setName("df2_" + timestamp);
			df2.setLocation(ds1Loc + UUID.randomUUID());
			df2.setDataset(ds1);
			writeToFile(df2, "df2 test content very compressible very compressible");

			// System.out.println("ds " + ds1.getId() + " holds dfs " + df1.getId() + " and "
			// + df2.getId());

			Datafile df3 = new Datafile();
			df3.setName("df3_" + timestamp);
			df3.setLocation(ds2Loc + UUID.randomUUID());
			df3.setDataset(ds2);
			writeToFile(df3, "df3 test content very compressible very compressible");

			Datafile df4 = new Datafile();
			df4.setName("df4_" + timestamp);
			df4.setLocation(ds2Loc + UUID.randomUUID());
			df4.setDataset(ds2);
			writeToFile(df4, "df4 test content very compressible very compressible");

			// System.out.println("ds " + ds2.getId() + " holds dfs " + df3.getId() + " and "
			// + df4.getId());

			datasetIds.add(ds1.getId());
			datasetIds.add(ds2.getId());

			datafileIds.add(df1.getId());
			datafileIds.add(df2.getId());
			datafileIds.add(df3.getId());
			datafileIds.add(df4.getId());

			if (twoLevel) {
				zipDatasetToArchive(ds1, ds1Loc, fac, inv);
				zipDatasetToArchive(ds2, ds2Loc, fac, inv);
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

	private void writeToFile(Datafile df, String content) throws IOException,
			IcatException_Exception {
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
		df.setId(icat.create(sessionId, df));
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

	private void zipDatasetToArchive(Dataset ds, String dsLoc, Facility fac, Investigation inv)
			throws IOException, IcatException_Exception {
		ds = (Dataset) icat.get(sessionId, "Dataset INCLUDE Datafile", ds.getId());

		Path zipFile = setup.getStorageArchiveDir().resolve(dsLoc);
		Files.createDirectories(zipFile.getParent());

		ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipFile));
		zos.setLevel(0);

		for (Datafile df : ds.getDatafiles()) {
			Path top = setup.getStorageDir();
			Path file = top.resolve(df.getLocation());
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
			Files.delete(file);
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

		zos.close();

	}

}
