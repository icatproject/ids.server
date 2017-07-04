package org.icatproject.ids.integration.two;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.icatproject.Datafile;
import org.icatproject.DatafileFormat;
import org.icatproject.Dataset;
import org.icatproject.DatasetType;
import org.icatproject.Facility;
import org.icatproject.Investigation;
import org.icatproject.InvestigationType;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.junit.BeforeClass;
import org.junit.Test;

public class FileCheckerTest extends BaseTest {

	private static Path errorLog;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
		errorLog = setup.getErrorLog();
	}

	@Test
	public void everythingTest() throws Exception {

		Path fileOnArchiveStorage = getFileOnArchiveStorage(datasetIds.get(0));

		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 204);

		waitForIds();

		testingClient.delete(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 204);

		Long dfid = 0L;
		for (int i = 0; i < 3; i++) {
			dfid = testingClient.put(sessionId, Files.newInputStream(newFileLocation), "uploaded_file_" + i,
					datasetIds.get(0), supportedDatafileFormat.getId(), "A rather splendid datafile", 201);
		}

		Datafile df = (Datafile) icatWS.get(sessionId, "Datafile INCLUDE 1", dfid);

		waitForIds();

		Files.deleteIfExists(errorLog);

		Long fileSize = df.getFileSize();
		String checksum = df.getChecksum();

		df.setFileSize(fileSize + 1);
		icatWS.update(sessionId, df);

		checkHas("Dataset", datasetIds.get(0), "file size wrong");

		df.setFileSize(null);
		icatWS.update(sessionId, df);
		Files.deleteIfExists(errorLog);
		checkHas("Dataset", datasetIds.get(0), "file size null");

		df.setFileSize(fileSize);
		df.setChecksum("Aardvark");
		icatWS.update(sessionId, df);
		Files.deleteIfExists(errorLog);
		checkHas("Dataset", datasetIds.get(0), "checksum wrong");

		df.setChecksum(null);
		icatWS.update(sessionId, df);
		Files.deleteIfExists(errorLog);
		checkHas("Dataset", datasetIds.get(0), "checksum null");

		df.setChecksum(checksum);
		icatWS.update(sessionId, df);
		Files.delete(fileOnArchiveStorage);
		Files.deleteIfExists(errorLog);
		checkHas("Dataset", datasetIds.get(0), "/" + datasetIds.get(0));
	}

	@Test
	public void badZip() throws Exception {

		Path fileOnArchiveStorage = getFileOnArchiveStorage(datasetIds.get(0));

		testingClient.restore(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 204);

		waitForIds();

		testingClient.delete(sessionId, new DataSelection().addDataset(datasetIds.get(0)), 204);

		for (int i = 0; i < 3; i++) {
			testingClient.put(sessionId, Files.newInputStream(newFileLocation), "uploaded_file_" + i, datasetIds.get(0),
					supportedDatafileFormat.getId(), "A rather splendid datafile", 201);
		}

		long timestamp = System.currentTimeMillis();

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

		for (int i = 0; i < 10; i++) {
			Dataset ds1 = new Dataset();
			ds1.setName("ds1_" + i);
			ds1.setLocation(invLoc + ds1.getId());
			ds1.setType(dsType);
			ds1.setInvestigation(inv);
			ds1.setId(icatWS.create(sessionId, ds1));

			testingClient.put(sessionId, Files.newInputStream(newFileLocation), "uploaded_file_1" + timestamp,
					ds1.getId(), supportedDatafileFormat.getId(), null, 201);

			testingClient.put(sessionId, Files.newInputStream(newFileLocation), "uploaded_file_2" + timestamp,
					ds1.getId(), supportedDatafileFormat.getId(), null, 201);
		}

		System.out.println("About to wait");
		waitForIds();

		truncate(fileOnArchiveStorage, 300);
		Files.deleteIfExists(errorLog);
		checkHas("Dataset", datasetIds.get(0), "java.io.EOFException Unexpected end of ZLIB input stream");

		truncate(fileOnArchiveStorage, 0);
		Files.deleteIfExists(errorLog);
		checkHas("Dataset", datasetIds.get(0), "zip file incomplete");

	}

	private void truncate(Path fileOnArchiveStorage, int size) throws IOException {
		System.out.println(fileOnArchiveStorage + " will be truncated to " + size);
		InputStream is = Files.newInputStream(fileOnArchiveStorage);
		Path t = Files.createTempFile(null, null);
		OutputStream os = Files.newOutputStream(t);

		byte[] buf = new byte[10000];
		int off = 0;
		int len = buf.length;
		int n;

		while ((n = is.read(buf, off, len)) > 0) {
			off += n;
			len -= n;
		}
		os.write(buf, 0, size);
		is.close();
		os.close();
		Files.move(t, fileOnArchiveStorage, StandardCopyOption.REPLACE_EXISTING);
	}

	private void checkHas(String type, Long id, String message) throws IOException, InterruptedException {
		Set<String> lines = new HashSet<String>();
		System.out.println("Watching " + errorLog);
		while (!Files.exists(errorLog)) {
			Thread.sleep(10);
		}
		for (String line : Files.readAllLines(errorLog, Charset.defaultCharset())) {
			lines.add(line.substring(21));
		}
		assertEquals(1, lines.size());
		String msg = new ArrayList<String>(lines).get(0);
		assertTrue(msg + ":" + type + " " + id, msg.startsWith(type + " " + id));
		assertTrue(msg + ":" + message, msg.endsWith(message));
	}

}
