package org.icatproject.ids.integration.one;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class PerformanceTest extends BaseTest {

	private static long timestamp;
	private static Path infile;

	@BeforeClass
	public static void beforeClass() throws Exception {
		setup = new Setup("one.properties");
		icatsetup();
		infile = Files.createTempFile("idstest", null);
		byte[] junk = new byte[1000];
		int n = 100000;

		start = System.currentTimeMillis();
		try (OutputStream f = Files.newOutputStream(infile, StandardOpenOption.CREATE)) {
			for (int i = 0; i < n; i++) {
				f.write(junk);
			}
		}

		ts("create file of size " + Files.size(infile));
	}

	@AfterClass
	public static void afterClass() throws Exception {
		Files.delete(infile);
	}

	private static long start;
	private static long end;

	@Test
	public void putOneFileTest() throws Exception {
		byte[] junk = new byte[1000];
		start = System.currentTimeMillis();
		Long dfid = testingClient.put(sessionId, Files.newInputStream(infile), "big_" + timestamp,
				datasetIds.get(0), supportedDatafileFormat.getId(), "A big datafile", 201);
		ts("store file (put)");

		try (InputStream stream = testingClient.getData(sessionId,
				new DataSelection().addDatafile(dfid), Flag.NONE, null, 0, null)) {
			boolean first = true;
			while (stream.read(junk) > 0) {
				if (first) {
					ts("get first 1000 bytes");
					first = false;
				}
			}
		}
		ts("get last byte");
	}

	private static void ts(String msg) {
		end = System.currentTimeMillis();
		System.out.println("Time to " + msg + ": " + (end - start) + "ms.");
		start = end;
	}

	@Test
	public void putAsPostOneFileTest() throws Exception {
		byte[] junk = new byte[1000];
		start = System.currentTimeMillis();
		Long dfid = testingClient.putAsPost(sessionId, Files.newInputStream(infile), "big2_"
				+ timestamp, datasetIds.get(0), supportedDatafileFormat.getId(),
				"A rather splendid datafile", null, null, null, true, 201);
		ts("store file (post)");

		int ntot = 0;
		try (InputStream stream = testingClient.getData(sessionId,
				new DataSelection().addDatafile(dfid), Flag.NONE, null, 0, null)) {
			boolean first = true;
			int n;

			while ((n = stream.read(junk)) > 0) {
				ntot += n;
				if (first) {
					ts("get first 1000 bytes");
					first = false;
				}
			}
		}
		ts("get last byte of " + ntot);
	}
}
