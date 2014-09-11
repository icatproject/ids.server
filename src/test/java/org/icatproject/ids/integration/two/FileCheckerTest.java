package org.icatproject.ids.integration.two;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.icatproject.Datafile;
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
			dfid = testingClient.put(sessionId, Files.newInputStream(newFileLocation),
					"uploaded_file_" + i, datasetIds.get(0), supportedDatafileFormat.getId(),
					"A rather splendid datafile", 201);
		}

		Datafile df = (Datafile) icat.get(sessionId, "Datafile INCLUDE 1", dfid);

		waitForIds();

		Files.deleteIfExists(errorLog);

		Long fileSize = df.getFileSize();
		String checksum = df.getChecksum();

		df.setFileSize(fileSize + 1);
		icat.update(sessionId, df);

		checkHas("Dataset", datasetIds.get(0), "file size wrong");

		df.setFileSize(null);
		icat.update(sessionId, df);
		Files.deleteIfExists(errorLog);
		checkHas("Dataset", datasetIds.get(0), "file size null");

		df.setFileSize(fileSize);
		df.setChecksum("Aardvark");
		icat.update(sessionId, df);
		Files.deleteIfExists(errorLog);
		checkHas("Dataset", datasetIds.get(0), "checksum wrong");

		df.setChecksum(null);
		icat.update(sessionId, df);
		Files.deleteIfExists(errorLog);
		checkHas("Dataset", datasetIds.get(0), "checksum null");

		df.setChecksum(checksum);
		icat.update(sessionId, df);
		Files.delete(fileOnArchiveStorage);
		Files.deleteIfExists(errorLog);
		checkHas("Dataset", datasetIds.get(0), "/" + datasetIds.get(0));
	}

	private void checkHas(String type, Long id, String message) throws IOException,
			InterruptedException {
		Set<String> lines = new HashSet<String>();
		while (!Files.exists(errorLog)) {
			Thread.sleep(10);
		}
		for (String line : Files.readAllLines(errorLog, Charset.defaultCharset())) {
			lines.add(line.substring(22));
		}
		assertEquals(1, lines.size());
		String msg = new ArrayList<String>(lines).get(0);
		assertTrue(msg + ":" + type + " " + id, msg.startsWith(type + " " + id));
		assertTrue(msg + ":" + message, msg.endsWith(message));
	}

}
