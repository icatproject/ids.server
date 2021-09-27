package org.icatproject.ids;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TidierTest {

	private static Path tempFile;
	private static Path tempDir;

	@BeforeClass
	public static void beforeClass() throws Exception {
		tempFile = Files.createTempFile(TidierTest.class.getSimpleName(), null);
		Files.copy(new ByteArrayInputStream(new byte[2000]), tempFile,
				StandardCopyOption.REPLACE_EXISTING);
		tempDir = Files.createTempDirectory(TidierTest.class.getSimpleName());
	}

	@Test
	public void testDeleteOldestFilesFromDir() throws Exception {
		Path pa = tempDir.resolve("pa");
		Path pb = tempDir.resolve("pb");
		Path pc = tempDir.resolve("pc");
		Path pd = tempDir.resolve("pd");
		Path pe = tempDir.resolve("pe");
		Path pf = tempDir.resolve("pf");

		// create some files
		// (delay ensures different timestamps)
		Files.copy(tempFile, pa);
		Thread.sleep(1000);
		Files.copy(tempFile, pb);
		Thread.sleep(1000);
		Files.copy(tempFile, pc);
		Thread.sleep(1000);
		Files.copy(tempFile, pd);
		Thread.sleep(1000);
		Files.copy(tempFile, pe);
		Thread.sleep(1000);
		Files.copy(tempFile, pf);
		Thread.sleep(1000);

		// check they exist
		assertTrue(Files.exists(pa));
		assertTrue(Files.exists(pb));
		assertTrue(Files.exists(pc));
		assertTrue(Files.exists(pd));
		assertTrue(Files.exists(pe));
		assertTrue(Files.exists(pf));

		// run the tidier and check none are deleted
		Tidier.deleteOldestFilesFromDir(tempDir, 8);
		assertTrue(Files.exists(pa));
		assertTrue(Files.exists(pb));
		assertTrue(Files.exists(pc));
		assertTrue(Files.exists(pd));
		assertTrue(Files.exists(pe));
		assertTrue(Files.exists(pf));

		// run the tidier and check only the most recent 4 remain
		Tidier.deleteOldestFilesFromDir(tempDir, 4);
		assertFalse(Files.exists(pa));
		assertFalse(Files.exists(pb));
		assertTrue(Files.exists(pc));
		assertTrue(Files.exists(pd));
		assertTrue(Files.exists(pe));
		assertTrue(Files.exists(pf));

		// run the tidier and check only the most recent 2 remain
		Tidier.deleteOldestFilesFromDir(tempDir, 2);
		assertFalse(Files.exists(pa));
		assertFalse(Files.exists(pb));
		assertFalse(Files.exists(pc));
		assertFalse(Files.exists(pd));
		assertTrue(Files.exists(pe));
		assertTrue(Files.exists(pf));

		// run the tidier and check that no files remain
		Tidier.deleteOldestFilesFromDir(tempDir, 0);
		assertFalse(Files.exists(pa));
		assertFalse(Files.exists(pb));
		assertFalse(Files.exists(pc));
		assertFalse(Files.exists(pd));
		assertFalse(Files.exists(pe));
		assertFalse(Files.exists(pf));
	}

	@AfterClass
	public static void tearDownClass() throws Exception {
		tempFile.toFile().delete();
		tempDir.toFile().delete();
	}

}
