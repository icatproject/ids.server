package org.icatproject.ids.integration.two;

/*
 * Test various error conditions in the DsRestorer caused by ZIP files
 * in archive storage having unexpected content.
 */

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.Test;

import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InternalException;
import org.icatproject.ids.integration.util.client.TestingClient.Status;

public class RestoreErrorsTest extends BaseTest {

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup("two.properties");
        icatsetup();
    }

    /*
     * Note that we cannot test for DUPLICATE_ENTRY here, because
     * ZipOutputStream() won't allow us to create such a defective ZIP file. But
     * that doesn't mean that this error cannot occur.
     */
    private enum Defect {
        NONE, MISSING_ENTRY, SPURIOUS_ENTRY, DUPLICATE_ENTRY
    }

    private void cloneZip(Path archivepath, Defect defect) throws IOException {
        Path savepath = archivepath.getParent().resolve(".sav");
        Files.move(archivepath, savepath);
        try (ZipOutputStream zipout = new ZipOutputStream(
                Files.newOutputStream(archivepath))) {
            try (ZipInputStream zipin = new ZipInputStream(
                    Files.newInputStream(savepath))) {
                ZipEntry entry = zipin.getNextEntry();
                boolean first = true;
                String entryName = "";
                while (entry != null) {
                    if (first && defect == Defect.MISSING_ENTRY) {
                        entry = zipin.getNextEntry();
                    }
                    first = false;
                    entryName = entry.getName();
                    zipout.putNextEntry(new ZipEntry(entryName));
                    byte[] bytes = new byte[8192];
                    int length;
                    while ((length = zipin.read(bytes)) >= 0) {
                        zipout.write(bytes, 0, length);
                    }
                    zipout.closeEntry();
                    entry = zipin.getNextEntry();
                }
            }
            if (defect == Defect.SPURIOUS_ENTRY) {
                zipout.putNextEntry(new ZipEntry("ids/spurious_entry"));
                byte[] bytes = new byte[64];
                zipout.write(bytes, 0, 64);
                zipout.closeEntry();
            }
        }
    }

    /*
     * As a reference: a restore with no errors.
     */
    @Test
    public void restoreOk() throws Exception {
        Long dsId = datasetIds.get(1);
        Path archivefile = getFileOnArchiveStorage(dsId);
        Path dirOnFastStorage = getDirOnFastStorage(dsId);
        DataSelection selection = new DataSelection().addDataset(dsId);
        cloneZip(archivefile, Defect.NONE);
        testingClient.restore(sessionId, selection, 204);
        waitForIds();
        checkPresent(dirOnFastStorage);
    }

    /*
     * A missing entry in the archive.
     */
    @Test
    public void restoreMissing() throws Exception {
        Long dsId = datasetIds.get(1);
        Path archivefile = getFileOnArchiveStorage(dsId);
        Path dirOnFastStorage = getDirOnFastStorage(dsId);
        DataSelection selection = new DataSelection().addDataset(dsId);
        cloneZip(archivefile, Defect.MISSING_ENTRY);
        testingClient.restore(sessionId, selection, 204);
        waitForIds();
        checkAbsent(dirOnFastStorage);
        try {
            testingClient.getStatus(sessionId, selection, null);
            fail("Expected InternalException to be thrown.");
        } catch (InternalException e) {
            assertEquals("Restore failed", e.getMessage());
        }
        testingClient.reset(sessionId, selection, 204);
        Status status = testingClient.getStatus(sessionId, selection, 200);
        assertEquals(Status.ARCHIVED, status);
    }

    /*
     * A spurious entry in the archive.
     */
    @Test
    public void restoreSpurious() throws Exception {
        Long dsId = datasetIds.get(1);
        Path archivefile = getFileOnArchiveStorage(dsId);
        Path dirOnFastStorage = getDirOnFastStorage(dsId);
        DataSelection selection = new DataSelection().addDataset(dsId);
        cloneZip(archivefile, Defect.SPURIOUS_ENTRY);
        testingClient.restore(sessionId, selection, 204);
        waitForIds();
        checkAbsent(dirOnFastStorage);
        try {
            testingClient.getStatus(sessionId, selection, null);
            fail("Expected InternalException to be thrown.");
        } catch (InternalException e) {
            assertEquals("Restore failed", e.getMessage());
        }
        testingClient.reset(sessionId, selection, 204);
        Status status = testingClient.getStatus(sessionId, selection, 200);
        assertEquals(Status.ARCHIVED, status);
    }
}
