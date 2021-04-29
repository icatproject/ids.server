package org.icatproject.ids;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;
import org.icatproject.Login.Credentials;
import org.icatproject.Login.Credentials.Entry;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.MainStorageInterface;

public class TestUtils {

    /**
     * Do an ICAT login and get the session ID using a list of credentials of
     * the format (as found in the properties files):
     * "db username READER password READERpass"
     *
     * @param icatService the ICAT to log in to
     * @param credsString a String of credentials in the format described above
     * @return an ICAT session ID
     * @throws IcatException_Exception if the login fails
     */
    public static String login(ICAT icatService, String credsString) throws IcatException_Exception {
        List<String> creds = Arrays.asList(credsString.trim().split("\\s+"));
        Credentials credentials = new Credentials();
        List<Entry> entries = credentials.getEntry();
        for (int i = 1; i < creds.size(); i += 2) {
            Entry entry = new Entry();
            entry.setKey(creds.get(i));
            entry.setValue(creds.get(i + 1));
            entries.add(entry);
        }
        return icatService.login(creds.get(0), credentials);
    }

    /**
     * Recursively delete a directory and everything below it
     * 
     * @param dirToDelete the Path of the directory to delete
     * @throws IOException
     */
    public static void recursivelyDeleteDirectory(Path dirToDelete) throws IOException {
        Files.walk(dirToDelete)
            .map(Path::toFile)
            .sorted((o1, o2) -> -o1.compareTo(o2))
            .forEach(File::delete);
    }

    /**
     * Check that all files from the list of DatafileInfo objects provided have
     * a corresponding file available on Main Storage (disk cache)
     * 
     * @param mainStorage an implementation of the IDS MainStorageInterface
     * @param dfInfos the list of DatafileInfo object to check
     * @throws IOException if any of the files are not found
     */
    public static void checkFilesOnMainStorage(MainStorageInterface mainStorage, 
            List<DfInfo> dfInfos) throws IOException {
        for (DfInfo dfInfo : dfInfos) {
            String filePath = dfInfo.getDfLocation();
            if (!mainStorage.exists(filePath)) {
                throw new IOException("File " + filePath + " not found on Main Storage");
            }
        }
    }

}
