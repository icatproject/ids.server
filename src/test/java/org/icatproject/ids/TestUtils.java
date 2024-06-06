package org.icatproject.ids;

import java.util.Arrays;
import java.util.List;

import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;
import org.icatproject.Login.Credentials;
import org.icatproject.Login.Credentials.Entry;

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

}
