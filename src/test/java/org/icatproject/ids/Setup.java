package org.icatproject.ids;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.namespace.QName;

import org.icatproject.ids.icatclient.icat42.ICAT;
import org.icatproject.ids.icatclient.icat42.ICATService;
import org.icatproject.ids.icatclient.icat42.IcatException_Exception;
import org.icatproject.ids.icatclient.icat42.Login.Credentials;
import org.icatproject.ids.icatclient.icat42.Login.Credentials.Entry;


/*
 * Setup the test environment for the IDS. This is done by reading property
 * values from the test.properties file and calling this class before 
 * running the tests.
 */
public class Setup {

    private Properties props = new Properties();
    
    private String icatUrl = null;
    private String idsUrl = null;
    
    private String goodSessionId = null;
    private String forbiddenSessionId = null;
    
    private static Map<String, String> filenameMD5 = null;
    private static ArrayList<String> datasetIds = null;
    private static ArrayList<String> datafileIds = null;

    public Setup() throws MalformedURLException, IcatException_Exception {
        InputStream is = getClass().getResourceAsStream("/test.properties");
        try {
          props.load(is);
        } catch (Exception e) {
            System.out.println("Problem loading test.properties\n" + e.getMessage());
        }

        icatUrl = props.getProperty("icat_url");
        idsUrl = props.getProperty("ids_url");
                
        goodSessionId = login(props.getProperty("valid_icat_username"), props.getProperty("valid_icat_password"));
        forbiddenSessionId = login(props.getProperty("invalid_icat_username"), props.getProperty("invalid_icat_password"));
        
        filenameMD5 = new HashMap<String, String>();
        filenameMD5.put(props.getProperty("df1_location"), props.getProperty("df1_md5"));
        filenameMD5.put(props.getProperty("df2_location"), props.getProperty("df2_md5"));

        datasetIds = new ArrayList<String>();
        datasetIds.add(props.getProperty("ds1_id"));

        datafileIds = new ArrayList<String>();
        datafileIds.add(props.getProperty("df1_id"));
        datafileIds.add(props.getProperty("df2_id"));
    }

    /*
     * This function may only work for ICAT version 4.2. If you want to use a different version, you
     * will need to include the appropriate ICATService, Entry, Credentials classes.
     */
    public String login(String username, String password) throws IcatException_Exception,
            MalformedURLException {
        ICAT icat = new ICATService(new URL(icatUrl), new QName("http://icatproject.org",
                "ICATService")).getICATPort();

        Credentials credentials = new Credentials();
        List<Entry> entries = credentials.getEntry();

        Entry u = new Entry();
        u.setKey("username");
        u.setValue(username);
        entries.add(u);

        Entry p = new Entry();
        p.setKey("password");
        p.setValue(password);
        entries.add(p);

        return icat.login("db", credentials);
    }
    
    public String getCommaSepDatafileIds() {
        return datafileIds.toString().replace("[", "").replace("]", "").replace(" ", "");
    }

    public String getForbiddenSessionId() {
        return forbiddenSessionId;
    }

    public String getGoodSessionId() {
        return goodSessionId;
    }
    
    public String getidsURL() throws MalformedURLException {
        return idsUrl;
    }
    
    public Map<String, String> getFilenameMD5() {
        return filenameMD5;
    }

    public ArrayList<String> getDatasetIds() {
        return datasetIds;
    }

    public ArrayList<String> getDatafileIds() {
        return datafileIds;
    }
}
