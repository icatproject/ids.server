package org.icatproject.ids;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.json.Json;
import javax.json.JsonReader;

import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;
import org.icatproject.icat.client.IcatException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * This test was created to fix issue #115 and was run against the Diamond 
 * pre-production ICAT using specifically selected usernames of users known
 * to have access to a lot of data.
 * 
 * As well as monitoring the time taken to create the DataSelection, detailed
 * monitoring of the eclipselink SQL logging was done to confirm that the 
 * changes were having the desired effect at the database level.
 * 
 * It would be extremely difficult to recreate an equivalent setup on a test 
 * ICAT in order to perform repeatable tests so this has not been attempted.
 * However, this test was invaluable for testing during development and may
 * prove useful in the future, so has been added to the test suite even if it
 * remains Ignored.
 * 
 * The test runs from a small set of properties defined in  
 * DataSelectionDevTest.properties. These need to be configured to point at the
 * desired ICAT (not the one used by the integration tests), define user and 
 * reader login details, and define the lists of investigation, dataset and 
 * datafile IDs which the defined user has access to.
 */
public class DataSelectionDevTest {

    @Mock
    private PropertyHandler mockedPropertyHandler;

    private IcatReader icatReader;

    private Properties testProps;
    private String icatUrl;
    private ICAT icatService;
    private org.icatproject.icat.client.ICAT restIcat;
    private int maxEntities;
    private String userSessionId;
    private List<String> readerCreds;
    // comma separated IDs as a single string from the properties file
    private String investigationIds;
    private String datasetIds;
    private String datafileIds;
    private boolean useReaderForPerformance;

    @Before
    public void setup() throws Exception {
        testProps = new Properties();
        testProps.load(new FileInputStream("src/test/resources/DataSelectionDevTest.properties"));
        // set up the SOAP and REST ICAT clients
        icatUrl = ICATGetter.getCleanUrl(testProps.getProperty("icat.url"));
        icatService = ICATGetter.getService(icatUrl);
        restIcat = new org.icatproject.icat.client.ICAT(icatUrl);
        // get session IDs for an end user and the reader user (with read-all permissions)
        String userCredsString = testProps.getProperty("login.user");
        userSessionId = TestUtils.login(icatService, userCredsString);
        System.out.println("userSessionId = " + userSessionId);
        List<String> readerCreds = Arrays.asList(testProps.getProperty("login.reader").trim().split("\\s+"));
        this.readerCreds = readerCreds;
        investigationIds = testProps.getProperty("investigation.ids");
        datasetIds = testProps.getProperty("dataset.ids");
        datafileIds = testProps.getProperty("datafile.ids");
        useReaderForPerformance = testProps.getProperty("useReaderForPerformance").equalsIgnoreCase("true");
        // set up a mocked version of the PropertyHandler
        setupPropertyHandler();
        icatReader = new IcatReader(mockedPropertyHandler);
    }

    private void setupPropertyHandler()
            throws URISyntaxException, IcatException_Exception, IcatException {
        JsonReader parser = Json.createReader(new ByteArrayInputStream(restIcat.getProperties().getBytes()));
        maxEntities = parser.readObject().getInt("maxEntities");
        MockitoAnnotations.initMocks(this);
        when(mockedPropertyHandler.getMaxEntities()).thenReturn(maxEntities);
        when(mockedPropertyHandler.getIcatService()).thenReturn(icatService);
        when(mockedPropertyHandler.getRestIcat()).thenReturn(restIcat);
        when(mockedPropertyHandler.getReader()).thenReturn(readerCreds);
        when(mockedPropertyHandler.getUseReaderForPerformance()).thenReturn(useReaderForPerformance);
    }

    @Ignore("Test requires a specific ICAT setup to produce meaningful results. See class javadoc comment.")
    @Test
    public void testCreateDataSelection() throws Exception {
        long startMs = System.currentTimeMillis();
        DataSelection dataSelection = new DataSelection(mockedPropertyHandler, icatReader, userSessionId, 
                investigationIds, datasetIds, datafileIds, DataSelection.Returns.DATASETS_AND_DATAFILES);
        System.out.println("Creating DataSelection took " + (System.currentTimeMillis()-startMs) + " ms");
        System.out.println("DsInfo size: " + dataSelection.getDsInfo().size());
        System.out.println("DfInfo size: " + dataSelection.getDfInfo().size());
        // there must be at least one Datafile in the DataSelection
        assertTrue("message", dataSelection.getDfInfo().size()>0 );
    }
}
