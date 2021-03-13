package org.icatproject.ids;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.icatproject.EntityBaseBean;
import org.icatproject.Grouping;
import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;
import org.icatproject.PublicStep;
import org.icatproject.Rule;
import org.icatproject.User;
import org.icatproject.UserGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class IcatReaderTest {

    @Mock
    private PropertyHandler mockedPropertyHandler;

    private String icatUrl;
    private ICAT icatService;
    private List<String> readerCreds;
    private String rootSessionId;

    private List<EntityBaseBean> createdIcatObjects = new ArrayList<>();
    private PublicStep publicStep;

    @Before
    public void setup() throws FileNotFoundException, IOException, IcatException_Exception {
        Properties testProps = new Properties();
        testProps.load(new FileInputStream("src/test/resources/test.properties"));
        readerCreds = Arrays.asList(testProps.getProperty("login.reader").trim().split("\\s+"));
        String rootCredsString = testProps.getProperty("login.root");
        // serverUrl must be defined in the maven ~/.m2/settings.xml file
        icatUrl = ICATGetter.getCleanUrl(System.getProperty("serverUrl"));
        icatService = ICATGetter.getService(icatUrl);
        rootSessionId = TestUtils.login(icatService, rootCredsString);
        setupPropertyHandler();
        createReaderAccessToPublicSteps();
    }

    private void setupPropertyHandler() {
        MockitoAnnotations.initMocks(this);
        when(mockedPropertyHandler.getIcatService()).thenReturn(icatService);
        when(mockedPropertyHandler.getReader()).thenReturn(readerCreds);
    }

    @Test
    public void testIcatReaderCreation() throws IcatException_Exception {
        IcatReader icatReader = new IcatReader(mockedPropertyHandler);
        assertNotNull("An ICAT session ID was expected", icatReader.getSessionId());
    }

    @Test
    public void testPublicStepAvailablability() throws IcatException_Exception {
        IcatReader icatReader1 = new IcatReader(mockedPropertyHandler);
        assertFalse("PublicStep Dataset to datafiles should not be available",
                icatReader1.isAvailablePublicStepDatasetToDatafile());
        createPublicStep();
        // even though the PublicStep now exists, this is not re-checked by the
        // IcatReader (expected/desired behaviour) so the IcatReader should
        // still report that the PublicStep is not available
        assertFalse("PublicStep Dataset to datafiles should still not be available",
                icatReader1.isAvailablePublicStepDatasetToDatafile());
        // however, if a new IcatReader is created it should be available
        IcatReader icatReader2 = new IcatReader(mockedPropertyHandler);
        assertTrue("PublicStep Dataset to datafiles should now be available",
                icatReader2.isAvailablePublicStepDatasetToDatafile());
    }

    private void createPublicStep() throws IcatException_Exception {
        publicStep = new PublicStep();
        publicStep.setOrigin("Dataset");
        publicStep.setField("datafiles");
        publicStep.setId(icatService.create(rootSessionId, publicStep));
        System.out.println("Created PublicStep with ID: " + publicStep.getId());
    }

    private void createReaderAccessToPublicSteps() throws IcatException_Exception {
        User readerUser = new User();
        // concatenate the mnemonic and the username from the reader credentials
        readerUser.setName(readerCreds.get(0) + "/" + readerCreds.get(2));
        readerUser.setId(icatService.create(rootSessionId, readerUser));
        createdIcatObjects.add(readerUser);
        System.out.println("Created User with ID: " + readerUser.getId());

        Grouping readAllGrouping = new Grouping();
        readAllGrouping.setName("readall");
        readAllGrouping.setId(icatService.create(rootSessionId, readAllGrouping));
        createdIcatObjects.add(readAllGrouping);
        System.out.println("Created Grouping with ID: " + readAllGrouping.getId());

        UserGroup userGroup = new UserGroup();
        userGroup.setUser(readerUser);
        userGroup.setGrouping(readAllGrouping);
        userGroup.setId(icatService.create(rootSessionId, userGroup));
        createdIcatObjects.add(userGroup);
        System.out.println("Created UserGroup with ID: " + userGroup.getId());

        Rule rule = new Rule();
        rule.setWhat("PublicStep");
        rule.setGrouping(readAllGrouping);
        rule.setCrudFlags("R");
        rule.setId(icatService.create(rootSessionId, rule));
        createdIcatObjects.add(rule);
        System.out.println("Created Rule with ID: " + rule.getId());
    }
    
    @After
    public void tearDown() throws IcatException_Exception {
        removeCreatedIcatObjects();
    }

    private void removeCreatedIcatObjects() throws IcatException_Exception {
        // delete the created ICAT objects in the reverse
        // order that they were created
        for (int i=createdIcatObjects.size()-1; i>=0; i--) {
            EntityBaseBean objectToDelete = createdIcatObjects.get(i);
            System.out.println("Deleting " + 
                    objectToDelete.getClass().getSimpleName() + 
                    " with ID: " + objectToDelete.getId());
            icatService.delete(rootSessionId, objectToDelete);
        }
        if (publicStep != null) {
            System.out.println("Deleting PublicStep with ID: " + publicStep.getId());
            icatService.delete(rootSessionId, publicStep);
        }
    }

}
