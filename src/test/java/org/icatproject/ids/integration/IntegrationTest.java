package org.icatproject.ids.integration;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.icatproject.Dataset;
import org.icatproject.Investigation;
import org.icatproject.ids.Constants;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.TestingClient;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTest extends BaseIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(IntegrationTest.class);

    private static TestingClient testingClient;

    @BeforeClass
	public static void setup() throws Exception {
//		setup = new Setup("integration.properties"); // for deploying the IDS

        setup = new Setup(); // for just setting up an ICAT URL and getting a rootSessionId
        testingClient = new TestingClient(setup.getIdsUrl());
        icatsetup();

        // these just require the default Setup() - not a redeploy of the IDS
        // cleanIcat();
        // populateIcat();
	}

    @Test
	// TODO: remove this - just for testing
    public void testPrintIcatContentsSummary() throws Exception {
		String rootSessionId = setup.getRootSessionId();
		List<Object> objects = icatSoapClient.search(rootSessionId, "Investigation INCLUDE Dataset, Datafile");
		for (Object o : objects) {
			Investigation inv = (Investigation)o;
			System.out.println("INV : " + inv.getName() + " " + inv.getId());
			for (Dataset ds : inv.getDatasets()) {
				System.out.println("    DS : " + ds.getName() + " " + ds.getId() + " " + ds.getDatafiles().size() + " files");
			}
		}
	}

    @Test
	public void testPrepareInvDatasetsAndDatafiles() throws Exception {
		String rootSessionId = setup.getRootSessionId();
 
        DataSelection dataSelection = new DataSelection();

        // the "h2db" investigation containing 2 small datasets
        String query1 = "select investigation.id from Investigation investigation " +
                        "where investigation.name = 'h2db'";
        List<Long> invIDs = getLongsFromQuery(query1);
        logger.info("IDs of Investigations to restore: " + invIDs);
        dataSelection.addInvestigations(invIDs);

        // all the smaller "glassfish" datasets (not lib and modules)
        String query2 = "select dataset.id from Dataset dataset " +
                        "where dataset.name != 'glassfish/lib' " +
                        "and dataset.name != 'glassfish/modules' " + 
                        "and dataset.investigation.name = 'glassfish'";
        List<Long> dsIDs = getLongsFromQuery(query2);
        logger.info("IDs of Datasets to restore: " + dsIDs);
        dataSelection.addDatasets(dsIDs);

        // the datafiles from the "mq/bin" dataset
        String query3 = "select datafile.id from Datafile datafile " +
                        "where datafile.dataset.name = 'mq/bin' " +
                        "and datafile.dataset.investigation.name = 'mq'";
        List<Long> dfIDs = getLongsFromQuery(query3);
        logger.info("IDs of Datafiles to restore: " + dfIDs);
        dataSelection.addDatafiles(dfIDs);

        long sizeFromDataSelection = testingClient.getSize(rootSessionId, dataSelection, 200);
        logger.info("Size via DataSelection: " + sizeFromDataSelection);

        String preparedId = testingClient.prepareData(rootSessionId, 
                dataSelection, Flag.ZIP, 200);
        
        long sizeFromPreparedId = testingClient.getSize(preparedId, 200);
        logger.info("Size via prepared ID: " + sizeFromPreparedId);

        assertEquals("Size returned via the two methods was different", sizeFromDataSelection, sizeFromPreparedId);

        monitorPrepareProgress(preparedId);

        // look up the datafile locations for all of the files in the 
        // prepareData request so that the resulting zip file can be checked
        List<String> locationList = new ArrayList<>();

        // all datafile locations in the 'h2db' investigation
        String query1loc = "select datafile.location from Datafile datafile " +
                           "where datafile.dataset.investigation.name = 'h2db'";
        locationList.addAll(getStringsFromQuery(query1loc));

        // all datafile locations of the files in the smaller "glassfish" datasets (not lib and modules)
        String query2loc = "select datafile.location from Datafile datafile " +
                           "where datafile.dataset.name != 'glassfish/lib' " +
                           "and datafile.dataset.name != 'glassfish/modules' " + 
                           "and datafile.dataset.investigation.name = 'glassfish'";
        locationList.addAll(getStringsFromQuery(query2loc));

        String query3loc = "select datafile.location from Datafile datafile " +
                           "where datafile.dataset.name = 'mq/bin' " +
                           "and datafile.dataset.investigation.name = 'mq'";
        locationList.addAll(getStringsFromQuery(query3loc));

        logger.info("Zip file should contain " + locationList.size() + " restored files:");
        for (String location : locationList) {
            logger.info(location);
        }
        
		checkZipFile(preparedId, locationList, Collections.emptyList());
    }

    @Test
	public void testPrepareMultipleSmallerDatasets() throws Exception {
		String rootSessionId = setup.getRootSessionId();
        // all the smaller "glassfish" datasets (not lib and modules)
        String query1 = "select dataset.id from Dataset dataset " +
                        "where dataset.name != 'glassfish/lib' " +
                        "and dataset.name != 'glassfish/modules' " + 
                        "and dataset.investigation.name = 'glassfish'";
        List<Long> dsIDs = getLongsFromQuery(query1);
        logger.info("IDs of Datasets to restore: " + dsIDs);

        DataSelection dataSelection = new DataSelection();
        dataSelection.addDatasets(dsIDs);

        long sizeFromDataSelection = testingClient.getSize(rootSessionId, dataSelection, 200);
        logger.info("Size via DataSelection: " + sizeFromDataSelection);

        String preparedId = testingClient.prepareData(rootSessionId, 
                dataSelection, Flag.ZIP, 200);
        
        long sizeFromPreparedId = testingClient.getSize(preparedId, 200);
        logger.info("Size via prepared ID: " + sizeFromPreparedId);

        assertEquals("Size returned via the two methods was different", sizeFromDataSelection, sizeFromPreparedId);

        monitorPrepareProgress(preparedId);

        // look up all datafile locations of the files in the smaller "glassfish" 
        // datasets (not lib and modules) for checking of the zip file
        String query2loc = "select datafile.location from Datafile datafile " +
                           "where datafile.dataset.name != 'glassfish/lib' " +
                           "and datafile.dataset.name != 'glassfish/modules' " + 
                           "and datafile.dataset.investigation.name = 'glassfish'";
        List<String> locationList = getStringsFromQuery(query2loc);
        logger.info("Zip file should contain " + locationList.size() + " restored files:");
        for (String location : locationList) {
            logger.info(location);
        }
        
		checkZipFile(preparedId, locationList, Collections.emptyList());
    }

    @Test
    public void testPrepareSimultaneousDatasets() throws Exception {
		String rootSessionId = setup.getRootSessionId();
        // the large glassfish/lib dataset
        String query1 = "select dataset.id from Dataset dataset " +
                        "where dataset.name = 'glassfish/lib' " +
                        "and dataset.investigation.name = 'glassfish'";
        List<Long> gfLibDsIDs = getLongsFromQuery(query1);
        logger.info("ID of glassfish/lib Dataset to restore: " + gfLibDsIDs);

        // the large glassfish/modules dataset
        String query2 = "select dataset.id from Dataset dataset " +
                        "where dataset.name = 'glassfish/modules' " +
                        "and dataset.investigation.name = 'glassfish'";
        List<Long> gfModDsIDs = getLongsFromQuery(query2);
        logger.info("ID of glassfish/modules Dataset to restore: " + gfModDsIDs);

        String gfLibPreparedId = testingClient.prepareData(rootSessionId, 
                new DataSelection().addDatasets(gfLibDsIDs), Flag.ZIP, 200);

        String gfModPreparedId = testingClient.prepareData(rootSessionId, 
                new DataSelection().addDatasets(gfModDsIDs), Flag.ZIP, 200);

        while (!testingClient.isPrepared(gfLibPreparedId, 200) || 
                !testingClient.isPrepared(gfModPreparedId, 200)) {
            String gfLibPercentageComplete = testingClient.getPercentageComplete(gfLibPreparedId, 200);
            logger.info("glassfish/lib is " + gfLibPercentageComplete + "% complete");
            String gfModPercentageComplete = testingClient.getPercentageComplete(gfModPreparedId, 200);
            logger.info("glassfish/modules is " + gfModPercentageComplete + "% complete");
            Thread.sleep(10000);
        }
        logger.info("Done - both restore requests completed");

        // look up all datafile locations of the files in the 
        // glassfish/lib dataset for checking of the zip file
        String query1loc = "select datafile.location from Datafile datafile " +
                           "where datafile.dataset.name = 'glassfish/lib' " +
                           "and datafile.dataset.investigation.name = 'glassfish'";
        List<String> gfLibLocationList = getStringsFromQuery(query1loc);
        logger.info("Zip file should contain " + gfLibLocationList.size() + " restored files:");
        for (String location : gfLibLocationList) {
            logger.info(location);
        }
		checkZipFile(gfLibPreparedId, gfLibLocationList, Collections.emptyList());

        // look up all datafile locations of the files in the 
        // glassfish/modules dataset for checking of the zip file
        String query2loc = "select datafile.location from Datafile datafile " +
                           "where datafile.dataset.name = 'glassfish/modules' " +
                           "and datafile.dataset.investigation.name = 'glassfish'";
        List<String> gfModLocationList = getStringsFromQuery(query2loc);
        logger.info("Zip file should contain " + gfModLocationList.size() + " restored files:");
        for (String location : gfModLocationList) {
            logger.info(location);
        }
		checkZipFile(gfModPreparedId, gfModLocationList, Collections.emptyList());

    }

    @Test
    public void testCancelThenResubmit() throws Exception {
		String rootSessionId = setup.getRootSessionId();
        // the mq/javadoc dataset
        String query1 = "select dataset.id from Dataset dataset " +
                        "where dataset.name = 'mq/javadoc' " +
                        "and dataset.investigation.name = 'mq'";
        List<Long> mqJavadocDsIDs = getLongsFromQuery(query1);
        logger.info("ID of mq/javadoc Dataset to restore: " + mqJavadocDsIDs);

        String preparedId = testingClient.prepareData(rootSessionId, 
                new DataSelection().addDatasets(mqJavadocDsIDs), Flag.ZIP, 200);

        logger.info("Prepared ID for mq/javadoc Dataset is " + preparedId);

        // wait 5 secs before cancelling the restore
        Thread.sleep(5000);

        testingClient.cancel(preparedId, 204);
        logger.info("Cancelled restore of mq/javadoc Dataset");
      
        // NOTE: don't call isPrepared here because that effectively 
        // re-activates the previously cancelled prepare request

        // there is no certain way via the IDS API to confirm that the prepare
        // request has been cancelled (apart from perhaps some parsing of the
        // getServiceStatus output?) but the percentage complete should now be
        // reporting "100"
        String percentageComplete = testingClient.getPercentageComplete(preparedId, 200);
        logger.info("Percentage complete for mq/javadoc Dataset is " + percentageComplete);
        assertEquals("Percentage complete was not 100 for cancelled prepared ID " + preparedId,
                "100", percentageComplete);

        // test requesting the same dataset again
        // this time some of the files should already be on main storage
        // (again there is no way to check this via the IDS API but 
        // manual monitoring of the IDS log file would confirm this)
        preparedId = testingClient.prepareData(rootSessionId, 
                new DataSelection().addDatasets(mqJavadocDsIDs), Flag.ZIP, 200);

        logger.info("New prepared ID for mq/javadoc Dataset is " + preparedId);

        monitorPrepareProgress(preparedId);

        // look up all datafile locations of the files in the 
        // mq/javadoc dataset for checking of the zip file
        String query1loc = "select datafile.location from Datafile datafile " +
                           "where datafile.dataset.name = 'mq/javadoc' " +
                           "and datafile.dataset.investigation.name = 'mq'";
        List<String> mqJavadocLocationList = getStringsFromQuery(query1loc);
        logger.info("Zip file should contain " + mqJavadocLocationList.size() + " restored files:");
        for (String location : mqJavadocLocationList) {
            logger.info(location);
        }
		checkZipFile(preparedId, mqJavadocLocationList, Collections.emptyList());

    }

    @Test
    public void testPrepareDatasetsWithMissingFiles() throws Exception {
		String rootSessionId = setup.getRootSessionId();
        // the mq/etc and mq/legal datasets each containing
        // a file that is not available from the Dummy Storage
        String query1 = "select dataset.id from Dataset dataset " +
                        "where dataset.name = 'mq/etc' " +
                        "or dataset.name = 'mq/legal' " +
                        "and dataset.investigation.name = 'mq'";
        List<Long> dsIDs = getLongsFromQuery(query1);
        logger.info("ID of mq/etc and mq/legal Datasets to restore: " + dsIDs);

        String preparedId = testingClient.prepareData(rootSessionId, 
                new DataSelection().addDatasets(dsIDs), Flag.ZIP, 200);

        logger.info("Prepared ID for mq/etc and mq/legal Datasets is " + preparedId);

        monitorPrepareProgress(preparedId);

        // look up all datafile locations of the files in the 
        // mq/etc and mq/legal datasets for checking of the zip file
        String query1loc = "select datafile.location from Datafile datafile " +
                           "where (datafile.dataset.name = 'mq/etc' " +
                           "or datafile.dataset.name = 'mq/legal') " +
                           "and datafile.name != 'non_existent_file.txt' " +
                           "and datafile.dataset.investigation.name = 'mq'";
        List<String> locationList = getStringsFromQuery(query1loc);
        locationList.add("/" + Constants.DEFAULT_MISSING_FILES_FILENAME);
        logger.info("Zip file should contain " + locationList.size() + " files:");
        for (String location : locationList) {
            logger.info(location);
        }

        // look up all datafile locations of the files in the 
        // mq/etc and mq/legal datasets of the non-existent files
        String query2loc = "select datafile.location from Datafile datafile " +
                           "where (datafile.dataset.name = 'mq/etc' " +
                           "or datafile.dataset.name = 'mq/legal') " +
                           "and datafile.name = 'non_existent_file.txt' " +
                           "and datafile.dataset.investigation.name = 'mq'";
        List<String> missingFiles = getStringsFromQuery(query2loc);
        logger.info("Missing files file should contain " + missingFiles.size() + " files:");
        for (String location : missingFiles) {
            logger.info(location);
        }

        checkZipFile(preparedId, locationList, missingFiles);

    }

    @Test
    public void testPrepareEmptyInvestigation() throws Exception {
		String rootSessionId = setup.getRootSessionId();
        // the ID of the "empty" Investigation
        String query1 = "select investigation.id from Investigation investigation " +
                        "where investigation.name = 'empty'";
        List<Long> invIDs = getLongsFromQuery(query1);
        logger.info("ID of 'empty' Investigation: " + invIDs);

        String preparedId = testingClient.prepareData(rootSessionId, 
                new DataSelection().addInvestigations(invIDs), Flag.ZIP, 200);

        logger.info("Prepared ID for 'empty' Investigation is " + preparedId);

        monitorPrepareProgress(preparedId);

        // the downloaded zip file should be empty
        checkZipFile(preparedId, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void testPrepareEmptyDataset() throws Exception {
		String rootSessionId = setup.getRootSessionId();
        // the ID of the "empty" Dataset
        String query1 = "select dataset.id from Dataset dataset " +
                        "where dataset.name = 'mq/empty' " +
                        "and dataset.investigation.name = 'mq'";
        List<Long> dsIDs = getLongsFromQuery(query1);
        logger.info("ID of empty Dataset: " + dsIDs);

        String preparedId = testingClient.prepareData(rootSessionId, 
                new DataSelection().addDatasets(dsIDs), Flag.ZIP, 200);

        logger.info("Prepared ID for 'empty' Dataset is " + preparedId);

        monitorPrepareProgress(preparedId);

        // the downloaded zip file should be empty
        checkZipFile(preparedId, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void testPrepareNonExistentFiles() throws Exception {
		String rootSessionId = setup.getRootSessionId();
        // the IDs of the "non-existent" Datafiles that are
        // not available from the Dummy Archive Storage
        String query1 = "select datafile.id from Datafile datafile " +
                        "where (datafile.dataset.name = 'mq/etc' " +
                        "or datafile.dataset.name = 'mq/legal') " +
                        "and datafile.name = 'non_existent_file.txt' " +
                        "and datafile.dataset.investigation.name = 'mq'";
        List<Long> dfIDs = getLongsFromQuery(query1);
        logger.info("ID of non-existent Datafiles: " + dfIDs);

        String preparedId = testingClient.prepareData(rootSessionId, 
                new DataSelection().addDatafiles(dfIDs), Flag.ZIP, 200);

        logger.info("Prepared ID for non-existent Datafiles is " + preparedId);

        monitorPrepareProgress(preparedId);

        List<String> locationList = new ArrayList<>();
        locationList.add("/" + Constants.DEFAULT_MISSING_FILES_FILENAME);
        logger.info("Zip file should contain " + locationList.size() + " files:");
        for (String location : locationList) {
            logger.info(location);
        }

        // look up all datafile locations of the files in the 
        // mq/etc and mq/legal datasets of the non-existent files
        String query2loc = "select datafile.location from Datafile datafile " +
                           "where (datafile.dataset.name = 'mq/etc' " +
                           "or datafile.dataset.name = 'mq/legal') " +
                           "and datafile.name = 'non_existent_file.txt' " +
                           "and datafile.dataset.investigation.name = 'mq'";
        List<String> missingFiles = getStringsFromQuery(query2loc);
        logger.info("Missing files file should contain " + missingFiles.size() + " files:");
        for (String location : missingFiles) {
            logger.info(location);
        }

        // the downloaded zip file should just contain a missing files file
        // listing the two files that could not be restored
        checkZipFile(preparedId, locationList, missingFiles);
    }

    @Test
    public void testGetDataSelectionSize() throws Exception {
		String rootSessionId = setup.getRootSessionId();
        // the size of all the datafiles in the "glassfish" Investigation
        String query1 = "select sum(datafile.fileSize) from Datafile datafile " +
                        "where datafile.dataset.investigation.name = 'glassfish'";
        long expectedSize = (Long)(icatSoapClient.search(rootSessionId, query1).get(0));
        logger.info("Expected size from sum of Datafiles in Inv is {}", expectedSize);

        // the size of all the datafiles in the h2db/bin and h2db/service Datasets
        String query2 = "select sum(datafile.fileSize) from Datafile datafile " +
                        "where (datafile.dataset.name = 'h2db/bin' " +
                        "or datafile.dataset.name = 'h2db/service') " +
                        "and datafile.dataset.investigation.name = 'h2db'";
        expectedSize += (Long)(icatSoapClient.search(rootSessionId, query2).get(0));
        logger.info("Expected size with Datasets added is {}", expectedSize);

        // the size of all the datafiles in the mq/examples Dataset
        String query3 = "select sum(datafile.fileSize) from Datafile datafile " +
                        "where datafile.dataset.name = 'mq/examples' " +
                        "and datafile.dataset.investigation.name = 'mq'";
        expectedSize += (Long)(icatSoapClient.search(rootSessionId, query3).get(0));
        logger.info("Expected size with Datafiles added is {}", expectedSize);

        // the ID of the glassfish Investigation
        String query1ids = "select investigation.id from Investigation investigation " +
                           "where investigation.name = 'glassfish'";
        List<Long> invIDs = getLongsFromQuery(query1ids);
        logger.info("ID of glassfish Investigation: " + invIDs);

        // the IDs of the h2db/bin and h2db/service Datasets
        String query2ids = "select dataset.id from Dataset dataset " +
                           "where (dataset.name = 'h2db/bin' " +
                           "or dataset.name = 'h2db/service') " +
                           "and dataset.investigation.name = 'h2db'";
        List<Long> dsIDs = getLongsFromQuery(query2ids);
        logger.info("IDs of h2db Datasets: " + dsIDs);

        // the IDs of all the datafiles in the mq/examples Dataset
        String query3ids = "select datafile.id from Datafile datafile " +
                           "where datafile.dataset.name = 'mq/examples' " +
                           "and datafile.dataset.investigation.name = 'mq'";
        List<Long> dfIDs = getLongsFromQuery(query3ids);
        logger.info("IDs of mq/examples Datafiles: " + dfIDs);

        DataSelection dataSelection = new DataSelection();
        dataSelection.addInvestigations(invIDs);
        dataSelection.addDatasets(dsIDs);
        dataSelection.addDatafiles(dfIDs);

        long sizeFromIDS = testingClient.getSize(rootSessionId, 
                dataSelection, 200);

        logger.info("Data Selection size from IDS call is {}", sizeFromIDS);
        
        assertEquals("Calculated Data Selection size and that from IDS don't match", 
                expectedSize, sizeFromIDS);
    }


    private void monitorPrepareProgress(String preparedId) throws Exception {
        int percentageCompleteInt = 0;
        while (!testingClient.isPrepared(preparedId, 200)) {
            String percentageComplete = testingClient.getPercentageComplete(preparedId, 200);
            logger.info(percentageComplete + "% complete");
            int updatedPercentageCompleteInt = Integer.parseInt(percentageComplete);
            if (updatedPercentageCompleteInt < percentageCompleteInt) {
                throw new Exception("Percentage complete reduced from " + percentageCompleteInt + " to " + updatedPercentageCompleteInt);
            } else {
                percentageCompleteInt = updatedPercentageCompleteInt;
            }
            Thread.sleep(10000);
        }
        logger.info("Done - all requested data restored");
    }

    private void checkZipFile(String preparedId, List<String> expectedFiles, List<String> missingFiles) throws Exception {
        boolean filesExpected = expectedFiles.size() > 0;
        boolean missingFilesExpected = missingFiles.size() > 0;
        try (InputStream stream = testingClient.getData(preparedId, 0, 200)) {
            ZipInputStream zis = new ZipInputStream(stream);
            ZipEntry ze = zis.getNextEntry();
            while (ze != null) {
                String name = "/" + ze.getName(); // add the preceeding forward slash not present in the zip entry
                if (!expectedFiles.remove(name)) {
                    String message = "Unexpected file " + name + " found in zip for prepared ID " + preparedId;
                    logger.error(message);
                    throw new Exception(message);
                }
                logger.info("Zip contains " + name);
                if (name.equals("/" + Constants.DEFAULT_MISSING_FILES_FILENAME)) {
                    if (missingFilesExpected) {
                        // check the contents of the missing files file in the zip
                        // note: there has to be a better way to do this - maybe in python!
                        // first read the missing files file contents 
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        byte[] bytesIn = new byte[1024];
                        int read = 0;
                        while ((read = zis.read(bytesIn)) != -1) {
                            baos.write(bytesIn, 0, read);
                        }
                        baos.close();
                        // then split the file contents into lines
                        try (Scanner s = new Scanner(new ByteArrayInputStream(baos.toByteArray()))) {
                            int lineCount = 0;  
                            while(s.hasNextLine()) {
                                lineCount++; 
                                String line = s.nextLine();
                                if (lineCount == 1) {
                                    logger.info("Skipping header line");
                                } else {
                                    logger.info("Line {}: {}", lineCount, line);
                                    String filepath = "/" + line;
                                    if (!missingFiles.remove(filepath)) {
                                        String message = "Missing file " + filepath + 
                                                " found in missing files file for prepared ID " + 
                                                preparedId + " was not expected";
                                        logger.error(message);
                                        throw new Exception(message);
                                    }
                                }
                            }
                        }
                    } else {
                        throw new Exception("A missing files file was not expected for prepared ID " + preparedId);  
                    }
                }
                ze = zis.getNextEntry();
            }
            zis.close();
            if (filesExpected) {
                if (expectedFiles.size() != 0) {
                    logger.error("The following files were not found in the zip file for prepared ID " + preparedId);
                    for (String location : expectedFiles) {
                        logger.error(location);
                    }
                    throw new Exception("Missing files in zip file for prepared ID " + preparedId);
                } else {
                    logger.info("All files found in zip for prepared ID " + preparedId);
                }
            } else {
                logger.info("No files expected in zip and none found");
            }
            if (missingFilesExpected) {
                if (missingFiles.size() != 0) {
                    logger.error("The following files were not listed in the missing files file for prepared ID " + preparedId);
                    for (String location : missingFiles) {
                        logger.error(location);
                    }
                    throw new Exception("Missing files not listed in missing files file for prepared ID " + preparedId);
                } else {
                    logger.info("All missing files found in missing files file for prepared ID " + preparedId);
                }
            }
        }
    }
    
}
