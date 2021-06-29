package org.icatproject.ids.integration.util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.icatproject.Datafile;
import org.icatproject.DatafileFormat;
import org.icatproject.Dataset;
import org.icatproject.DatasetType;
import org.icatproject.EntityBaseBean;
import org.icatproject.Facility;
import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;
import org.icatproject.Investigation;
import org.icatproject.InvestigationType;
import org.icatproject.ids.storage.ArchiveStorageDummy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcatSetup {

    private static final Logger logger = LoggerFactory.getLogger(IcatSetup.class);

    private static final String TEST_FACILITY_NAME = "Integration_Test_Facility";

    private ICAT icatSoapClient;
    private String rootSessionId;

    public IcatSetup(ICAT icatSoapClient, String rootSessionId) {
        this.icatSoapClient = icatSoapClient;
        this.rootSessionId = rootSessionId;
    }

	public void cleanIcat() throws Exception {
		String query = "select facility from Facility facility where facility.name = '" + TEST_FACILITY_NAME + "'";
		List<Object> objects = icatSoapClient.search(rootSessionId, query);
		if (objects.size() == 1) {
			logger.warn("Deleting existing Facility found in the ICAT");
			icatSoapClient.delete(rootSessionId, (EntityBaseBean)objects.get(0));
		} else {
			logger.warn("{} existing Facilities were found in the ICAT", objects.size());
		}
	}

	public void populateIcat() throws Exception {
        logger.info("Populating ICAT with test data");
		Facility fac = createFacility(rootSessionId, TEST_FACILITY_NAME);
		InvestigationType invType = createInvestigationType(rootSessionId, fac, "Integration Test InvestigationType");
		DatasetType dsType = createDatasetType(rootSessionId, fac, "Integration Test DatasetType");
		DatafileFormat datafileFormat = createDatafileFormat(rootSessionId, fac, "Integation Test DatafileFormat", "1.0");

		ArchiveStorageDummy archiveStorageDummy = new ArchiveStorageDummy(null);
		Map<String, Integer> pathToSizeMap = archiveStorageDummy.getPathToSizeMap();
		Set<String> filePaths = pathToSizeMap.keySet();
		Map<String, Investigation> invMap = new LinkedHashMap<>();
		Map<String, Dataset> datasetMap = new LinkedHashMap<>();
		for (String path : filePaths) {
			logger.debug("path: {}", path);
			String[] pathBits = path.split("/");
			for (int i=0; i<pathBits.length; i++) {
				logger.trace("pathBits[{}] : {}", i, pathBits[i]);
			}
			if (pathBits.length >= 6) {
				String invName = pathBits[3];
				String datasetName = pathBits[4];
				String datafileName = "";
				for (int i=5; i<pathBits.length; i++) {
					datafileName += pathBits[i];
					if (i < pathBits.length-1) {
						datafileName += "/";
					}
				}
				logger.trace("invName     : {}", invName);
				logger.trace("datasetName : {}", datasetName);
				logger.trace("datafileName: {}", datafileName);
				Investigation inv = invMap.get(invName);
				if (inv == null) {
					inv = createInvestigation(rootSessionId, fac, invType, invName, "1");
					invMap.put(invName, inv);
					logger.info("Created new investigation {} with ID: {}", invName, inv.getId());
					
				}
				String invDatasetName = invName + "/" + datasetName;
				Dataset dataset = datasetMap.get(invDatasetName);
				if (dataset == null) {
					dataset = createDataset(rootSessionId, dsType, inv, invDatasetName);
					datasetMap.put(invDatasetName, dataset);
					logger.info("Created new dataset {} with ID: {}", invDatasetName, dataset.getId());
				}
				Datafile datafile = createDatafile(rootSessionId, datafileFormat, dataset, datafileName, path, pathToSizeMap.get(path));
				logger.info("Created new datafile {} with ID: {}", path, datafile.getId());
			} else {
				logger.debug("Path is not long enough");
			}
		}

		// add an empty investigation
		String emptyInvName = "empty";
		Investigation emptyInv = createInvestigation(rootSessionId, fac, invType, emptyInvName, "1");
		invMap.put(emptyInvName, emptyInv);
		
		// add an empty dataset to the "mq" investigation
		String emptyDsName = "mq/empty";
		Dataset emptyDs = createDataset(rootSessionId, dsType, invMap.get("mq"), emptyDsName);
		datasetMap.put(emptyDsName, emptyDs);

		// add a file that doesn't exist on the storage
		// to the mq/etc and the mq/legal datasets
		createDatafile(rootSessionId, datafileFormat, datasetMap.get("mq/etc"), 
				"non_existent_file.txt", 
				"/dls/payara5/mq/etc/non_existent_file.txt", 0L);

		createDatafile(rootSessionId, datafileFormat, datasetMap.get("mq/legal"), 
				"non_existent_file.txt", 
				"/dls/payara5/mq/legal/non_existent_file.txt", 0L);

		logger.trace("invMap keys:");
		for (String invName : invMap.keySet()) {
			logger.trace(invName);
		}
		logger.trace("datasetMap keys:");
		for (String invDatasetName : datasetMap.keySet()) {
			logger.trace(invDatasetName);
		}

	}

	private Facility createFacility(String sessionId, String name) throws IcatException_Exception {
		Facility fac = new Facility();
		fac.setName(name);
		fac.setId(icatSoapClient.create(sessionId, fac));
		return fac;
	}

	private InvestigationType createInvestigationType(String sessionId, Facility fac, String name) throws IcatException_Exception {
		InvestigationType invType = new InvestigationType();
		invType.setFacility(fac);
		invType.setName(name);
		invType.setId(icatSoapClient.create(sessionId, invType));
		return invType;
	}

	private DatasetType createDatasetType(String sessionId, Facility fac, String name) throws IcatException_Exception {
		DatasetType dsType = new DatasetType();
		dsType.setFacility(fac);
		dsType.setName(name);
		dsType.setId(icatSoapClient.create(sessionId, dsType));
		return dsType;
	}

	private DatafileFormat createDatafileFormat(String sessionId, Facility fac, String name, String version) throws IcatException_Exception {
		DatafileFormat datafileFormat = new DatafileFormat();
		datafileFormat.setFacility(fac);
		datafileFormat.setName(name);
		datafileFormat.setVersion(version);
		datafileFormat.setId(icatSoapClient.create(sessionId, datafileFormat));
		return datafileFormat;
	}

	private Investigation createInvestigation(String sessionId, Facility fac, InvestigationType invType, String name, String visitId) throws IcatException_Exception {
		Investigation inv = new Investigation();
		inv.setFacility(fac);
		inv.setType(invType);
		inv.setName(name);
		inv.setTitle(name);
		inv.setVisitId(visitId);
		inv.setId(icatSoapClient.create(sessionId, inv));
		return inv;
	}

	private Dataset createDataset(String sessionId, DatasetType dsType, Investigation inv, String name) throws IcatException_Exception {
		Dataset dataset = new Dataset();
		dataset.setType(dsType);
		dataset.setInvestigation(inv);
		dataset.setName(name);
		dataset.setId(icatSoapClient.create(sessionId, dataset));
		return dataset;
	}

	private Datafile createDatafile(String sessionId, DatafileFormat datafileFormat, Dataset dataset, String name, String location, long size) throws IcatException_Exception {
		Datafile datafile = new Datafile();
		datafile.setDatafileFormat(datafileFormat);
		datafile.setDataset(dataset);
		datafile.setName(name);
		datafile.setLocation(location);
		datafile.setFileSize(size);
		datafile.setId(icatSoapClient.create(sessionId, datafile));
		return datafile;
	}
    

}
