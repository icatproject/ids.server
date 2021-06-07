package org.icatproject.ids.integration;

import java.util.ArrayList;
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
import org.icatproject.IcatException_Exception;
import org.icatproject.Investigation;
import org.icatproject.InvestigationType;
import org.icatproject.ids.ICATGetter;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.storage.ArchiveStorageDummy;

public class BaseIntegrationTest {

    protected static org.icatproject.ICAT icatSoapClient;
	protected static org.icatproject.icat.client.ICAT icatRestClient;
	protected static Setup setup = null;

	public static void icatsetup() throws Exception {
		String icatUrl = setup.getIcatUrl().toString();
		icatSoapClient = ICATGetter.getService(icatUrl);
		icatRestClient = new org.icatproject.icat.client.ICAT(icatUrl);
	}

	public static void cleanIcat() throws Exception {
		String rootSessionId = setup.getRootSessionId();
		List<Object> objects = icatSoapClient.search(rootSessionId, "Facility");
		List<EntityBaseBean> facilities = new ArrayList<>();
		for (Object o : objects) {
			facilities.add((Facility) o);
		}
		icatSoapClient.deleteMany(rootSessionId, facilities);
	}

	public static void populateIcat() throws Exception {
		String rootSessionId = setup.getRootSessionId();

		Facility fac = createFacility(rootSessionId, "Integration_Test_Facility");
		InvestigationType invType = createInvestigationType(rootSessionId, fac, "Integration Test InvestigationType");
		DatasetType dsType = createDatasetType(rootSessionId, fac, "Integration Test DatasetType");
		DatafileFormat datafileFormat = createDatafileFormat(rootSessionId, fac, "Integation Test DatafileFormat", "1.0");

		ArchiveStorageDummy archiveStorageDummy = new ArchiveStorageDummy(null);
		Map<String, Integer> pathToSizeMap = archiveStorageDummy.getPathToSizeMap();
		Set<String> filePaths = pathToSizeMap.keySet();
		Map<String, Investigation> invMap = new LinkedHashMap<>();
		Map<String, Dataset> datasetMap = new LinkedHashMap<>();
		int count = 0;
		for (String path : filePaths) {
			System.out.println(path);
			String[] pathBits = path.split("/");
			for (int i=0; i<pathBits.length; i++) {
				System.out.print(i + " : " + pathBits[i] + ", ");
			}
			System.out.println();
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
				System.out.println("invName     : " + invName);
				System.out.println("datasetName : " + datasetName);
				System.out.println("datafileName: " + datafileName);
				Investigation inv = invMap.get(invName);
				if (inv == null) {
					inv = createInvestigation(rootSessionId, fac, invType, invName, "1");
					invMap.put(invName, inv);
					System.out.println("Created new investigation with ID: " + inv.getId());
					
				}
				String invDatasetName = invName + "/" + datasetName;
				Dataset dataset = datasetMap.get(invDatasetName);
				if (dataset == null) {
					dataset = createDataset(rootSessionId, dsType, inv, invDatasetName);
					datasetMap.put(invDatasetName, dataset);
					System.out.println("Created new dataset with ID: " + dataset.getId());
				}
				Datafile datafile = createDatafile(rootSessionId, datafileFormat, dataset, datafileName, path, pathToSizeMap.get(path));
				System.out.println("Created new datafile with ID: " + datafile.getId());
			} else {
				System.out.println("Path is not long enough");
			}
			count++;
			// if (count == 10) {
			// 	break;
			// }
		}
		System.out.println("invMap:");
		for (String invName : invMap.keySet()) {
			System.out.println(invName);
		}
		System.out.println("datasetMap:");
		for (String invDatasetName : datasetMap.keySet()) {
			System.out.println(invDatasetName);
		}

	}

	static Facility createFacility(String sessionId, String name) throws IcatException_Exception {
		Facility fac = new Facility();
		fac.setName(name);
		fac.setId(icatSoapClient.create(sessionId, fac));
		return fac;
	}

	static InvestigationType createInvestigationType(String sessionId, Facility fac, String name) throws IcatException_Exception {
		InvestigationType invType = new InvestigationType();
		invType.setFacility(fac);
		invType.setName(name);
		invType.setId(icatSoapClient.create(sessionId, invType));
		return invType;
	}

	static DatasetType createDatasetType(String sessionId, Facility fac, String name) throws IcatException_Exception {
		DatasetType dsType = new DatasetType();
		dsType.setFacility(fac);
		dsType.setName(name);
		dsType.setId(icatSoapClient.create(sessionId, dsType));
		return dsType;
	}

	static DatafileFormat createDatafileFormat(String sessionId, Facility fac, String name, String version) throws IcatException_Exception {
		DatafileFormat datafileFormat = new DatafileFormat();
		datafileFormat.setFacility(fac);
		datafileFormat.setName(name);
		datafileFormat.setVersion(version);
		datafileFormat.setId(icatSoapClient.create(sessionId, datafileFormat));
		return datafileFormat;
	}

	static Investigation createInvestigation(String sessionId, Facility fac, InvestigationType invType, String name, String visitId) throws IcatException_Exception {
		Investigation inv = new Investigation();
		inv.setFacility(fac);
		inv.setType(invType);
		inv.setName(name);
		inv.setTitle(name);
		inv.setVisitId(visitId);
		inv.setId(icatSoapClient.create(sessionId, inv));
		return inv;
	}

	static Dataset createDataset(String sessionId, DatasetType dsType, Investigation inv, String name) throws IcatException_Exception {
		Dataset dataset = new Dataset();
		dataset.setType(dsType);
		dataset.setInvestigation(inv);
		dataset.setName(name);
		dataset.setId(icatSoapClient.create(sessionId, dataset));
		return dataset;
	}

	static Datafile createDatafile(String sessionId, DatafileFormat datafileFormat, Dataset dataset, String name, String location, long size) throws IcatException_Exception {
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
