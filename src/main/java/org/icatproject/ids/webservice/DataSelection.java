package org.icatproject.ids.webservice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.webservice.exceptions.BadRequestException;
import org.icatproject.ids.webservice.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.webservice.exceptions.InternalException;
import org.icatproject.ids.webservice.exceptions.NotFoundException;

public class DataSelection {

	/**
	 * Checks to see if the investigation, dataset or datafile id list is a valid comma separated
	 * list of longs. No spaces or leading 0's. Also accepts null.
	 */
	public static List<Long> getValidIds(String thing, String idList) throws BadRequestException {

		List<Long> result;
		if (idList == null) {
			result = Collections.emptyList();
		} else {
			String[] ids = idList.split("\\s*,\\s*");
			result = new ArrayList<>(ids.length);
			for (String id : ids) {
				try {
					result.add(Long.parseLong(id));
				} catch (NumberFormatException e) {
					throw new BadRequestException("The " + thing + " parameter '" + idList
							+ "' is not a valid "
							+ "string representation of a comma separated list of longs");
				}
			}
		}
		return result;
	}

	private Set<DsInfo> dsInfo;
	private ICAT icat;

	private String sessionId;

	public DataSelection(ICAT icat, String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws BadRequestException, NotFoundException,
			InsufficientPrivilegesException, InternalException {

		this.icat = icat;
		this.sessionId = sessionId;
		List<Long> dfids = getValidIds("datafileIds", datafileIds);
		List<Long> dsids = getValidIds("datasetIds", datasetIds);
		List<Long> invids = getValidIds("investigationIds", investigationIds);

		dsInfo = resolveDatasetIds(dfids, dsids, invids);

	}

	public Set<DsInfo> getDsInfo() {
		return dsInfo;
	}

	private Set<DsInfo> resolveDatasetIds(List<Long> dfids, List<Long> dsids, List<Long> invids)
			throws NotFoundException, InsufficientPrivilegesException, InternalException {
		Set<DsInfo> rids = new HashSet<>();

		try {
			for (Long dfid : dfids) {
				List<Object> dss = icat.search(sessionId,
						"SELECT ds FROM Dataset ds JOIN ds.datafiles df WHERE df.id = " + dfid
								+ " INCLUDE ds.datafiles, ds.investigation.facility");
				if (dss.size() == 1) {
					Dataset ds = (Dataset) dss.get(0);
					rids.add(new DsInfoImpl(ds, icat, sessionId));
				} else {
					icat.get(sessionId, "Datafile", dfid); // May reveal a permissions problem
					throw new NotFoundException("Datafile " + dfid);
				}
			}
			for (Long dsid : dsids) {
				Dataset ds = (Dataset) icat.get(sessionId,
						"Dataset ds INCLUDE ds.datafiles, ds.investigation.facility", dsid);
				rids.add(new DsInfoImpl(ds, icat, sessionId));
			}

			for (Long invid : invids) {
				List<Object> dss = icat.search(sessionId,
						"SELECT ds FROM Dataset ds WHERE ds.investigation.id = " + invid
								+ " INCLUDE ds.datafiles, ds.investigation.facility");
				if (dss.size() >= 1) {
					Dataset ds = (Dataset) dss.get(0);
					rids.add(new DsInfoImpl(ds, icat, sessionId));
				} else {
					icat.get(sessionId, "Investigation", invid); // May reveal a permissions problem
					throw new NotFoundException("Investigation " + invid);
				}
			}
			return rids;

		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES
					|| type == IcatExceptionType.SESSION) {
				throw new InsufficientPrivilegesException(e.getMessage());
			} else if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
				throw new NotFoundException(e.getMessage());
			} else {
				throw new InternalException(e.getClass() + " " + e.getMessage());

			}

		}
	}

}
