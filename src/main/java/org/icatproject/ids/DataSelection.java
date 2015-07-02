package org.icatproject.ids;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.plugin.DsInfo;

public class DataSelection {

	/**
	 * Checks to see if the investigation, dataset or datafile id list is a
	 * valid comma separated list of longs. No spaces or leading 0's. Also
	 * accepts null.
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
					throw new BadRequestException("The " + thing + " parameter '" + idList + "' is not a valid "
							+ "string representation of a comma separated list of longs");
				}
			}
		}
		return result;
	}

	private Map<Long, DsInfo> dsInfos;
	private ICAT icat;
	private String sessionId;
	private boolean dfWanted;
	private List<Long> dfids;
	private List<Long> dsids;
	private List<Long> invids;
	private Set<DfInfoImpl> dfInfos;
	private Set<Long> emptyDatasets;
	private boolean dsWanted;

	public enum Returns {
		DATASETS, DATASETS_AND_DATAFILES, DATAFILES
	}

	public DataSelection(ICAT icat, String sessionId, String investigationIds, String datasetIds, String datafileIds,
			Returns returns) throws BadRequestException, NotFoundException, InsufficientPrivilegesException,
			InternalException {

		this.icat = icat;
		this.sessionId = sessionId;
		dfids = getValidIds("datafileIds", datafileIds);
		dsids = getValidIds("datasetIds", datasetIds);
		invids = getValidIds("investigationIds", investigationIds);
		dfWanted = returns == Returns.DATASETS_AND_DATAFILES || returns == Returns.DATAFILES;
		dsWanted = returns == Returns.DATASETS_AND_DATAFILES || returns == Returns.DATASETS;
		resolveDatasetIds();

	}

	public Map<Long, DsInfo> getDsInfo() {
		return dsInfos;
	}

	private void resolveDatasetIds() throws NotFoundException, InsufficientPrivilegesException, InternalException,
			BadRequestException {
		dsInfos = new HashMap<>();
		emptyDatasets = new HashSet<>();
		if (dfWanted) {
			dfInfos = new HashSet<>();
		}

		try {
			for (Long dfid : dfids) {
				List<Object> dss = icat.search(sessionId,
						"SELECT ds FROM Dataset ds JOIN ds.datafiles df WHERE df.id = " + dfid
								+ " INCLUDE ds.investigation.facility");
				if (dss.size() == 1) {
					Dataset ds = (Dataset) dss.get(0);
					long dsid = ds.getId();
					dsInfos.put(dsid, new DsInfoImpl(ds));
					if (dfWanted) {
						Datafile df = (Datafile) icat.get(sessionId, "Datafile", dfid);
						String location = IdsBean.getLocation(df);
						dfInfos.add(new DfInfoImpl(df.getId(), df.getName(), location, df.getCreateId(), df.getModId(),
								dsid));
					}
				} else {
					// Next line may reveal a permissions problem
					icat.get(sessionId, "Datafile", dfid);
					throw new NotFoundException("Datafile " + dfid);
				}
			}
			for (Long dsid : dsids) {
				Dataset ds = (Dataset) icat.get(sessionId,
						"Dataset ds INCLUDE ds.datafiles, ds.investigation.facility", dsid);
				if (dfWanted) {
					for (Datafile df : ds.getDatafiles()) {
						String location = IdsBean.getLocation(df);
						dfInfos.add(new DfInfoImpl(df.getId(), df.getName(), location, df.getCreateId(), df.getModId(),
								dsid));
					}
				}
				dsInfos.put(dsid, new DsInfoImpl(ds));
				if (ds.getDatafiles().isEmpty()) {
					emptyDatasets.add(dsid);
				}
			}

			for (Long invid : invids) {
				/*
				 * This code now avoids getting all the datafiles in an
				 * investigation in one go as it will fail for huge
				 * investigations.
				 */
				List<Object> dss = icat.search(sessionId, "SELECT ds FROM Dataset ds WHERE ds.investigation.id = "
						+ invid + " INCLUDE ds.investigation.facility");

				if (dss.size() >= 1) {
					for (Object o : dss) {
						Dataset ds = (Dataset) o;
						boolean empty = true;
						long dsid = ds.getId();
						if (dfWanted) {
							ds = (Dataset) icat.get(sessionId,
									"Dataset ds INCLUDE ds.datafiles, ds.investigation.facility", dsid);
							for (Datafile df : ds.getDatafiles()) {
								String location = IdsBean.getLocation(df);
								dfInfos.add(new DfInfoImpl(df.getId(), df.getName(), location, df.getCreateId(), df
										.getModId(), dsid));
								empty = false;
							}
						}
						dsInfos.put(dsid, new DsInfoImpl(ds));
						if (empty) {
							emptyDatasets.add(dsid);
						}
					}
				}
			}

		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES || type == IcatExceptionType.SESSION) {
				throw new InsufficientPrivilegesException(e.getMessage());
			} else if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
				throw new NotFoundException(e.getMessage());
			} else {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}

		}
		/*
		 * TODO don't calculate what is not needed - however this ensures that
		 * the flag is respected
		 */
		if (!dsWanted) {
			dsInfos = null;
			emptyDatasets = null;
		}
	}

	public Set<DfInfoImpl> getDfInfo() {
		return dfInfos;
	}

	public boolean mustZip() {
		return dfids.size() > 1L || !dsids.isEmpty() || !invids.isEmpty()
				|| (dfids.isEmpty() && dsids.isEmpty() && invids.isEmpty());
	}

	public boolean isSingleDataset() {
		return dfids.isEmpty() && dsids.size() == 1 && invids.isEmpty();
	}

	public Set<Long> getEmptyDatasets() {
		return emptyDatasets;
	}

}
