package org.icatproject.ids;

import org.icatproject.Dataset;
import org.icatproject.Facility;
import org.icatproject.Investigation;
import org.icatproject.ids.plugin.DsInfo;

public class DsInfoImpl implements DsInfo {

	private long dsId;

	private String dsName;

	private long facilityId;

	private String facilityName;

	private long invId;

	private String invName;

	private String visitId;

	public DsInfoImpl(Dataset ds) {
		Investigation investigation = ds.getInvestigation();
		Facility facility = investigation.getFacility();
		dsId = ds.getId();
		dsName = ds.getName();
		invId = investigation.getId();
		invName = investigation.getName();
		visitId = investigation.getVisitId();
		facilityId = facility.getId();
		facilityName = facility.getName();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		return dsId == ((DsInfoImpl) obj).getDsId();
	}

	@Override
	public long getDsId() {
		return dsId;
	}

	@Override
	public String getDsName() {
		return dsName;
	}

	@Override
	public long getFacilityId() {
		return facilityId;
	}

	@Override
	public String getFacilityName() {
		return facilityName;
	}

	@Override
	public long getInvId() {
		return invId;
	}

	@Override
	public String getInvName() {
		return invName;
	}

	@Override
	public String getVisitId() {
		return visitId;
	}

	@Override
	public int hashCode() {
		return (int) (dsId ^ (dsId >>> 32));
	}

	@Override
	public String toString() {
		return invId + "/" + dsId + " (" + facilityName + "/" + invName + "/" + visitId + "/"
				+ dsName + ")";
	}

}
