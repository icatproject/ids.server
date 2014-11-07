package org.icatproject.ids;

import org.icatproject.Dataset;
import org.icatproject.Facility;
import org.icatproject.Investigation;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.plugin.DsInfo;

public class DsInfoImpl implements DsInfo {

	private long dsId;

	private String dsName;

	private long facilityId;

	private String facilityName;

	private long invId;

	private String invName;

	private String visitId;

	private String dsLocation;

	public DsInfoImpl(Dataset ds) throws InsufficientPrivilegesException {
		Investigation investigation = ds.getInvestigation();
		if (investigation == null) {
			throw new InsufficientPrivilegesException(
					"Probably not able to read Investigation for dataset id " + ds.getId());
		}
		Facility facility = investigation.getFacility();
		if (facility == null) {
			throw new InsufficientPrivilegesException(
					"Probably not able to read Facility for investigation id "
							+ investigation.getId());
		}
		dsId = ds.getId();
		dsName = ds.getName();
		dsLocation = ds.getLocation();
		invId = investigation.getId();
		invName = investigation.getName();
		visitId = investigation.getVisitId();
		facilityId = facility.getId();
		facilityName = facility.getName();
	}

	public DsInfoImpl(long dsId, String dsName, String dsLocation, long invId, String invName,
			String visitId, long facilityId, String facilityName) {
		this.dsId = dsId;
		this.dsName = dsName;
		this.dsLocation = dsLocation;
		this.invId = invId;
		this.invName = invName;
		this.visitId = visitId;
		this.facilityId = facilityId;
		this.facilityName = facilityName;
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
	public Long getDsId() {
		return dsId;
	}

	@Override
	public String getDsName() {
		return dsName;
	}

	@Override
	public Long getFacilityId() {
		return facilityId;
	}

	@Override
	public String getFacilityName() {
		return facilityName;
	}

	@Override
	public Long getInvId() {
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

	@Override
	public String getDsLocation() {
		return dsLocation;
	}

}
