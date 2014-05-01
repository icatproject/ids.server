package org.icatproject.ids;

import org.icatproject.ids.plugin.DfInfo;

public class DfInfoImpl implements DfInfo {

	private String creator;

	private long dfId;

	private String dfLocation;

	private String dfName;

	private long dsId;
	public DfInfoImpl(long dfId, String dfName, String dfLocation, String creator, long dsId) {
		this.dfId = dfId;
		this.dfName = dfName;
		this.dfLocation = dfLocation;
		this.creator = creator;
		this.dsId = dsId;
	}
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		return dfId == ((DfInfoImpl) obj).getDfId();
	}
	@Override
	public String getCreator() {
		return creator;
	}
	@Override
	public long getDfId() {
		return dfId;
	}

	@Override
	public String getDfLocation() {
		return dfLocation;
	}

	@Override
	public String getDfName() {
		return dfName;
	}

	public long getDsId() {
		return dsId;
	}

	@Override
	public int hashCode() {
		return (int) (dfId ^ (dfId >>> 32));
	}

	@Override
	public String toString() {
		return dfLocation;
	}

}
