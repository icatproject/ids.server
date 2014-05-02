package org.icatproject.ids;

import org.icatproject.ids.plugin.DfInfo;

public class DfInfoImpl implements DfInfo {

	private String createId;

	private long dfId;

	private String dfLocation;

	private String dfName;

	private long dsId;

	private String modId;

	public DfInfoImpl(long dfId, String dfName, String dfLocation, String createId, String modId,
			long dsId) {
		this.dfId = dfId;
		this.dfName = dfName;
		this.dfLocation = dfLocation;
		this.createId = createId;
		this.modId = modId;
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
	public String getCreateId() {
		return createId;
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
	public String getModId() {
		return modId;
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
