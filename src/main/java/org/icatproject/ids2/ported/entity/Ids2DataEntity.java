package org.icatproject.ids2.ported.entity;

import java.util.List;

import org.icatproject.Dataset;
import org.icatproject.ids.util.StatusInfo;

public interface Ids2DataEntity {
	public boolean overlapsWith(Ids2DataEntity e);
	public List<Dataset> getDatasets();
	public StatusInfo getStatus();
	public void setStatus(StatusInfo status);
	public RequestEntity getRequest();
}
