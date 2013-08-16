package org.icatproject.ids2.ported.entity;

import java.util.List;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.util.StatusInfo;

public interface Ids2DataEntity {
	public boolean overlapsWith(Ids2DataEntity e);

	/**
	 * Returns the ICAT Dataset this DataEntity is contained in OR the ICAT
	 * Datasets this DataEntity contains (e.g. for Investigations)
	 */
	public List<Dataset> getIcatDatasets();

	/**
	 * Returns all the ICAT Datafiles this DataEntity contains.
	 */
	public List<Datafile> getIcatDatafiles();

	public StatusInfo getStatus();

	public void setStatus(StatusInfo status);

	public RequestEntity getRequest();
}
