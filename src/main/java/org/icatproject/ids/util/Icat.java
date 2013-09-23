package org.icatproject.ids.util;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;

import org.icatproject.Datafile;
import org.icatproject.DatafileFormat;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.IcatException;
import org.icatproject.IcatException_Exception;

@Stateless
public class Icat {

	private ICAT service;

	@PostConstruct
	public void init() {
		service = PropertyHandler.getInstance().getIcatService();
	}

	public String getUserName(String sessionId) throws IcatException_Exception {
		return service.getUserName(sessionId);
	}

	public Dataset getDatasetWithDatafilesForDatasetId(String sessionId, Long datasetId) throws IcatException_Exception {
		return (Dataset) service.get(sessionId, "Dataset INCLUDE Datafile", datasetId);
	}

	public Datafile getDatafileWithDatasetForDatafileId(String sessionId, Long datafileId)
			throws IcatException_Exception {
		return (Datafile) service.get(sessionId, "Datafile INCLUDE Dataset", datafileId);
	}

	public DatafileFormat findDatafileFormatById(String sessionId, Long datafileFormatId)
			throws IcatException_Exception {
		return (DatafileFormat) service.get(sessionId, "DatafileFormat", datafileFormatId);
	}

	public Long registerDatafile(String sessionId, Datafile datafile) throws IcatException_Exception {
		return (Long) service.create(sessionId, datafile);
	}
}
