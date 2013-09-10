package org.icatproject.ids.icatclient;

import java.util.ArrayList;

import org.icatproject.Datafile;
import org.icatproject.DatafileFormat;
import org.icatproject.Dataset;
import org.icatproject.ids.icatclient.exceptions.ICATClientException;


public interface ICATClientBase {
	public abstract String getUserId(String sessionId) throws ICATClientException;
	
	public Dataset getDatasetForDatasetId(String sessionId, Long datasetId) throws ICATClientException;
	public Datafile getDatafileWithDatasetForDatafileId(String sessionId, Long datafileId) throws ICATClientException;
	public DatafileFormat findDatafileFormatById(String sessionId, String datafileFormatId) throws ICATClientException;
	public Long registerDatafile(String sessionId, Datafile datafile) throws ICATClientException;
}