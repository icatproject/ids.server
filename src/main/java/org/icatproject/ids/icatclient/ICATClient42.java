package org.icatproject.ids.icatclient;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;

import org.icatproject.Datafile;
import org.icatproject.DatafileFormat;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.icatclient.exceptions.ICATBadParameterException;
import org.icatproject.ids.icatclient.exceptions.ICATClientException;
import org.icatproject.ids.icatclient.exceptions.ICATInsufficientPrivilegesException;
import org.icatproject.ids.icatclient.exceptions.ICATInternalException;
import org.icatproject.ids.icatclient.exceptions.ICATNoSuchObjectException;
import org.icatproject.ids.icatclient.exceptions.ICATObjectAlreadyExistsException;
import org.icatproject.ids.icatclient.exceptions.ICATSessionException;
import org.icatproject.ids.icatclient.exceptions.ICATValidationException;
import org.icatproject.ids.util.StatusInfo;

/*
 * TODO: move out code that references DatafileEntity ie. make better
 * separation
 */
public class ICATClient42 implements ICATClientBase {

	private ICAT service;

	public ICATClient42(String url) throws MalformedURLException {
		service = new ICATService(new URL(url), new QName("http://icatproject.org", "ICATService")).getICATPort();
	}

	@Override
	public String getUserId(String sessionId) throws ICATClientException {
		String retval = null;
		try {
			retval = service.getUserName(sessionId);
		} catch (IcatException_Exception e) {
			throw convertToICATClientException(e);
		}
		return retval;
	}

	public String getICATVersion() throws ICATClientException {
		String version = null;
		try {
			version = service.getApiVersion();
		} catch (IcatException_Exception e) {
			throw convertToICATClientException(e);
		}
		return version;
	}

	// TODO: add proper logging
	private ICATClientException convertToICATClientException(IcatException_Exception e) {
		switch (e.getFaultInfo().getType()) {
		case BAD_PARAMETER:
			return new ICATBadParameterException();
		case INSUFFICIENT_PRIVILEGES:
			return new ICATInsufficientPrivilegesException();
		case INTERNAL:
			return new ICATInternalException();
		case NO_SUCH_OBJECT_FOUND:
			return new ICATNoSuchObjectException();
		case OBJECT_ALREADY_EXISTS:
			return new ICATObjectAlreadyExistsException();
		case SESSION:
			return new ICATSessionException();
		case VALIDATION:
			return new ICATValidationException();
		default:
			return new ICATClientException("Unknown ICATException_Exception, is the ICAT version supported?");
		}
	}

	@Override
	public Dataset getDatasetForDatasetId(String sessionId, Long datasetId) throws ICATClientException {
		try {
			Dataset icatDs = (Dataset) service.get(sessionId, "Dataset INCLUDE Datafile", datasetId);
			return icatDs;
		} catch (IcatException_Exception e) {
			throw convertToICATClientException(e);
		}
	}

	@Override
    public Datafile getDatafileWithDatasetForDatafileId(String sessionId, Long datafileId) throws ICATClientException {
    	try {
    		Datafile icatDf = (Datafile) service.get(sessionId, "Datafile INCLUDE Dataset", datafileId);
    		return icatDf;
    	} catch (IcatException_Exception e) {
    		throw convertToICATClientException(e);
    	}
    }
	
	@Override
	public DatafileFormat findDatafileFormatById(String sessionId, String datafileFormatId) throws ICATClientException {
		try {
			List<Object> formats = service.search(sessionId, "DatafileFormat [name = '"
					+ datafileFormatId + "']");
			if (formats.size() == 0) {
				return null;
			} else {
				return (DatafileFormat) formats.get(0);
			}
		} catch (IcatException_Exception e) {
			throw convertToICATClientException(e);
		}
	}
	
	@Override
	public Long registerDatafile(String sessionId, Datafile datafile) throws ICATClientException {
		try {
			return (Long) service.create(sessionId, datafile);
		} catch (IcatException_Exception e) {
			throw convertToICATClientException(e);
		}
	}
}
