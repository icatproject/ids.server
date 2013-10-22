package org.icatproject.ids.webservice.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class DataNotOnlineException extends IdsException {

	public DataNotOnlineException(String msg) {
		super(HttpURLConnection.HTTP_NOT_FOUND, msg);
	}

}
