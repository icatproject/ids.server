package org.icatproject.ids.webservice.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class NotImplementedException extends IdsException {

	public NotImplementedException(Code code, String message) {
		super(HttpURLConnection.HTTP_NOT_IMPLEMENTED, code, message);

	}

}
