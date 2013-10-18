package org.icatproject.ids.webservice.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class NotFoundException extends IdsException {

	public NotFoundException(Code code, String message) {
		super(HttpURLConnection.HTTP_NOT_FOUND, code, message);
	}
}
