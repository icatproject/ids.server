package org.icatproject.ids.webservice.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class BadRequestException extends IdsException {

	public BadRequestException(Code code, String message) {
		super(HttpURLConnection.HTTP_BAD_REQUEST, code, message);
	}
}
