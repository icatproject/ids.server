package org.icatproject.ids.webservice.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class InternalServerErrorException extends IdsException {

	public InternalServerErrorException(Code code, String message) {
		super(HttpURLConnection.HTTP_INTERNAL_ERROR, code, message);
	}
}
