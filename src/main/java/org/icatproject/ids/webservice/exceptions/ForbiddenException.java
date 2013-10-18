package org.icatproject.ids.webservice.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class ForbiddenException extends IdsException {

	public ForbiddenException(Code code, String message) {
		super(HttpURLConnection.HTTP_FORBIDDEN, code, message);
	}
}
