package org.icatproject.ids.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class NotImplementedException extends IdsException {

	public NotImplementedException(String message) {
		super(HttpURLConnection.HTTP_NOT_IMPLEMENTED, message);
	}

}
