package org.icatproject.ids.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class NotFoundException extends IdsException {

	public NotFoundException(String message) {
		super(HttpURLConnection.HTTP_NOT_FOUND, message);
	}
}
