package org.icatproject.ids.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the parent class for all the response code exceptions the web service can throw. Each
 * exception is logged with its level determined by the value of it's response code.
 */
@SuppressWarnings("serial")
public class IdsException extends Exception {
	private final static Logger logger = LoggerFactory.getLogger(IdsException.class);

	private int httpStatusCode;
	private String message;

	public IdsException(int httpStatusCode, String message) {
		this.httpStatusCode = httpStatusCode;

		this.message = message;
		logger.debug(httpStatusCode + ": " + message);
	}

	public String getShortMessage() {
		return message;
	}

	public int getHttpStatusCode() {
		return httpStatusCode;
	}

	public String getMessage() {
		return "(" + httpStatusCode + ") : " + message;
	}

}