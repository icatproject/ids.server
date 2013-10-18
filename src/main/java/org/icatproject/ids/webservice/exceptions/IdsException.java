package org.icatproject.ids.webservice.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the parent class for all the response code exceptions the web service can throw. Each
 * exception is logged with its level determined by the value of it's response code.
 */
@SuppressWarnings("serial")
public class IdsException extends Exception {
	private final static Logger logger = LoggerFactory.getLogger(IdsException.class);

	public enum Code {
		BAD_SESSION_ID, INTERNAL_SERVER_ERROR, NOT_IMPLEMENTED, BAD_DATASET_IDS, BAD_DATAFILE_IDS,

		PARAMETER_MISSING, BAD_OUTNAME, BAD_PREPARED_ID, NEGATIVE, DENIED, NOT_IN_ICAT, NO_PREPARED_ID, CHECK_THIS_CODE
	}

	private int httpStatusCode;
	private String message;
	private Code code;

	@Deprecated
	public IdsException(int httpStatusCode, String message) {
		this.httpStatusCode = httpStatusCode;
		this.code = Code.INTERNAL_SERVER_ERROR;
		this.message = message;
		logger.debug(message);
	}

	public IdsException(int httpStatusCode, Code code, String message) {
		this.httpStatusCode = httpStatusCode;
		this.code = code;
		this.message = message;
		logger.debug(message);
	}

	public String getShortMessage() {
		return message;
	}

	public Code getCode() {
		return code;
	}

	public int getHttpStatusCode() {
		return httpStatusCode;
	}

	public String getMessage() {
		return "(" + httpStatusCode + ") " + code + ": " + message;
	}

}