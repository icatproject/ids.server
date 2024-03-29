package org.icatproject.ids.exceptions;

/**
 * This is the parent class for all the response code exceptions the web service
 * can throw. Each exception is logged with its level determined by the value of
 * it's response code.
 */
@SuppressWarnings("serial")
public class IdsException extends Exception {
    private int httpStatusCode;
    private String message;

    public IdsException(int httpStatusCode, String message) {
        this.httpStatusCode = httpStatusCode;
        this.message = message;
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
