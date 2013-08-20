package org.icatproject.ids.webservice.exceptions;

import org.slf4j.LoggerFactory;

/**
 * This is the parent class for all the response code exceptions the web service can throw. Each
 * exception is logged with its level determined by the value of it's response code.
 */
public class WebServiceException extends RuntimeException
{
    private static final long serialVersionUID = 1L;
    
    public enum Response
    {
        BAD_REQUEST           (400, "Bad Request"),
        FORBIDDEN             (403, "Forbidden"),
        NOT_FOUND             (404, "Not Found"),
        INTERNAL_SERVER_ERROR (500, "Internal Server Error"),
        NOT_IMPLEMENTED       (501, "Not Implemented"),
        INSUFFICIENT_STORAGE  (507, "Insufficient Storage");

        private final int code;
        private final String reason;

        Response(final int responseCode, final String reasonPhrase) {
            this.code = responseCode;
            this.reason = reasonPhrase;
        }

        public int getResponseCode() {
            return code;
        }

        public String getReasonPhrase() {
            return reason;
        }
    }
    
    private WebServiceException.Response response;
    private String message;
    
    public WebServiceException(WebServiceException.Response response, String message)
    {
        this.response = response;
        this.message = message;
        
        if (response.getResponseCode() == 500 || response.getResponseCode() == 507) {
        	LoggerFactory.getLogger(WebServiceExceptionMapper.class).error(getMessage());
        } else {
        	LoggerFactory.getLogger(WebServiceExceptionMapper.class).info(getMessage());
        }
    }
    
    public String getMessage()
    {
        return response.getReasonPhrase() + ": " + message;
    }
    
    public int getResponse()
    {
        return response.getResponseCode();
    }
}