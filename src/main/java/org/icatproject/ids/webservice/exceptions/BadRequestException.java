package org.icatproject.ids.webservice.exceptions;

public class BadRequestException extends WebServiceException
{
    private static final long serialVersionUID = 1L;
 
    public BadRequestException(String message)
    {    
        super(WebServiceException.Response.BAD_REQUEST, message);
    }
}
