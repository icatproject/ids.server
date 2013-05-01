package org.icatproject.ids.webservice.exceptions;

public class ForbiddenException extends WebServiceException
{
    private static final long serialVersionUID = 1L;
 
    public ForbiddenException(String message)
    {    
        super(WebServiceException.Response.FORBIDDEN, message);
    }
}

