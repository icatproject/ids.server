package org.icatproject.ids.webservice.exceptions;

public class NotFoundException extends WebServiceException
{
    private static final long serialVersionUID = 1L;
 
    public NotFoundException(String message)
    {    
        super(WebServiceException.Response.NOT_FOUND, message);
    }
}

