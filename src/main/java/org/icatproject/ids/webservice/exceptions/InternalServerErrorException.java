package org.icatproject.ids.webservice.exceptions;

public class InternalServerErrorException extends WebServiceException
{
    private static final long serialVersionUID = 1L;
 
    public InternalServerErrorException(String message)
    {    
        super(WebServiceException.Response.INTERNAL_SERVER_ERROR, message);
    }
}

