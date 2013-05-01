package org.icatproject.ids.webservice.exceptions;

public class NotImplementedException extends WebServiceException
{
    private static final long serialVersionUID = 1L;
 
    public NotImplementedException(String message)
    {    
        super(WebServiceException.Response.NOT_IMPLEMENTED, message);
    }
}
