package org.icatproject.ids.webservice.exceptions;

public class InsufficientStorageException extends WebServiceException
{
    private static final long serialVersionUID = 1L;
 
    public InsufficientStorageException(String message)
    {    
        super(WebServiceException.Response.INSUFFICIENT_STORAGE, message);
    }
}

