package org.icatproject.idsclient.exception;

@SuppressWarnings("serial")
public class TestingClientBadRequestException extends TestingClientException
{
    public TestingClientBadRequestException(String message)
    {    
        super(message);
    }
}
