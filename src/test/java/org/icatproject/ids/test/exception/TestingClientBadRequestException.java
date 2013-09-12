package org.icatproject.ids.test.exception;

@SuppressWarnings("serial")
public class TestingClientBadRequestException extends TestingClientException
{
    public TestingClientBadRequestException(String message)
    {    
        super(message);
    }
}
