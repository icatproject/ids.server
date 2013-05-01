package org.icatproject.ids.icatclient.exceptions;

@SuppressWarnings("serial")
public class ICATClientException extends Exception
{
    public ICATClientException() {}

    public ICATClientException(String msg) {
        super(msg);
    }
}