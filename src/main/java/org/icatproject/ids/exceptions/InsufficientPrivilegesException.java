package org.icatproject.ids.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class InsufficientPrivilegesException extends IdsException {

    public InsufficientPrivilegesException(String message) {
        super(HttpURLConnection.HTTP_FORBIDDEN, message);
    }
}
