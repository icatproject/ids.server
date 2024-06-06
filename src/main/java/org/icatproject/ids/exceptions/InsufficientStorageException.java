package org.icatproject.ids.exceptions;

import java.net.HttpURLConnection;

public class InsufficientStorageException extends IdsException {

    private static final long serialVersionUID = 1L;

    public InsufficientStorageException(String message) {
        super(HttpURLConnection.HTTP_INTERNAL_ERROR, message);
    }
}
