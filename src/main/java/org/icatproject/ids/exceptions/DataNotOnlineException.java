package org.icatproject.ids.exceptions;

import java.net.HttpURLConnection;

@SuppressWarnings("serial")
public class DataNotOnlineException extends IdsException {

    public DataNotOnlineException(String msg) {
        super(HttpURLConnection.HTTP_UNAVAILABLE, msg);
    }
}
