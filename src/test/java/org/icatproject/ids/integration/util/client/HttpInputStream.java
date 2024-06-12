package org.icatproject.ids.integration.util.client;

import java.io.FilterInputStream;
import java.io.IOException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;

public class HttpInputStream extends FilterInputStream {

    private CloseableHttpResponse response;
    private CloseableHttpClient httpclient;

    /**
     * Creates an input stream using the specified response.
     *
     * @param httpclient the CloseableHttpClient to close
     * @param response   the response to use and ultimately close
     */
    public HttpInputStream(CloseableHttpClient httpclient,
            CloseableHttpResponse response)
            throws IllegalStateException, IOException {
        super(response.getEntity().getContent());
        this.response = response;
        this.httpclient = httpclient;
    }

    /**
     * Ensure that response is closed as well as the underlying inputstream
     */
    @Override
    public void close() throws IOException {
        IOException exception = null;
        try {
            in.close();
        } catch (IOException e) {
            exception = e;
        }
        try {
            response.close();
        } catch (IOException e) {
            if (exception == null) {
                exception = e;
            }
        }
        try {
            httpclient.close();
        } catch (IOException e) {
            if (exception == null) {
                exception = e;
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

}
