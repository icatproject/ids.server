package org.icatproject.ids.integration.util;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

/*
 * Contains an OutputStream from an IDS request accompanied with the HTTP header information.
 */
public class Response {

    ByteArrayOutputStream response = null;
    Map<String, List<String>> header = null;

    /*
     * response - OutputStream from an IDS request
     * header - The header information from an IDS request
     */
    public Response(ByteArrayOutputStream response, Map<String, List<String>> header) {
        this.response = response;
        this.header = header;
    }

    /*
     * Get OutputStream of an IDS request.
     */
    public ByteArrayOutputStream getResponse() {
        return response;
    }

    /*
     * Get header information of an IDS request.
     */
    public Map<String, List<String>> getHeader() {
        return header;
    }

    /*
     * Get the filename from the Content-Disposition field in the HTTP header.
     */
    public String getFilename() {
        String filename = null;
        List<String> contentDispositionList = header.get("Content-Disposition");
        if (contentDispositionList != null && contentDispositionList.get(0) != null
                && contentDispositionList.get(0).indexOf("=") != -1) {
            filename = contentDispositionList.get(0).split("=")[1].replace("\"", "");
        }
        return filename;
    }
}
