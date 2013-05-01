package org.icatproject.idsclient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.icatproject.idsclient.exceptions.BadRequestException;
import org.icatproject.idsclient.exceptions.IDSException;

/*
 * The test suite for the IDS makes use of the IDS client. This has some passive 
 * validation built in (ie. parameters constrained to specific types). To be able to test
 * all possible combinations and values, new methods have been created that take strings for
 * all of their parameters.
 */
public class TestingClient extends Client {

    public TestingClient(String idsUrl) throws BadRequestException {
        super(idsUrl);
    }

    /**
     * Based on the prepareData method but takes all parameters as strings. This is used for testing
     * and should not be used otherwise.
     */
    public String prepareDataTest(String sessionId, String investigationIds, String datasetIds,
            String datafileIds, String compress, String zip) throws IDSException, IOException {
        Map<String, String> parameters = new HashMap<String, String>();

        // create parameter list
        parameters.put("sessionId", sessionId);
        if (investigationIds != null)
            parameters.put("investigationIds", investigationIds);
        if (datasetIds != null)
            parameters.put("datasetIds", datasetIds);
        if (datafileIds != null)
            parameters.put("datafileIds", datafileIds);
        if (compress != null)
            parameters.put("compress", compress);
        if (zip != null)
            parameters.put("zip", zip);

        Response response = HTTPConnect("POST", "prepareData", parameters);
        return response.getResponse().toString().trim();
    }

    /**
     * Based on the archive method but takes all parameters as strings. This is used for testing and
     * should not be used otherwise.
     */
    public void archiveTest(String sessionId, String investigationIds, String datasetIds,
            String datafileIds) throws IDSException, IOException {
        Map<String, String> parameters = new HashMap<String, String>();

        // create parameter list
        parameters.put("sessionId", sessionId);
        if (investigationIds != null)
            parameters.put("investigationIds", investigationIds);
        if (datasetIds != null)
            parameters.put("datasetIds", datasetIds);
        if (datafileIds != null)
            parameters.put("datafileIds", datafileIds);

        Response response = HTTPConnect("POST", "archive", parameters);
        response.getResponse().toString().trim();
    }

    /**
     * Same as original getStatus but throws all exceptions
     */
    public Status getStatusTest(String preparedId) throws IOException, IDSException {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("preparedId", preparedId);
        Response response = HTTPConnect("GET", "getStatus", parameters);
        return Status.valueOf(response.getResponse().toString().trim());
    }

    /**
     * Same as original getDataTest but throws all exceptions
     */
    public Response getDataTest(String preparedId, String outname, Long offset) throws IOException,
            IDSException {
        Map<String, String> parameters = new HashMap<String, String>();

        // create parameter list
        parameters.put("preparedId", preparedId);
        if (outname != null)
            parameters.put("outname", outname);
        if (offset != null)
            parameters.put("offset", offset.toString());

        return HTTPConnect("GET", "getData", parameters);
    }
}