package org.icatproject.ids.test.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.io.IOUtils;
import org.icatproject.ids.webservice.Status;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.representation.Form;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/*
 * The test suite for the IDS makes use of the IDS client. This has some passive
 * validation built in (ie. parameters constrained to specific types). To be
 * able to test all possible combinations and values, new methods have been
 * created that take strings for all of their parameters.
 */
public class TestingClient {

	private String idsUrl;

	public TestingClient(String idsUrl) throws Exception {
		if (idsUrl == null) {
			throw new Exception("You must supply a URL to the IDS.");
		} else {
			try {
				new URL(idsUrl);
			} catch (MalformedURLException e) {
				throw new Exception("Bad IDS URL '" + idsUrl + "'");
			}

			this.idsUrl = idsUrl;
		}
	}

	/*
	 * Based on the prepareData method but takes all parameters as strings. This
	 * is used for testing and should not be used otherwise.
	 */
	public String prepareDataTest(String sessionId, String investigationIds, String datasetIds, String datafileIds,
			String compress, String zip) throws Exception {
		Client client = Client.create();
		Form form = new Form();
		form.add("sessionId", sessionId);
		if (investigationIds != null)
			form.add("investigationIds", investigationIds);
		if (datasetIds != null)
			form.add("datasetIds", datasetIds);
		if (datafileIds != null)
			form.add("datafileIds", datafileIds);
		if (compress != null)
			form.add("compress", compress);
		if (zip != null)
			form.add("zip", zip);
		WebResource resource = client.resource(idsUrl).path("prepareData");
		return resource.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE).accept(MediaType.TEXT_PLAIN_TYPE)
				.post(String.class, form).trim();
	}

	/*
	 * Based on the restore method but takes all parameters as strings. This is
	 * used for testing and should not be used otherwise.
	 */
	public void restoreTest(String sessionId, String investigationIds, String datasetIds, String datafileIds)
			throws Exception {
		Client client = Client.create();
		Form form = new Form();
		form.add("sessionId", sessionId);
		if (investigationIds != null)
			form.add("investigationIds", investigationIds);
		if (datasetIds != null)
			form.add("datasetIds", datasetIds);
		if (datafileIds != null)
			form.add("datafileIds", datafileIds);
		WebResource resource = client.resource(idsUrl).path("restore");
		resource.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE).accept(MediaType.TEXT_PLAIN_TYPE).post(form);
	}

	/*
	 * Based on the archive method but takes all parameters as strings. This is
	 * used for testing and should not be used otherwise.
	 */
	public void archiveTest(String sessionId, String investigationIds, String datasetIds, String datafileIds)
			throws Exception {
		Client client = Client.create();
		Form form = new Form();
		form.add("sessionId", sessionId);
		if (investigationIds != null)
			form.add("investigationIds", investigationIds);
		if (datasetIds != null)
			form.add("datasetIds", datasetIds);
		if (datafileIds != null)
			form.add("datafileIds", datafileIds);
		WebResource resource = client.resource(idsUrl).path("archive");
		resource.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE).accept(MediaType.TEXT_PLAIN_TYPE).post(form);
	}

	/*
	 * Same as original getStatus but throws all exceptions
	 */
	public Status getStatusTest(String preparedId) throws Exception {
		Client client = Client.create();
		MultivaluedMap<String, String> params = new MultivaluedMapImpl();
		params.add("preparedId", preparedId);
		WebResource resource = client.resource(idsUrl).path("getStatus");
		return Status.valueOf(resource.queryParams(params).accept(MediaType.TEXT_PLAIN_TYPE).get(String.class).trim());
	}

	/*
	 * Same as original getDataTest but throws all exceptions
	 */
	public Response getDataTest(String preparedId, String outname, Long offset) throws Exception {
		Client client = Client.create();
		MultivaluedMap<String, String> params = new MultivaluedMapImpl();
		params.add("preparedId", preparedId);
		if (outname != null)
			params.add("outname", outname);
		if (offset != null)
			params.add("offset", offset.toString());
		WebResource resource = client.resource(idsUrl).path("getData");
		ClientResponse response = resource.queryParams(params).accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
				.get(ClientResponse.class);
		// if we use ClientResponse, the UniformInterfaceException is not thrown
		// automatically; see:
		// http://jersey.java.net/nonav/apidocs/1.8/jersey/com/sun/jersey/api/client/WebResource.Builder.html#get(java.lang.Class)
		if (response.getStatus() != 200) {
			throw new UniformInterfaceException(response);
		}
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		IOUtils.copy(response.getEntityInputStream(), os);
		return new Response(os, response.getHeaders());
	}

	public String putTest(String sessionId, String name, String datafileFormatId, String datasetId, String description,
			String doi, String datafileCreateTime, String datafileModTime, File file) throws Exception {
		Client client = Client.create();
		MultivaluedMap<String, String> params = new MultivaluedMapImpl();
		params.add("sessionId", sessionId);
		params.add("name", name);
		params.add("datafileFormatId", datafileFormatId);
		params.add("datasetId", datasetId);
		if (description != null)
			params.add("description", description);
		if (doi != null)
			params.add("doi", doi);
		if (datafileCreateTime != null)
			params.add("datafileCreateTime", datafileCreateTime);
		if (datafileModTime != null)
			params.add("datafileModTime", datafileModTime);
		WebResource resource = client.resource(idsUrl).path("put");
		InputStream in = new FileInputStream(file);
		return resource.queryParams(params).type(MediaType.APPLICATION_OCTET_STREAM_TYPE)
				.accept(MediaType.TEXT_PLAIN_TYPE).put(String.class, in);
	}

	// private TestingClientException
	// mapJerseyClientException(UniformInterfaceException e) {
	// String msg = e.getResponse().getEntity(String.class);
	// switch (e.getResponse().getStatus()) {
	// case 400:
	// return new TestingClientBadRequestException(msg);
	// case 403:
	// return new TestingClientForbiddenException(msg);
	// case 404:
	// return new TestingClientNotFoundException(msg);
	// case 500:
	// return new TestingClientInternalServerErrorException(msg);
	// case 501:
	// return new TestingClientNotImplementedException(msg);
	// case 507:
	// return new TestingClientInsufficientStorageException(msg);
	// default:
	// return new TestingClientException("unknown exception, shouldn't appear");
	// }
	// }
}