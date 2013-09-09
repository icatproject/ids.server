package org.icatproject.idsclient;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.icatproject.ids.webservice.Status;
import org.icatproject.idsclient.exception.TestingClientBadRequestException;
import org.icatproject.idsclient.exception.TestingClientForbiddenException;
import org.icatproject.idsclient.exception.TestingClientInsufficientStorageException;
import org.icatproject.idsclient.exception.TestingClientInternalServerErrorException;
import org.icatproject.idsclient.exception.TestingClientNotFoundException;
import org.icatproject.idsclient.exception.TestingClientNotImplementedException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.representation.Form;

/**
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

	/**
	 * Based on the prepareData method but takes all parameters as strings. This
	 * is used for testing and should not be used otherwise.
	 */
	public String prepareDataTest(String sessionId, String investigationIds, String datasetIds, String datafileIds,
			String compress, String zip) throws Exception {
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

		Response response = HTTPConnect("POST", "prepareData", parameters, null);
		return response.getResponse().toString().trim();
	}

	/**
	 * Based on the restore method but takes all parameters as strings. This is
	 * used for testing and should not be used otherwise.
	 */
	public void restoreTest(String sessionId, String investigationIds, String datasetIds, String datafileIds)
			throws Exception {
		 Map<String, String> parameters = new HashMap<String, String>();
		
		 // create parameter list
		 parameters.put("sessionId", sessionId);
		 if (investigationIds != null)
		 parameters.put("investigationIds", investigationIds);
		 if (datasetIds != null)
		 parameters.put("datasetIds", datasetIds);
		 if (datafileIds != null)
		 parameters.put("datafileIds", datafileIds);
		
		 Response response = HTTPConnect("POST", "restore", parameters, null);
		 response.getResponse().toString().trim();

//		Client client = Client.create();
//
//		Form form = new Form();
//		form.add("sessionId", sessionId);
//		if (investigationIds != null)
//			form.add("investigationIds", investigationIds);
//		if (datasetIds != null)
//			form.add("datasetIds", datasetIds);
//		if (datafileIds != null)
//			form.add("datafileIds", datafileIds);
//		WebResource resource = client.resource(idsUrl).path("restore");
//
//		resource.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE).accept(MediaType.TEXT_PLAIN_TYPE).post(form);
	}

	/**
	 * Based on the archive method but takes all parameters as strings. This is
	 * used for testing and should not be used otherwise.
	 */
	public void archiveTest(String sessionId, String investigationIds, String datasetIds, String datafileIds)
			throws Exception {
		Map<String, String> parameters = new HashMap<String, String>();

		// create parameter list
		parameters.put("sessionId", sessionId);
		if (investigationIds != null)
			parameters.put("investigationIds", investigationIds);
		if (datasetIds != null)
			parameters.put("datasetIds", datasetIds);
		if (datafileIds != null)
			parameters.put("datafileIds", datafileIds);

		Response response = HTTPConnect("POST", "archive", parameters, null);
		response.getResponse().toString().trim();
	}

	/**
	 * Same as original getStatus but throws all exceptions
	 */
	public Status getStatusTest(String preparedId) throws Exception {
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("preparedId", preparedId);
		Response response = HTTPConnect("GET", "getStatus", parameters, null);
		return Status.valueOf(response.getResponse().toString().trim());
	}

	/**
	 * Same as original getDataTest but throws all exceptions
	 */
	public Response getDataTest(String preparedId, String outname, Long offset) throws Exception {
		Map<String, String> parameters = new HashMap<String, String>();

		// create parameter list
		parameters.put("preparedId", preparedId);
		if (outname != null)
			parameters.put("outname", outname);
		if (offset != null)
			parameters.put("offset", offset.toString());

		return HTTPConnect("GET", "getData", parameters, null);
	}

	public Response putTest(String sessionId, String name, String datafileFormatId, String datasetId, String location,
			String description, String doi, String datafileCreateTime, String datafileModTime, File file)
			throws Exception {
		Map<String, String> parameters = new HashMap<String, String>();

		parameters.put("sessionId", sessionId);
		parameters.put("name", name);
		parameters.put("datafileFormatId", datafileFormatId);
		parameters.put("datasetId", datasetId);
		if (location != null)
			parameters.put("location", location);
		if (description != null)
			parameters.put("description", description);
		if (doi != null)
			parameters.put("doi", doi);
		if (datafileCreateTime != null)
			parameters.put("datafileCreateTime", datafileCreateTime);
		if (datafileModTime != null)
			parameters.put("datafileModTime", datafileModTime);

		return HTTPConnect("PUT", "put", parameters, file);
	}

	/*
	 * Create HTTP request of type defined by method to 'page' defined by
	 * relativeURL. Converts parameter list into format suitable for either URL
	 * (GET, DELETE, PUT) or message body (POST).
	 */
	protected Response HTTPConnect(String method, String relativeUrl, Map<String, String> parameters, File file)
			throws Exception {
		StringBuilder url = new StringBuilder();
		HttpURLConnection connection;

		// construct url
		url.append(idsUrl);

		// check if idsURL ends with a /
		if (idsUrl.toString().charAt(idsUrl.toString().length() - 1) != '/') {
			url.append("/");
		}
		url.append(relativeUrl);

		// add parameters to url for GET and DELETE requests
		if ("GET".equals(method) || "DELETE".equals(method) || "PUT".equals(method)) {
			url.append("?");
			url.append(parametersToString(parameters));
		}
		System.out.println("prepared url = " + url.toString());
		// setup connection
		connection = (HttpURLConnection) new URL(url.toString()).openConnection();
		connection.setDoOutput(true);
		connection.setDoInput(true);
		connection.setUseCaches(false);
		connection.setAllowUserInteraction(false);
		connection.setRequestMethod(method);

		// add parameters to message body for POST requests
		if ("POST".equals(method)) {
			String messageBody = parametersToString(parameters);
			OutputStream os = null;
			try {
				os = connection.getOutputStream();
				os.write(messageBody.getBytes());
			} finally {
				if (os != null) {
					os.close();
				}
			}
		}

		if ("PUT".equals(method)) {
			writeFileToBody(file, connection);
		}

		// read in response
		InputStream is = null;
		ByteArrayOutputStream os = null;
		try {
			os = new ByteArrayOutputStream();
			if (connection.getResponseCode() != 200) {
				is = connection.getErrorStream();
			} else {
				is = connection.getInputStream();
			}

			int len;
			byte[] buffer = new byte[1024];
			while ((len = is.read(buffer)) != -1) {
				os.write(buffer, 0, len);
			}
		} finally {
			if (is != null) {
				is.close();
			}
			if (os != null) {
				os.close();
			}
		}

		// convert response code into relevant IDSException
//		System.err.println("SC = " + connection.getResponseCode());
		switch (connection.getResponseCode()) {
		case 200:
			break;
		case 400:
			throw new TestingClientBadRequestException(os.toString());
		case 403:
			throw new TestingClientForbiddenException(os.toString());
		case 404:
			throw new TestingClientNotFoundException(os.toString());
		case 500:
			throw new TestingClientInternalServerErrorException(os.toString());
		case 501:
			throw new TestingClientNotImplementedException(os.toString());
		case 507:
			throw new TestingClientInsufficientStorageException(os.toString());
		default:
			throw new TestingClientInternalServerErrorException("Unknown response " + connection.getResponseCode()
					+ ": " + os.toString());
		}

		connection.disconnect();

		return new Response(os, connection.getHeaderFields());
	}

	/*
	 * Turn a list of key-value pairs into format suitable for HTTP GET request
	 * ie. key=value&key=value
	 */
	private String parametersToString(Map<String, String> parameters) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder();
		Iterator<Map.Entry<String, String>> it = parameters.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pair = (Map.Entry<String, String>) it.next();
			sb.append(pair.getKey() + "=" + URLEncoder.encode(pair.getValue(), "UTF-8"));
			if (it.hasNext()) {
				sb.append("&");
			}
			it.remove();
		}
		return sb.toString();
	}

	private void writeFileToBody(File file, HttpURLConnection urlc) throws IOException {
		OutputStream out = null;
		InputStream in = null;
		int n = 0;
		final byte[] bytes = new byte[1024];
		out = urlc.getOutputStream();
		in = new FileInputStream(file);
		while ((n = in.read(bytes)) != -1) {
			out.write(bytes, 0, n);
		}
		in.close();
		out.close();
		// this.processResponseCode(urlc);
		// urlc.disconnect();
	}
}