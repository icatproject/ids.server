package org.icatproject.ids.integration.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Map.Entry;

public class TestingClient {

	private URL idsUrl;

	private final int BUFSIZ = 2048;

	public enum Method {
		POST, PUT, DELETE, GET
	};

	public enum ParmPos {
		BODY, URL
	};

	public TestingClient(URL idsUrl) {
		this.idsUrl = idsUrl;
	}

	public HttpURLConnection process(String relativeUrl, Map<String, String> parameters,
			Method method, ParmPos parmPos, InputStream inputStream) throws IOException {
		URL url;
		url = new URL(idsUrl, relativeUrl);

		String parms = null;

		if (!parameters.isEmpty()) {

			StringBuilder sb = new StringBuilder();
			for (Entry<String, String> e : parameters.entrySet()) {
				if (sb.length() != 0) {
					sb.append("&");
				}
				sb.append(e.getKey() + "=" + URLEncoder.encode(e.getValue(), "UTF-8"));
			}
			parms = sb.toString();
		}

		if (parmPos == ParmPos.URL && parms != null) {
			url = new URL(url + "?" + parms);
		}

		HttpURLConnection urlc = (HttpURLConnection) url.openConnection();
		if (!parameters.isEmpty()) {
			urlc.setDoOutput(true);
		}

		urlc.setUseCaches(false);
		urlc.setRequestMethod(method.name());

		if (parmPos == ParmPos.BODY && parms != null) {

			OutputStream os = null;
			try {
				os = urlc.getOutputStream();
				os.write(parms.getBytes());
			} finally {
				if (os != null) {
					os.close();
				}
			}
		}

		if (inputStream != null) {
			OutputStream os = null;
			try {
				os = urlc.getOutputStream();
				os.write(parms.getBytes());
			} finally {
				if (os != null) {
					os.close();
				}
			}

			BufferedOutputStream bos = null;
			BufferedInputStream bis = null;
			try {
				int bytesRead = 0;
				byte[] buffer = new byte[BUFSIZ];
				bis = new BufferedInputStream(inputStream);
				bos = new BufferedOutputStream(urlc.getOutputStream());

				// write bytes to output stream
				while ((bytesRead = bis.read(buffer)) > 0) {
					bos.write(buffer, 0, bytesRead);
				}
			} finally {
				if (bis != null) {
					bis.close();
				}
				if (bos != null) {
					bos.close();
				}
			}
		}

		int rc = urlc.getResponseCode();
		System.out.println(rc);
		if (rc / 100 != 2) {
			InputStream error = urlc.getErrorStream();
			ByteArrayOutputStream os = null;
			try {
				os = new ByteArrayOutputStream();
				int len;
				byte[] buffer = new byte[1024];
				while ((len = error.read(buffer)) != -1) {
					os.write(buffer, 0, len);
				}
			} finally {
				error.close();
			}
			System.out.println("*** Error " + rc + " " + os.toString());
		}
		return urlc;
	}

	// public String prepareData(String sessionId, String investigationIds, String datasetIds,
	// String datafileIds, String compress, String zip) throws Exception {
	// Client client = Client.create();
	// Form form = new Form();
	// form.add("sessionId", sessionId);
	// if (investigationIds != null)
	// form.add("investigationIds", investigationIds);
	// if (datasetIds != null)
	// form.add("datasetIds", datasetIds);
	// if (datafileIds != null)
	// form.add("datafileIds", datafileIds);
	// if (compress != null)
	// form.add("compress", compress);
	// if (zip != null)
	// form.add("zip", zip);
	// WebResource resource = client.resource(idsUrl).path("prepareData");
	// return resource.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
	// .accept(MediaType.TEXT_PLAIN_TYPE).post(String.class, form).trim();
	// }
	//
	// public void restore(String sessionId, String investigationIds, String datasetIds,
	// String datafileIds) throws Exception {
	// Client client = Client.create();
	// Form form = new Form();
	// form.add("sessionId", sessionId);
	// if (investigationIds != null)
	// form.add("investigationIds", investigationIds);
	// if (datasetIds != null)
	// form.add("datasetIds", datasetIds);
	// if (datafileIds != null)
	// form.add("datafileIds", datafileIds);
	// WebResource resource = client.resource(idsUrl).path("restore");
	// resource.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE).accept(MediaType.TEXT_PLAIN_TYPE)
	// .post(form);
	// }
	//
	// public void archive(String sessionId, String investigationIds, String datasetIds,
	// String datafileIds) throws Exception {
	// Client client = Client.create();
	// Form form = new Form();
	// form.add("sessionId", sessionId);
	// if (investigationIds != null)
	// form.add("investigationIds", investigationIds);
	// if (datasetIds != null)
	// form.add("datasetIds", datasetIds);
	// if (datafileIds != null)
	// form.add("datafileIds", datafileIds);
	// WebResource resource = client.resource(idsUrl).path("archive");
	// resource.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE).accept(MediaType.TEXT_PLAIN_TYPE)
	// .post(form);
	// }
	//
	// public Status getStatus(String preparedId) throws Exception {
	// Client client = Client.create();
	// MultivaluedMap<String, String> params = new MultivaluedMapImpl();
	// params.add("preparedId", preparedId);
	// WebResource resource = client.resource(idsUrl).path("getStatus");
	// return Status.valueOf(resource.queryParams(params).accept(MediaType.TEXT_PLAIN_TYPE)
	// .get(String.class).trim());
	// }
	//
	// public Status getStatus(String sessionId, String investigationIds, String datasetIds,
	// String datafileIds) throws Exception {
	// Client client = Client.create();
	// MultivaluedMap<String, String> params = new MultivaluedMapImpl();
	// params.add("sessionId", sessionId);
	// if (investigationIds != null)
	// params.add("investigationIds", investigationIds);
	// if (datasetIds != null)
	// params.add("datasetIds", datasetIds);
	// if (datafileIds != null)
	// params.add("datafileIds", datafileIds);
	// WebResource resource = client.resource(idsUrl).path("getStatus");
	// return Status.valueOf(resource.queryParams(params).accept(MediaType.TEXT_PLAIN_TYPE)
	// .get(String.class).trim());
	// }
	//
	// public Response getData(String preparedId, String outname, Long offset) throws Exception {
	// Client client = Client.create();
	// MultivaluedMap<String, String> params = new MultivaluedMapImpl();
	// params.add("preparedId", preparedId);
	// if (outname != null)
	// params.add("outname", outname);
	// if (offset != null)
	// params.add("offset", offset.toString());
	// WebResource resource = client.resource(idsUrl).path("getData");
	// ClientResponse response = resource.queryParams(params)
	// .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE).get(ClientResponse.class);
	// // if we use ClientResponse, the UniformInterfaceException is not thrown
	// // automatically; see:
	// //
	// http: //
	// jersey.java.net/nonav/apidocs/1.8/jersey/com/sun/jersey/api/client/WebResource.Builder.html#get(java.lang.Class)
	// if (response.getStatus() >= 300) {
	// throw new UniformInterfaceException(response);
	// }
	// ByteArrayOutputStream os = new ByteArrayOutputStream();
	// WebService.copy(response.getEntityInputStream(), os);
	// return new Response(os, response.getHeaders());
	// }
	//
	// public Response getData(String sessionId, String investigationIds, String datasetIds,
	// String datafileIds, String compress, String zip, String outname, String offset)
	// throws Exception {
	// Client client = Client.create();
	// MultivaluedMap<String, String> params = new MultivaluedMapImpl();
	// params.add("sessionId", sessionId);
	// if (investigationIds != null)
	// params.add("investigationIds", investigationIds);
	// if (datasetIds != null)
	// params.add("datasetIds", datasetIds);
	// if (datafileIds != null)
	// params.add("datafileIds", datafileIds);
	// if (compress != null)
	// params.add("compress", compress);
	// if (zip != null)
	// params.add("zip", zip);
	// if (outname != null)
	// params.add("outname", outname);
	// if (offset != null)
	// params.add("offset", offset);
	//
	// WebResource resource = client.resource(idsUrl).path("getData");
	// ClientResponse response = resource.queryParams(params)
	// .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE).get(ClientResponse.class);
	// if (response.getStatus() >= 300) {
	// throw new UniformInterfaceException(response);
	// }
	// ByteArrayOutputStream os = new ByteArrayOutputStream();
	// WebService.copy(response.getEntityInputStream(), os);
	// return new Response(os, response.getHeaders());
	// }
	//
	// public String put(String sessionId, String name, String datafileFormatId, String datasetId,
	// String description, String doi, String datafileCreateTime, String datafileModTime,
	// File file) throws Exception {
	// Client client = Client.create();
	// MultivaluedMap<String, String> params = new MultivaluedMapImpl();
	// params.add("sessionId", sessionId);
	// params.add("name", name);
	// params.add("datafileFormatId", datafileFormatId);
	// params.add("datasetId", datasetId);
	// if (description != null)
	// params.add("description", description);
	// if (doi != null)
	// params.add("doi", doi);
	// if (datafileCreateTime != null)
	// params.add("datafileCreateTime", datafileCreateTime);
	// if (datafileModTime != null)
	// params.add("datafileModTime", datafileModTime);
	// WebResource resource = client.resource(idsUrl).path("put");
	// InputStream in = new FileInputStream(file);
	// return resource.queryParams(params).type(MediaType.APPLICATION_OCTET_STREAM_TYPE)
	// .accept(MediaType.TEXT_PLAIN_TYPE).put(String.class, in);
	// }
	//
	// public void delete(String sessionId, String investigationIds, String datasetIds,
	// String datafileIds) {
	// Client client = Client.create();
	// MultivaluedMap<String, String> params = new MultivaluedMapImpl();
	// params.add("sessionId", sessionId);
	// if (investigationIds != null)
	// params.add("investigationIds", investigationIds);
	// if (datasetIds != null)
	// params.add("datasetIds", datasetIds);
	// if (datafileIds != null)
	// params.add("datafileIds", datafileIds);
	// WebResource resource = client.resource(idsUrl).path("delete");
	// resource.queryParams(params).delete();
	// }

	public static void print(HttpURLConnection response) throws IOException {
		System.out.println(response);
		System.out.println(response.getContentType());
		System.out.println(response.getResponseMessage());
		System.out.println(response.getHeaderFields());

		InputStream input = null;
		try {
			input = response.getInputStream();
		} catch (IOException e) {
			// Ignore
		}
		if (input != null) {
			System.out.println("Input: " + getResult(input));
		}
	}

	public static ByteArrayOutputStream getResult(InputStream stream) throws IOException {
		ByteArrayOutputStream os = null;
		try {
			os = new ByteArrayOutputStream();
			int len;
			byte[] buffer = new byte[1024];
			while ((len = stream.read(buffer)) != -1) {
				os.write(buffer, 0, len);
			}
			return os;
		} finally {
			if (stream != null) {
				stream.close();
			}
			if (os != null) {
				os.close();
			}
		}

	}

}