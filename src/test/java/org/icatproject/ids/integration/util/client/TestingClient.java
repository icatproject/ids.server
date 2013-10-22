package org.icatproject.ids.integration.util.client;

import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.icatproject.ids.integration.util.client.TestingClient.Status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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

	// public String put(String sessionId, String name, String datafileFormatId, String datasetId,
	// String description, String doi, String datafileCreateTime, String datafileModTime,
	// File file) throws Exception {
	// Client client = Client.create();
	// MultivaluedMap<String, String> params = new MultivaluedMapI// public String
	// prepareData(String sessionId, String investigationIds, String datasetIds,
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
	// }mpl();
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

	public HttpURLConnection process(String relativeUrl, Map<String, String> parameters,
			Method method, ParmPos parmPos, InputStream inputStream, Integer sc)
			throws InternalException, BadRequestException, InsufficientPrivilegesException,
			InsufficientStorageException, NotFoundException, NotImplementedException,
			DataNotOnlineException {
		HttpURLConnection urlc;
		int rc;
		try {
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

			urlc = (HttpURLConnection) url.openConnection();
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

			rc = urlc.getResponseCode();
			if (sc != null) {
				assertEquals(sc.intValue(), urlc.getResponseCode());
			}
		} catch (Exception e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		if (rc / 100 != 2) {
			String error = null;
			String code;
			String message;
			try {
				InputStream stream = urlc.getErrorStream();
				ByteArrayOutputStream os = null;
				try {
					os = new ByteArrayOutputStream();
					int len;
					byte[] buffer = new byte[1024];
					while ((len = stream.read(buffer)) != -1) {
						os.write(buffer, 0, len);
					}
					error = os.toString();
				} finally {
					if (stream != null) {
						stream.close();
					}
				}
				ObjectMapper om = new ObjectMapper();
				JsonNode rootNode = om.readValue(error, JsonNode.class);
				code = rootNode.get("code").asText();
				message = rootNode.get("message").asText();
				System.out.println("*** Error " + rc + " " + code + " " + message);
			} catch (Exception e) {
				throw new InternalException("TestingClient " + error);
			}

			if (code.equals("BadRequestException")) {
				throw new BadRequestException(message);
			}

			if (code.equals("DataNotOnlineException")) {
				throw new DataNotOnlineException(message);
			}

			if (code.equals("InsufficientPrivilegesException")) {
				throw new InsufficientPrivilegesException(message);
			}

			if (code.equals("InsufficientStorageException")) {
				throw new InsufficientStorageException(message);
			}

			if (code.equals("InternalException")) {
				throw new InternalException(message);
			}

			if (code.equals("NotFoundException")) {
				throw new NotFoundException(message);
			}

			if (code.equals("NotImplementedException")) {
				throw new NotImplementedException(message);
			}
		}
		return urlc;
	}

	public String prepareData(String sessionId, DataSelection data, Flag flags, Integer sc)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			NotFoundException, InternalException {
		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(data.getParameters());
		if (flags == Flag.ZIP || flags == Flag.ZIP_AND_COMPRESS) {
			parameters.put("zip", "true");
		}
		if (flags == Flag.COMPRESS || flags == Flag.ZIP_AND_COMPRESS) {
			parameters.put("compress", "true");
		}
		HttpURLConnection urlc;
		try {
			urlc = process("prepareData", parameters, Method.POST, ParmPos.BODY, null, sc);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}
		return getOutput(urlc);
	}

	public void restore(String sessionId, DataSelection data, Integer sc)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			InternalException, NotFoundException {

		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(data.getParameters());

		try {
			process("restore", parameters, Method.POST, ParmPos.BODY, null, sc);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}

	}

	public void archive(String sessionId, DataSelection data, Integer sc)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			InternalException, NotFoundException {

		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(data.getParameters());

		try {
			process("archive", parameters, Method.POST, ParmPos.BODY, null, sc);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}
	}

	public enum Status {
		ONLINE, INCOMPLETE, RESTORING, ARCHIVED
	}

	public enum Flag {
		NONE, ZIP, COMPRESS, ZIP_AND_COMPRESS
	}

	public Status getStatus(String preparedId, Integer sc) throws NotImplementedException,
			BadRequestException, InsufficientPrivilegesException, NotFoundException,
			InternalException {
		Map<String, String> parameters = new HashMap<>();
		parameters.put("preparedId", preparedId);

		HttpURLConnection urlc;
		try {
			urlc = process("getStatus", parameters, Method.GET, ParmPos.URL, null, sc);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}

		return Status.valueOf(getOutput(urlc));
	}

	public Status getStatus(String sessonId, DataSelection data, Integer sc)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			NotFoundException, InternalException {
		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessonId);
		parameters.putAll(data.getParameters());

		HttpURLConnection urlc;

		try {
			urlc = process("getStatus", parameters, Method.GET, ParmPos.URL, null, sc);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}

		return Status.valueOf(getOutput(urlc));
	}

	public InputStream getData(String sessionId, DataSelection data, Flag flags, String outname,
			long offset, Integer sc) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, NotFoundException, InternalException,
			DataNotOnlineException {
		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(data.getParameters());
		if (flags == Flag.ZIP || flags == Flag.ZIP_AND_COMPRESS) {
			parameters.put("zip", "true");
		}
		if (flags == Flag.COMPRESS || flags == Flag.ZIP_AND_COMPRESS) {
			parameters.put("compress", "true");
		}
		if (outname != null) {
			parameters.put("outname", outname);
		}
		if (offset != 0) {
			parameters.put("offset", Long.toString(offset));
		}
		HttpURLConnection urlc;
		try {
			urlc = process("getData", parameters, Method.GET, ParmPos.URL, null, sc);
		} catch (InsufficientStorageException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}

		return getStream(urlc);

	}

	public InputStream getData(String preparedId, String outname, long offset, Integer sc)

	throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			NotFoundException, InternalException, DataNotOnlineException {
		Map<String, String> parameters = new HashMap<>();
		parameters.put("preparedId", preparedId);
		if (outname != null) {
			parameters.put("outname", outname);
		}
		if (offset != 0) {
			parameters.put("offset", Long.toString(offset));
		}
		HttpURLConnection urlc;
		try {
			urlc = process("getData", parameters, Method.GET, ParmPos.URL, null, sc);
		} catch (InsufficientStorageException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}

		return getStream(urlc);
	}

	public void delete(String sessionId, DataSelection data, Integer sc)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			InternalException, NotFoundException {

		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(data.getParameters());

		try {
			process("delete", parameters, Method.DELETE, ParmPos.URL, null, sc);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}

	}

	private InputStream getStream(HttpURLConnection urlc) throws InternalException {
		try {
			return urlc.getInputStream();
		} catch (IOException e) {
			throw new InternalException("IOException " + e.getMessage());
		}
	}

	public static void print(HttpURLConnection response) throws IOException {
		System.out.println(response);
		System.out.println(response.getContentType());
		System.out.println(response.getResponseMessage());
		System.out.println(response.getHeaderFields());

	}

	public static String getOutput(HttpURLConnection urlc) throws InternalException {
		try {
			InputStream stream = urlc.getInputStream();
			ByteArrayOutputStream os = null;
			try {
				os = new ByteArrayOutputStream();
				int len;
				byte[] buffer = new byte[1024];
				while ((len = stream.read(buffer)) != -1) {
					os.write(buffer, 0, len);
				}
				return os.toString().trim();
			} finally {
				if (stream != null) {
					stream.close();
				}
			}
		} catch (IOException e) {
			throw new InternalException("IOException " + e.getMessage());
		}
	}



}