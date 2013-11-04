package org.icatproject.ids.integration.util.client;

import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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

	public Long put(String sessionId, InputStream inputStream, String name, long datasetId,
			long datafileFormatId, String description, Integer sc) throws BadRequestException,
			NotFoundException, InternalException, InsufficientPrivilegesException,
			NotImplementedException, DataNotOnlineException, InsufficientStorageException {
		return put(sessionId, inputStream, name, datasetId, datafileFormatId, description, null,
				null, null, sc);
	}

	public Long put(String sessionId, InputStream inputStream, String name, long datasetId,
			long datafileFormatId, String description, String doi, Date datafileCreateTime,
			Date datafileModTime, Integer sc) throws BadRequestException, NotFoundException,
			InternalException, InsufficientPrivilegesException, NotImplementedException,
			DataNotOnlineException, InsufficientStorageException {
		Map<String, String> parameters = new HashMap<>();

		parameters.put("sessionId", sessionId);
		parameters.put("name", name);
		parameters.put("datafileFormatId", Long.toString(datafileFormatId));
		parameters.put("datasetId", Long.toString(datasetId));
		if (description != null) {
			parameters.put("description", description);
		}
		if (doi != null) {
			parameters.put("doi", doi);
		}
		if (datafileCreateTime != null) {
			parameters.put("datafileCreateTime", Long.toString(datafileCreateTime.getTime()));
		}
		if (datafileModTime != null) {
			parameters.put("datafileModTime", Long.toString(datafileModTime.getTime()));
		}

		try {
			HttpURLConnection urlc = process("put", parameters, Method.PUT, ParmPos.URL, null,
					inputStream, sc);
			return Long.parseLong(getOutput(urlc));
		} catch (NumberFormatException e) {
			throw new InternalException("Web service call did not return a valid Long value");
		}

	}

	public HttpURLConnection process(String relativeUrl, Map<String, String> parameters,
			Method method, ParmPos parmPos, Map<String, String> headers, InputStream inputStream,
			Integer sc) throws InternalException, BadRequestException,
			InsufficientPrivilegesException, InsufficientStorageException, NotFoundException,
			NotImplementedException, DataNotOnlineException {
		HttpURLConnection urlc;
		int rc;
		try {
			URL url = new URL(idsUrl + "/" + relativeUrl);

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

			if (headers != null) {
				for (Entry<String, String> entry : headers.entrySet()) {
					urlc.setRequestProperty(entry.getKey(), entry.getValue());
				}
			}

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
			if (sc != null && sc.intValue() != urlc.getResponseCode()) {
				if (rc / 100 != 2) {
					System.out.println(urlc.getURL() + " => " + rc);
					fail(getError(urlc));
				} else
					fail("Expected " + sc.intValue() + " but was " + urlc.getResponseCode());
			}
		} catch (Exception e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		if (rc / 100 != 2) {
			String error = getError(urlc);
			String code;
			String message;
			try {
				ObjectMapper om = new ObjectMapper();
				JsonNode rootNode = om.readValue(error, JsonNode.class);
				code = rootNode.get("code").asText();
				message = rootNode.get("message").asText();
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

	private String getError(HttpURLConnection urlc) {
		InputStream stream = urlc.getErrorStream();
		ByteArrayOutputStream os = null;
		try {
			os = new ByteArrayOutputStream();
			int len;
			byte[] buffer = new byte[1024];
			while ((len = stream.read(buffer)) != -1) {
				os.write(buffer, 0, len);
			}
			return os.toString();
		} catch (IOException e) {
			return "IOException handling errorStream";
		}
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
			urlc = process("prepareData", parameters, Method.POST, ParmPos.BODY, null, null, sc);
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
			process("restore", parameters, Method.POST, ParmPos.BODY, null, null, sc);
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
			process("archive", parameters, Method.POST, ParmPos.BODY, null, null, sc);
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
			urlc = process("getStatus", parameters, Method.GET, ParmPos.URL, null, null, sc);
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
			urlc = process("getStatus", parameters, Method.GET, ParmPos.URL, null, null, sc);
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
		HttpURLConnection urlc;
		Map<String, String> headers = null;
		if (offset != 0) {
			headers = new HashMap<>();
			headers.put("Range", "bytes=" + offset + "-");
		}
		try {
			urlc = process("getData", parameters, Method.GET, ParmPos.URL, headers, null, sc);
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
		HttpURLConnection urlc;
		Map<String, String> headers = null;
		if (offset != 0) {
			headers = new HashMap<>();
			headers.put("Range", "bytes=" + offset + "-");
		}
		try {
			urlc = process("getData", parameters, Method.GET, ParmPos.URL, headers, null, sc);
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
			process("delete", parameters, Method.DELETE, ParmPos.URL, null, null, sc);
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

	public void ping(Integer sc) throws InternalException, NotFoundException {
		Map<String, String> emptyMap = Collections.emptyMap();
		HttpURLConnection urlc;
		try {
			urlc = process("ping", emptyMap, Method.GET, ParmPos.URL, null, null, sc);
		} catch (InsufficientStorageException | DataNotOnlineException | InternalException
				| BadRequestException | InsufficientPrivilegesException | NotFoundException
				| NotImplementedException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}
		String result = getOutput(urlc);
		if (!result.equals("IdsOK")) {
			throw new NotFoundException("Server gave invalid response: " + result);
		}
	}

}