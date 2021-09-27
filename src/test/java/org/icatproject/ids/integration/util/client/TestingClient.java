package org.icatproject.ids.integration.util.client;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class TestingClient {

	public enum Flag {
		COMPRESS, NONE, ZIP, ZIP_AND_COMPRESS
	}

	private String basePath;
	private URI idsUri;

	public TestingClient(URL idsUrl) {

		try {
			idsUri = new URI(idsUrl.getProtocol(), null, idsUrl.getHost(), idsUrl.getPort(), null, null, null);
			basePath = idsUrl.getPath();
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}

	}

	private void checkStatus(HttpResponse response, Integer sc)
			throws InternalException, BadRequestException, DataNotOnlineException, ParseException, IOException,
			InsufficientPrivilegesException, NotImplementedException, InsufficientStorageException, NotFoundException {
		StatusLine status = response.getStatusLine();
		if (status == null) {
			throw new InternalException("Status line returned is empty");
		}
		int rc = status.getStatusCode();
		if (sc != null && sc.intValue() != rc) {
			if (rc / 100 != 2) {
				HttpEntity entity = response.getEntity();
				if (entity == null) {
					fail("No explanation provided");
				} else {
					fail("rc was " + rc + " " + EntityUtils.toString(entity));
				}
			} else
				fail("Expected " + sc.intValue() + " but was " + rc);
		}
		if (rc / 100 != 2) {
			HttpEntity entity = response.getEntity();
			String error;
			if (entity == null) {
				throw new InternalException("No explanation provided");
			} else {
				error = EntityUtils.toString(entity);
			}
			String code;
			String message;
			try (JsonReader jsonReader = Json.createReader(new StringReader(error))) {
				JsonObject json = jsonReader.readObject();
				code = json.getString("code");
				message = json.getString("message");
			} catch (JsonException e) {
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

	}

	private void expectNothing(CloseableHttpResponse response, Integer sc) throws InternalException,
			BadRequestException, DataNotOnlineException, ParseException, InsufficientPrivilegesException,
			NotImplementedException, InsufficientStorageException, NotFoundException, IOException {
		checkStatus(response, sc);
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			if (!EntityUtils.toString(entity).isEmpty()) {
				throw new InternalException("No http entity expected in response");
			}
		}
	}

	public String getApiVersion(int sc) throws InternalException, ParseException, NotImplementedException {
		URI uri = getUri(getUriBuilder("getApiVersion"));
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return getString(response, sc);
			} catch (IOException | InsufficientStorageException | DataNotOnlineException | BadRequestException
					| InsufficientPrivilegesException | NotFoundException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	public String getPercentageComplete(String preparedId, int sc) throws InternalException {
		URIBuilder uriBuilder = getUriBuilder("getPercentageComplete");
		uriBuilder.setParameter("preparedId", preparedId);
		URI uri = getUri(uriBuilder);
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return getString(response, sc);
			} catch (Exception e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	public void cancel(String preparedId, int sc) throws InternalException {
		URIBuilder uriBuilder = getUriBuilder("cancel");
		uriBuilder.setParameter("preparedId", preparedId);
		URI uri = getUri(uriBuilder);
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				expectNothing(response, sc);
			} catch (Exception e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	public URL getIcatUrl(int sc) throws InternalException, ParseException, NotImplementedException {
		URI uri = getUri(getUriBuilder("getIcatUrl"));
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return new URL(getString(response, sc));
			} catch (IOException | InsufficientStorageException | DataNotOnlineException | BadRequestException
					| InsufficientPrivilegesException | NotFoundException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	public InputStream getData(String preparedId, long offset, Integer sc)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, NotFoundException,
			InternalException, DataNotOnlineException {

		URIBuilder uriBuilder = getUriBuilder("getData");
		uriBuilder.setParameter("preparedId", preparedId);
		URI uri = getUri(uriBuilder);

		CloseableHttpResponse response = null;
		CloseableHttpClient httpclient = null;
		HttpGet httpGet = new HttpGet(uri);
		if (offset != 0) {
			httpGet.setHeader("Range", "bytes=" + offset + "-");
		}
		boolean closeNeeded = true;
		try {
			httpclient = HttpClients.createDefault();
			response = httpclient.execute(httpGet);
			checkStatus(response, sc);
			closeNeeded = false;
			return new HttpInputStream(httpclient, response);
		} catch (IOException | InsufficientStorageException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		} finally {
			if (closeNeeded && httpclient != null) {
				try {
					if (response != null) {
						try {
							response.close();
						} catch (Exception e) {
							// Ignore it
						}
					}
					httpclient.close();
				} catch (IOException e) {
					// Ignore it
				}
			}
		}
	}

	public long getSize(String sessionId, DataSelection data, int sc) throws BadRequestException, NotFoundException,
			InsufficientPrivilegesException, InternalException, NotImplementedException {

		URIBuilder uriBuilder = getUriBuilder("getSize");
		uriBuilder.setParameter("sessionId", sessionId);
		for (Entry<String, String> entry : data.getParameters().entrySet()) {
			uriBuilder.setParameter(entry.getKey(), entry.getValue());
		}
		URI uri = getUri(uriBuilder);

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return Long.parseLong(getString(response, sc));
			} catch (IOException | InsufficientStorageException | DataNotOnlineException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	public long getSize(String preparedId, int sc) throws BadRequestException, NotFoundException,
			InsufficientPrivilegesException, InternalException, NotImplementedException {

		URIBuilder uriBuilder = getUriBuilder("getSize");
		uriBuilder.setParameter("preparedId", preparedId);
		URI uri = getUri(uriBuilder);

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return Long.parseLong(getString(response, sc));
			} catch (IOException | InsufficientStorageException | DataNotOnlineException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	public boolean isPrepared(String preparedId, Integer sc)
			throws BadRequestException, NotFoundException, InternalException, NotImplementedException {

		URIBuilder uriBuilder = getUriBuilder("isPrepared");
		uriBuilder.setParameter("preparedId", preparedId);
		URI uri = getUri(uriBuilder);

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);

			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return Boolean.parseBoolean(getString(response, sc));
			} catch (InsufficientStorageException | DataNotOnlineException | InsufficientPrivilegesException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	public boolean isReadOnly(int sc) throws InternalException, NotImplementedException {
		URI uri = getUri(getUriBuilder("isReadOnly"));
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return Boolean.parseBoolean(getString(response, sc));
			} catch (IOException | InsufficientStorageException | DataNotOnlineException | BadRequestException
					| InsufficientPrivilegesException | NotFoundException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	public boolean isTwoLevel(int sc) throws InternalException, NotImplementedException {
		URI uri = getUri(getUriBuilder("isTwoLevel"));
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return Boolean.parseBoolean(getString(response, sc));
			} catch (IOException | InsufficientStorageException | DataNotOnlineException | BadRequestException
					| InsufficientPrivilegesException | NotFoundException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	public void ping(Integer sc) throws InternalException, NotFoundException, NotImplementedException {

		URI uri = getUri(getUriBuilder("ping"));
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				String result = getString(response, sc);
				if (!result.equals("IdsOK")) {
					throw new NotFoundException("Server gave invalid response: " + result);
				}
			} catch (IOException | InsufficientStorageException | DataNotOnlineException | BadRequestException
					| InsufficientPrivilegesException | NotFoundException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	public String prepareData(String sessionId, DataSelection data, Flag flags, Integer sc)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, NotFoundException,
			InternalException {

		URI uri = getUri(getUriBuilder("prepareData"));
		List<NameValuePair> formparams = new ArrayList<>();
		formparams.add(new BasicNameValuePair("sessionId", sessionId));
		for (Entry<String, String> entry : data.getParameters().entrySet()) {
			formparams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
		}
		if (flags == Flag.ZIP || flags == Flag.ZIP_AND_COMPRESS) {
			formparams.add(new BasicNameValuePair("zip", "true"));
		}
		if (flags == Flag.COMPRESS || flags == Flag.ZIP_AND_COMPRESS) {
			formparams.add(new BasicNameValuePair("compress", "true"));
		}
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpEntity entity = new UrlEncodedFormEntity(formparams);
			HttpPost httpPost = new HttpPost(uri);
			httpPost.setEntity(entity);
			try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
				return getString(response, sc);
			} catch (InsufficientStorageException | DataNotOnlineException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

	}

	public List<Long> getDatafileIds(String preparedId, Integer sc)
			throws InternalException, BadRequestException, NotFoundException {

		URIBuilder uriBuilder = getUriBuilder("getDatafileIds");
		uriBuilder.setParameter("preparedId", preparedId);
		URI uri = getUri(uriBuilder);

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);

			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				String result = getString(response, sc);
				try (JsonReader jsonReader = Json.createReader(new StringReader(result))) {

					JsonObject rootNode = jsonReader.readObject();
					List<Long> ids = new ArrayList<>();
					for (JsonValue num : rootNode.getJsonArray("ids")) {
						Long id = ((JsonNumber) num).longValueExact();
						ids.add(id);
					}
					return ids;
				} catch (JsonException e) {
					throw new InternalException(
							"TestingClient " + e.getClass() + " " + e.getMessage() + " from " + result);
				}

			} catch (InsufficientStorageException | DataNotOnlineException | InsufficientPrivilegesException
					| NotImplementedException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	public void reset(String preparedId, Integer sc) throws InternalException, BadRequestException, ParseException,
			InsufficientPrivilegesException, NotImplementedException, NotFoundException {
		URI uri = getUri(getUriBuilder("reset"));
		List<NameValuePair> formparams = new ArrayList<>();
		formparams.add(new BasicNameValuePair("preparedId", preparedId));

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpEntity entity = new UrlEncodedFormEntity(formparams);
			HttpPost httpPost = new HttpPost(uri);
			httpPost.setEntity(entity);
			try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
				expectNothing(response, sc);
			} catch (InsufficientStorageException | DataNotOnlineException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	private String getString(CloseableHttpResponse response, Integer sc) throws InternalException, BadRequestException,
			DataNotOnlineException, ParseException, InsufficientPrivilegesException, NotImplementedException,
			InsufficientStorageException, NotFoundException, IOException {
		checkStatus(response, sc);
		HttpEntity entity = response.getEntity();
		if (entity == null) {
			throw new InternalException("No http entity returned in response");
		}
		return EntityUtils.toString(entity);
	}

	private URI getUri(URIBuilder uriBuilder) throws InternalException {
		try {
			return uriBuilder.build();
		} catch (URISyntaxException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	private URIBuilder getUriBuilder(String path) {
		return new URIBuilder(idsUri).setPath(basePath + "/" + path);
	}

}