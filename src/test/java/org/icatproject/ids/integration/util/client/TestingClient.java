package org.icatproject.ids.integration.util.client;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import jakarta.json.Json;
import jakarta.json.JsonException;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.icatproject.ids.enums.RequestIdNames;

import static org.junit.Assert.fail;

public class TestingClient {

    public enum Flag {
        COMPRESS, NONE, ZIP, ZIP_AND_COMPRESS
    }

    public enum Method {
        DELETE, GET, POST, PUT
    }

    public enum ParmPos {
        BODY, URL
    }

    public class ServiceStatus {

        private int lockCount;
        private Set<Long> lockedDs = new HashSet<>();
        private Map<String, String> opItems = new HashMap<>();
        private Set<Long> failures = new HashSet<>();

        int getLockCount() {
            return lockCount;
        }

        Set<Long> getLockedDs() {
            return lockedDs;
        }

        public Map<String, String> getOpItems() {
            return opItems;
        }

        void setLockedCount(int lockCount) {
            this.lockCount = lockCount;
        }

        void storeLockedDs(Long dsId) {
            lockedDs.add(dsId);
        }

        void storeOpItems(String dsInfo, String request) {
            opItems.put(dsInfo, request);
        }

        public Set<Long> getFailures() {
            return failures;
        }

        void storeFailure(Long id) {
            failures.add(id);
        }

        @Override
        public String toString() {
            String result = "lockCount: " + this.lockCount + ", ";
            result += "lockedIDs: " + lockedDs.size() + "\n";
            for(long lockedId : lockedDs) result += "\t" + lockedId + "\n";
            result += "opItems: " + opItems.size() + "\n";
            for(String key : opItems.keySet()) result += "\t" + key + ": " + opItems.get(key) + "\n";
            result += "failures: " + failures.size() + "\n";
            for(long fail : failures) result += " " + fail;
            result += "\n";

            return result;
        }

    }

    public enum Status {
        ARCHIVED, ONLINE, RESTORING
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

    public void archive(String sessionId, DataSelection data, Integer sc) throws NotImplementedException,
            BadRequestException, InsufficientPrivilegesException, InternalException, NotFoundException {

        URI uri = getUri(getUriBuilder("archive"));
        List<NameValuePair> formparams = new ArrayList<>();
        formparams.add(new BasicNameValuePair(RequestIdNames.sessionId, sessionId));
        for (Entry<String, String> entry : data.getParameters().entrySet()) {
            formparams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(uri);
            httpPost.setEntity(new UrlEncodedFormEntity(formparams));
            try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
                expectNothing(response, sc);
            } catch (InsufficientStorageException | DataNotOnlineException e) {
                throw new InternalException(e.getClass() + " " + e.getMessage());
            }
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
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

    public void delete(String sessionId, DataSelection data, Integer sc)
            throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, InternalException,
            NotFoundException, DataNotOnlineException {

        URIBuilder uriBuilder = getUriBuilder("delete");
        uriBuilder.addParameter(RequestIdNames.sessionId, sessionId);
        for (Entry<String, String> entry : data.getParameters().entrySet()) {
            uriBuilder.addParameter(entry.getKey(), entry.getValue());
        }
        URI uri = getUri(uriBuilder);

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpDelete httpDelete = new HttpDelete(uri);
            try (CloseableHttpResponse response = httpclient.execute(httpDelete)) {
                expectNothing(response, sc);
            } catch (InsufficientStorageException e) {
                throw new InternalException(e.getClass() + " " + e.getMessage());
            }
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
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

    public URL getIcatUrl(int sc) throws InternalException, ParseException, NotImplementedException {
        URI uri = getUri(getUriBuilder("getIcatUrl"));
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(uri);
            try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
                checkResponseConformity(response);
                return new URL(getString(response, sc));
            } catch (IOException | InsufficientStorageException | DataNotOnlineException | BadRequestException
                     | InsufficientPrivilegesException | NotFoundException e) {
                throw new InternalException(e.getClass() + " " + e.getMessage());
            }
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }
    }

    public InputStream getData(String sessionId, DataSelection data, Flag flags, long offset, Integer sc)
            throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, NotFoundException,
            InternalException, DataNotOnlineException {

        URIBuilder uriBuilder = getUriBuilder("getData");
        uriBuilder.setParameter(RequestIdNames.sessionId, sessionId);
        for (Entry<String, String> entry : data.getParameters().entrySet()) {
            uriBuilder.setParameter(entry.getKey(), entry.getValue());
        }

        if (flags == Flag.ZIP || flags == Flag.ZIP_AND_COMPRESS) {
            uriBuilder.setParameter("zip", "true");
        }
        if (flags == Flag.COMPRESS || flags == Flag.ZIP_AND_COMPRESS) {
            uriBuilder.setParameter("compress", "true");
        }

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
            checkResponseConformity(response);
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

    public InputStream getData(String preparedId, long offset, Integer sc)
            throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, NotFoundException,
            InternalException, DataNotOnlineException {

        URIBuilder uriBuilder = getUriBuilder("getData");
        uriBuilder.setParameter(RequestIdNames.preparedId, preparedId);
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
            checkResponseConformity(response);
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

    public ServiceStatus getServiceStatus(String sessionId, Integer sc)
            throws InternalException, InsufficientPrivilegesException, NotImplementedException {

        URIBuilder uriBuilder = getUriBuilder("getServiceStatus");
        uriBuilder.setParameter(RequestIdNames.sessionId, sessionId);
        URI uri = getUri(uriBuilder);

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(uri);

            try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
                checkResponseConformity(response);
                String result = getString(response, sc);
                try (JsonReader jsonReader = Json.createReader(new StringReader(result))) {
                    ServiceStatus serviceStatus = new ServiceStatus();
                    JsonObject rootNode = jsonReader.readObject();
                    for (JsonValue on : rootNode.getJsonArray("opsQueue")) {
                        String dsInfo = ((JsonObject) on).getString("data");
                        String request = ((JsonObject) on).getString("request");
                        serviceStatus.storeOpItems(dsInfo, request);
                    }
                    serviceStatus.setLockedCount(rootNode.getInt("lockCount"));
                    for (JsonValue lock : rootNode.getJsonArray("locks")) {
                        Long dsId = ((JsonObject) lock).getJsonNumber("id").longValueExact();
                        serviceStatus.storeLockedDs(dsId);
                    }
                    for (JsonValue num : rootNode.getJsonArray("failures")) {
                        Long id = ((JsonNumber) num).longValueExact();
                        serviceStatus.storeFailure(id);
                    }
                    return serviceStatus;
                } catch (JsonException e) {
                    throw new InternalException(
                            "TestingClient " + e.getClass() + " " + e.getMessage() + " from " + result);
                }

            } catch (InsufficientStorageException | DataNotOnlineException | InternalException | BadRequestException
                     | NotFoundException e) {
                throw new InternalException(e.getClass() + " " + e.getMessage());
            }
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }
    }

    public long getSize(String sessionId, DataSelection data, int sc) throws BadRequestException, NotFoundException,
            InsufficientPrivilegesException, InternalException, NotImplementedException {

        URIBuilder uriBuilder = getUriBuilder("getSize");
        uriBuilder.setParameter(RequestIdNames.sessionId, sessionId);
        for (Entry<String, String> entry : data.getParameters().entrySet()) {
            uriBuilder.setParameter(entry.getKey(), entry.getValue());
        }
        URI uri = getUri(uriBuilder);

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(uri);
            try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
                checkResponseConformity(response);
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
        uriBuilder.setParameter(RequestIdNames.preparedId, preparedId);
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

    public Status getStatus(String sessionId, DataSelection data, Integer sc) throws BadRequestException,
            NotFoundException, InsufficientPrivilegesException, InternalException, NotImplementedException {

        URIBuilder uriBuilder = getUriBuilder("getStatus");
        if (sessionId != null) {
            uriBuilder.setParameter(RequestIdNames.sessionId, sessionId);
        }
        for (Entry<String, String> entry : data.getParameters().entrySet()) {
            uriBuilder.setParameter(entry.getKey(), entry.getValue());
        }
        URI uri = getUri(uriBuilder);

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(uri);

            try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
                checkResponseConformity(response);
                return Status.valueOf(getString(response, sc));
            } catch (InsufficientStorageException | DataNotOnlineException e) {
                throw new InternalException(e.getClass() + " " + e.getMessage());
            }
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

    }

    public Status getStatus(String preparedId, Integer sc) throws BadRequestException,
            NotFoundException, InsufficientPrivilegesException, InternalException, NotImplementedException {

        URIBuilder uriBuilder = getUriBuilder("getStatus");
        uriBuilder.setParameter(RequestIdNames.preparedId, preparedId);
        URI uri = getUri(uriBuilder);

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(uri);

            try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
                return Status.valueOf(getString(response, sc));
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

    public boolean isPrepared(String preparedId, Integer sc)
            throws BadRequestException, NotFoundException, InternalException, NotImplementedException {

        URIBuilder uriBuilder = getUriBuilder("isPrepared");
        uriBuilder.setParameter(RequestIdNames.preparedId, preparedId);
        URI uri = getUri(uriBuilder);

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(uri);

            try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
                checkResponseConformity(response);
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
                checkResponseConformity(response);
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
                checkResponseConformity(response);
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
                checkResponseConformity(response);
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
        formparams.add(new BasicNameValuePair(RequestIdNames.sessionId, sessionId));
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
                checkResponseConformity(response);
                return getString(response, sc);
            } catch (InsufficientStorageException | DataNotOnlineException e) {
                throw new InternalException(e.getClass() + " " + e.getMessage());
            }
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

    }

    public Long put(String sessionId, InputStream inputStream, String name, long datasetId, long datafileFormatId,
                    String description, Integer sc)
            throws BadRequestException, NotFoundException, InternalException, InsufficientPrivilegesException,
            NotImplementedException, DataNotOnlineException, InsufficientStorageException {
        return put(sessionId, inputStream, name, datasetId, datafileFormatId, description, null, null, null, sc);
    }

    public Long put(String sessionId, InputStream inputStream, String name, long datasetId, long datafileFormatId,
                    String description, String doi, Date datafileCreateTime, Date datafileModTime, Integer sc)
            throws BadRequestException, NotFoundException, InternalException, InsufficientPrivilegesException,
            NotImplementedException, DataNotOnlineException, InsufficientStorageException {

        if (inputStream == null) {
            throw new BadRequestException("Input stream is null");
        }
        CRC32 crc = new CRC32();
        inputStream = new CheckedInputStream(inputStream, crc);
        URIBuilder uriBuilder = getUriBuilder("put");
        uriBuilder.setParameter(RequestIdNames.sessionId, sessionId).setParameter("name", name)
                .setParameter("datafileFormatId", Long.toString(datafileFormatId))
                .setParameter("datasetId", Long.toString(datasetId));
        if (description != null) {
            uriBuilder.setParameter("description", description);
        }
        if (doi != null) {
            uriBuilder.setParameter("doi", doi);
        }
        if (datafileCreateTime != null) {
            uriBuilder.setParameter("datafileCreateTime", Long.toString(datafileCreateTime.getTime()));
        }
        if (datafileModTime != null) {
            uriBuilder.setParameter("datafileModTime", Long.toString(datafileModTime.getTime()));
        }

        URI uri = getUri(uriBuilder);
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPut httpPut = new HttpPut(uri);
        httpPut.setEntity(new InputStreamEntity(inputStream, ContentType.APPLICATION_OCTET_STREAM));

        try (CloseableHttpResponse response = httpclient.execute(httpPut)) {
            String result = getString(response, sc);
            checkResponseConformity(response);
            try (JsonReader jsonReader = Json.createReader(new StringReader(result))) {
                JsonObject rootNode = jsonReader.readObject();
                if (rootNode.getJsonNumber("checksum").longValueExact() != crc.getValue()) {
                    throw new InternalException("Error uploading - the checksum was not as expected");
                }
                return rootNode.getJsonNumber("id").longValueExact();
            }
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        } catch (NumberFormatException e) {
            throw new InternalException("Web service call did not return a valid Long value");
        }

    }

    public Long putAsPost(String sessionId, InputStream inputStream, String name, long datasetId, long datafileFormatId,
                          String description, String doi, Date datafileCreateTime, Date datafileModTime, boolean wrap, Integer sc)
            throws BadRequestException, NotFoundException, InternalException, InsufficientPrivilegesException,
            NotImplementedException, DataNotOnlineException, InsufficientStorageException {

        if (inputStream == null) {
            throw new BadRequestException("Input stream is null");
        }
        CRC32 crc = new CRC32();
        inputStream = new CheckedInputStream(inputStream, crc);
        URI uri = getUri(getUriBuilder("put"));

        MultipartEntityBuilder reqEntityBuilder = MultipartEntityBuilder.create()
                .addPart(RequestIdNames.sessionId, new StringBody(sessionId, ContentType.TEXT_PLAIN))
                .addPart("datafileFormatId", new StringBody(Long.toString(datafileFormatId), ContentType.TEXT_PLAIN))
                .addPart("name", new StringBody(name, ContentType.TEXT_PLAIN))
                .addPart("datasetId", new StringBody(Long.toString(datasetId), ContentType.TEXT_PLAIN));
        if (description != null) {
            reqEntityBuilder.addPart("description", new StringBody(description, ContentType.TEXT_PLAIN));
        }
        if (doi != null) {
            reqEntityBuilder.addPart("doi", new StringBody(doi, ContentType.TEXT_PLAIN));
        }
        if (datafileCreateTime != null) {
            reqEntityBuilder.addPart("datafileCreateTime",
                    new StringBody(Long.toString(datafileCreateTime.getTime()), ContentType.TEXT_PLAIN));
        }
        if (datafileModTime != null) {
            reqEntityBuilder.addPart("datafileModTime",
                    new StringBody(Long.toString(datafileModTime.getTime()), ContentType.TEXT_PLAIN));
        }
        if (wrap) {
            reqEntityBuilder.addPart("wrap", new StringBody("true", ContentType.TEXT_PLAIN));
        }
        InputStreamBody body = new InputStreamBody(new BufferedInputStream(inputStream),
                ContentType.APPLICATION_OCTET_STREAM, "unreliable");
        HttpEntity entity = reqEntityBuilder.addPart("file", body).build();

        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(uri);
        httpPost.setEntity(entity);

        try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
            checkResponseConformity(response);
            String result = getString(response, sc);
            String prefix = "<html><script type=\"text/javascript\">window.name='";
            String suffix = "';</script></html>";
            if (result.startsWith(prefix)) {
                result = result.substring(prefix.length(), result.length() - suffix.length());
            }
            try (JsonReader jsonReader = Json.createReader(new StringReader(result))) {
                JsonObject rootNode = jsonReader.readObject();
                if (rootNode.getJsonNumber("checksum").longValueExact() != crc.getValue()) {
                    throw new InternalException("Error uploading - the checksum was not as expected");
                }
                return rootNode.getJsonNumber("id").longValueExact();
            }
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        } catch (NumberFormatException e) {
            throw new InternalException("Web service call did not return a valid Long value");
        }

    }

    public void restore(String sessionId, DataSelection data, Integer sc) throws NotImplementedException,
            BadRequestException, InsufficientPrivilegesException, InternalException, NotFoundException {

        URI uri = getUri(getUriBuilder("restore"));
        List<NameValuePair> formparams = new ArrayList<>();
        formparams.add(new BasicNameValuePair(RequestIdNames.sessionId, sessionId));
        for (Entry<String, String> entry : data.getParameters().entrySet()) {
            formparams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }
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

    public List<Long> getDatafileIds(String preparedId, Integer sc)
            throws InternalException, BadRequestException, NotFoundException {

        URIBuilder uriBuilder = getUriBuilder("getDatafileIds");
        uriBuilder.setParameter(RequestIdNames.preparedId, preparedId);
        URI uri = getUri(uriBuilder);

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(uri);

            try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
                String result = getString(response, sc);
                checkResponseConformity(response);
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

    public List<Long> getDatafileIds(String sessionId, DataSelection data, Integer sc)
            throws InternalException, BadRequestException, ParseException, NotFoundException {
        URIBuilder uriBuilder = getUriBuilder("getDatafileIds");
        uriBuilder.setParameter(RequestIdNames.sessionId, sessionId);
        for (Entry<String, String> entry : data.getParameters().entrySet()) {
            uriBuilder.setParameter(entry.getKey(), entry.getValue());
        }
        URI uri = getUri(uriBuilder);

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(uri);

            try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
                checkResponseConformity(response);
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
        formparams.add(new BasicNameValuePair(RequestIdNames.preparedId, preparedId));

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

    public void reset(String sessionId, DataSelection data, int sc) throws InternalException, BadRequestException,
            ParseException, InsufficientPrivilegesException, NotImplementedException, NotFoundException {
        URI uri = getUri(getUriBuilder("reset"));
        List<NameValuePair> formparams = new ArrayList<>();
        formparams.add(new BasicNameValuePair(RequestIdNames.sessionId, sessionId));
        for (Entry<String, String> entry : data.getParameters().entrySet()) {
            formparams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }
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

    public void write(String sessionId, DataSelection data, Integer sc) throws NotImplementedException,
            BadRequestException, InsufficientPrivilegesException, InternalException, NotFoundException,
            DataNotOnlineException {

        URI uri = getUri(getUriBuilder("write"));
        List<NameValuePair> formparams = new ArrayList<>();
        formparams.add(new BasicNameValuePair(RequestIdNames.sessionId, sessionId));
        for (Entry<String, String> entry : data.getParameters().entrySet()) {
            formparams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(uri);
            httpPost.setEntity(new UrlEncodedFormEntity(formparams));
            try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
                expectNothing(response, sc);
            } catch (InsufficientStorageException e) {
                throw new InternalException(e.getClass() + " " + e.getMessage());
            }
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }
    }

    private void checkResponseConformity(HttpResponse response) throws InternalException {

        StatusLine status = response.getStatusLine();
        var statusCode = status.getStatusCode();
        if(statusCode == 200 && response.getEntity() != null) {
            //we have a status of 200 and a payload. Check for header conformity
            Boolean chunked = response.getFirstHeader("Transfer-Encoding") != null ? response.getFirstHeader("Transfer-Encoding").getValue().contains("chunked")
                                                                                        : false;

            if ( (response.getFirstHeader("Content-Length") == null && !chunked)
                || (response.getFirstHeader("Content-Length") != null && chunked ) ) {

                throw new InternalException("Responses with payload should either contain the Content-Length header or the 'Transfer-Encoding: chunked' header. It needs exactly one of them.");
            }
        }
    }

}
