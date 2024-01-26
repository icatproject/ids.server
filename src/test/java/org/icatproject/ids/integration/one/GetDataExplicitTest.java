package org.icatproject.ids.integration.one;

import java.io.InputStream;
import java.util.Arrays;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

import org.icatproject.Datafile;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.icatproject.ids.integration.util.client.BadRequestException;
import org.icatproject.ids.integration.util.client.DataSelection;
import org.icatproject.ids.integration.util.client.InsufficientPrivilegesException;
import org.icatproject.ids.integration.util.client.NotFoundException;
import org.icatproject.ids.integration.util.client.TestingClient.Flag;

public class GetDataExplicitTest extends BaseTest {

    @BeforeClass
    public static void setup() throws Exception {
        setup = new Setup("one.properties");
        icatsetup();
    }

    @Test
    public void getSizes() throws Exception {
        Datafile df = null;
        Long size = 0L;
        try {
            df = (Datafile) icatWS.get(sessionId, "Datafile INCLUDE 1", datafileIds.get(0));
            size = df.getFileSize();
            df.setFileSize(size + 1);
            icatWS.update(sessionId, df);
            assertEquals(209L, testingClient.getSize(sessionId, new DataSelection().addDatafiles(datafileIds), 200));
        } finally {
            if (df != null) {
                df.setFileSize(size);
                icatWS.update(sessionId, df);
            }
        }
    }

    @Test
    public void getSizePreparedId() throws Exception {
        Datafile df = null;
        Long size = 0L;
        try {
            df = (Datafile) icatWS.get(sessionId, "Datafile INCLUDE 1", datafileIds.get(0));
            size = df.getFileSize();
            df.setFileSize(size + 1);
            icatWS.update(sessionId, df);
            DataSelection selection = new DataSelection().addDatafiles(datafileIds);
            String preparedId = testingClient.prepareData(sessionId, selection, Flag.NONE, 200);
            assertEquals(209L, testingClient.getSize(preparedId, 200));
        } finally {
            if (df != null) {
                df.setFileSize(size);
                icatWS.update(sessionId, df);
            }
        }
    }

    @Test
    public void getSizes1() throws Exception {
        assertEquals(208L, testingClient.getSize(sessionId, new DataSelection().addDatafiles(datafileIds), 200));
        assertEquals(208L, testingClient.getSize(sessionId, new DataSelection().addDatasets(datasetIds), 200));
        assertEquals(208L,
                testingClient.getSize(sessionId, new DataSelection().addInvestigation(investigationId), 200));
        assertEquals(208L, testingClient.getSize(sessionId,
                new DataSelection().addInvestigation(investigationId).addDatafiles(datafileIds), 200));
        assertEquals(0L, testingClient.getSize(sessionId, new DataSelection().addDataset(datasetIds.get(2)), 200));
    }

    @Test(expected = NotFoundException.class)
    public void getSizesBad() throws Exception {
        testingClient.getSize(sessionId, new DataSelection().addDatafile(563L), 404);
    }

    @Test(expected = NotFoundException.class)
    public void getSizesBad2() throws Exception {
        testingClient.getSize(sessionId, new DataSelection().addDatafile(563L).addDatafile(564L), 404);
    }

    @Test(expected = BadRequestException.class)
    public void badPreparedIdFormatTest() throws Exception {
        try (InputStream z = testingClient.getData("bad preparedId format", 0, 400)) {
        }
    }

    @Test(expected = InsufficientPrivilegesException.class)
    public void forbiddenTest() throws Exception {
        try (InputStream z = testingClient.getData(setup.getForbiddenSessionId(),
                new DataSelection().addDatafiles(datafileIds), Flag.NONE, 0, 403)) {
        }
    }

    @Test
    public void correctBehaviourTestNone() throws Exception {
        try (InputStream stream = testingClient.getData(sessionId, new DataSelection().addDatafiles(datafileIds),
                Flag.NONE, 0, 200, r -> {
                    assertThat(r.getFirstHeader("Content-Length"), is(nullValue()));
                    assertThat(r.getFirstHeader("Transfer-Encoding").getValue(),
                            is(equalToIgnoringCase("chunked")));
                })) {
            checkZipStream(stream, datafileIds, 57L, 0);
        }

        var datafileId = datafileIds.get(0);
        var fileLength = fileLength(datafileId);
        try (InputStream stream = testingClient.getData(sessionId, new DataSelection().addDatafile(datafileId),
                Flag.NONE, 0, 200, r -> {
                    assertThat(r.getFirstHeader("Content-Length"), is(not(nullValue())));
                    var contentLength = r.getFirstHeader("Content-Length").getValue();
                    assertThat(Long.valueOf(contentLength), is(equalTo(fileLength)));
                    assertThat(r.getFirstHeader("Transfer-Encoding"), is(nullValue()));
                })) {
            checkStream(stream, datafileIds.get(0));
        }
    }

    @Test
    public void correctBehaviourTestCompress() throws Exception {
        try (InputStream stream = testingClient.getData(sessionId, new DataSelection().addDatafiles(datafileIds),
                Flag.COMPRESS, 0, 200, r -> {
                    assertThat(r.getFirstHeader("Content-Length"), is(nullValue()));
                    assertThat(r.getFirstHeader("Transfer-Encoding").getValue(),
                            is(equalToIgnoringCase("chunked")));
                })) {
            checkZipStream(stream, datafileIds, 36L, 0);
        }

        try (InputStream stream = testingClient.getData(sessionId, new DataSelection().addDatafile(datafileIds.get(0)),
                Flag.COMPRESS, 0, 200, r -> {
                    assertThat(r.getFirstHeader("Content-Length"), is(nullValue()));
                    assertThat(r.getFirstHeader("Transfer-Encoding").getValue(),
                            is(equalToIgnoringCase("chunked")));
                })) {
            checkStream(stream, datafileIds.get(0));
        }
    }

    @Test
    public void correctBehaviourTestZip() throws Exception {
        try (InputStream stream = testingClient.getData(sessionId, new DataSelection().addDatafiles(datafileIds),
                Flag.ZIP, 0, 200, r -> {
                    assertThat(r.getFirstHeader("Content-Length"), is(nullValue()));
                    assertThat(r.getFirstHeader("Transfer-Encoding").getValue(),
                            is(equalToIgnoringCase("chunked")));
                })) {
            checkZipStream(stream, datafileIds, 57L, 0);
        }

        try (InputStream stream = testingClient.getData(sessionId, new DataSelection().addDatafile(datafileIds.get(0)),
                Flag.ZIP, 0, 200, r -> {
                    assertThat(r.getFirstHeader("Content-Length"), is(nullValue()));
                    assertThat(r.getFirstHeader("Transfer-Encoding").getValue(),
                            is(equalToIgnoringCase("chunked")));
                })) {
            checkZipStream(stream, datafileIds.subList(0, 1), 57L, 0);
        }
    }

    @Test
    public void correctBehaviourTestZipAndCompress() throws Exception {
        try (InputStream stream = testingClient.getData(sessionId, new DataSelection().addDatafiles(datafileIds),
                Flag.ZIP_AND_COMPRESS, 0, 200, r -> {
                    assertThat(r.getFirstHeader("Content-Length"), is(nullValue()));
                    assertThat(r.getFirstHeader("Transfer-Encoding").getValue(),
                            is(equalToIgnoringCase("chunked")));
                })) {
            checkZipStream(stream, datafileIds, 36L, 0);
        }

        try (InputStream stream = testingClient.getData(sessionId, new DataSelection().addDatafile(datafileIds.get(0)),
                Flag.ZIP_AND_COMPRESS, 0, 200, r -> {
                    assertThat(r.getFirstHeader("Content-Length"), is(nullValue()));
                    assertThat(r.getFirstHeader("Transfer-Encoding").getValue(),
                            is(equalToIgnoringCase("chunked")));
                })) {
            checkZipStream(stream, datafileIds.subList(0, 1), 36L, 0);
        }
    }

    @Test
    public void correctBehaviourInvestigation() throws Exception {
        try (InputStream stream = testingClient.getData(sessionId,
                new DataSelection().addInvestigation(investigationId), Flag.NONE, 0, 200)) {
            checkZipStream(stream, datafileIds, 57L, 0);
        }

        try (InputStream stream = testingClient.getData(sessionId, new DataSelection().addDatafile(datafileIds.get(0)),
                Flag.ZIP, 0, 200)) {
            checkZipStream(stream, datafileIds.subList(0, 1), 57L, 0);
        }
    }

    @Test
    public void correctBehaviourInvestigations() throws Exception {
        try (InputStream stream = testingClient.getData(sessionId,
                new DataSelection().addInvestigations(Arrays.asList(investigationId)), Flag.NONE, 0, 200)) {
            checkZipStream(stream, datafileIds, 57L, 0);
        }

        try (InputStream stream = testingClient.getData(sessionId, new DataSelection().addDatafile(datafileIds.get(0)),
                Flag.ZIP, 0, 200)) {
            checkZipStream(stream, datafileIds.subList(0, 1), 57L, 0);
        }
    }

}
