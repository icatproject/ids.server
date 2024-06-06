package org.icatproject.ids;

import static org.junit.Assert.assertEquals;

import org.icatproject.ICAT;
import org.icatproject.ids.services.ICATGetter;
import org.junit.Test;

public class TestICATGetter {

    @Test
    public void testGFUrl() throws Exception {
        assertEquals(
            "http://localhost",
            ICATGetter.getCleanUrl("http://localhost/ICATService/ICAT?wsdl")
        );
    }

    @Test
    public void testSlash() throws Exception {
        assertEquals(
            "http://localhost",
            ICATGetter.getCleanUrl("http://localhost/")
        );
    }

    @Test
    public void testClean() throws Exception {
        assertEquals(
            "http://localhost",
            ICATGetter.getCleanUrl("http://localhost")
        );
    }

    @Test
    public void testGetService() throws Exception {
        ICAT icat = ICATGetter.getService(System.getProperty("serverUrl"));
        System.out.println(icat.getApiVersion());
    }
}
