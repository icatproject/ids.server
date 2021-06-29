package org.icatproject.ids;

import static org.junit.Assert.assertEquals;

import org.icatproject.ICAT;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestICATGetter {

	private static final Logger logger = LoggerFactory.getLogger(TestICATGetter.class);

	@Test
	public void testGFUrl() throws Exception {
		assertEquals("http://localhost", ICATGetter.getCleanUrl("http://localhost/ICATService/ICAT?wsdl"));
	}

	@Test
	public void testSlash() throws Exception {
		assertEquals("http://localhost", ICATGetter.getCleanUrl("http://localhost/"));
	}

	@Test
	public void testClean() throws Exception {
		assertEquals("http://localhost", ICATGetter.getCleanUrl("http://localhost"));
	}

	@Ignore("not really a unit test as it requires an installed ICAT")
	@Test
	public void testGetService() throws Exception {
		ICAT icat = ICATGetter.getService(System.getProperty("serverUrl"));
		logger.debug(icat.getApiVersion());
	}
}