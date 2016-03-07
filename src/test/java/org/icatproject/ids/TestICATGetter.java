package org.icatproject.ids;

import static org.junit.Assert.assertTrue;

import org.icatproject.IcatException_Exception;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestICATGetter {

	@BeforeClass
	public static void beforeClass() {
		serverUrl = System.getProperty("serverUrl");
		System.out.println("serverUrl is " + serverUrl);
		if (serverUrl == null) {
			throw new NullPointerException();
		}
	}

	static String serverUrl;

	@Test(expected = IcatException_Exception.class)
	public final void testNull() throws Exception {
		ICATGetter.getService(null);
	}

	@Test
	public final void testGood() throws Exception {
		ICATGetter.getService(serverUrl);
	}

	@Test
	public final void testGood3() {

		boolean found = false;

		try {
			ICATGetter.getService(serverUrl + "/icat/ICAT?wsdl");
			found = true;
		} catch (IcatException_Exception e) {

		}
		try {
			ICATGetter.getService(serverUrl + "/ICATService/ICAT?wsdl");
			found = true;
		} catch (IcatException_Exception e) {

		}
		assertTrue(found);

	}

	@Test(expected = IcatException_Exception.class)
	public final void testBadSuffix() throws Exception {
		ICATGetter.getService(serverUrl + "/QQQ/ICAT?wsdl");
	}

}