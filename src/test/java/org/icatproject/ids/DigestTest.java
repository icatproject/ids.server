package org.icatproject.ids;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.junit.Test;

public class DigestTest {

	String location = "/abc/def/ghi/jkl";
	String key = "abcdefghijklm";

	@Test
	public void testDigest() throws Exception {

		String a = IdsBean.digest(1234567L, location, key);
		assertEquals(a, IdsBean.digest(1234567L, location, key));
		assertFalse(a.equals(IdsBean.digest(1234568L, location, key)));
		assertFalse(a.equals(IdsBean.digest(1234567L, "/abc/def/ghi/jkm", key)));
		assertFalse(a.equals(IdsBean.digest(1234567L, location, "abcdefghijkln")));
	}

	@Test
	public void testCheck() throws Exception {
		String a = location + " " + IdsBean.digest(1234567L, location, key);

		assertEquals(location, IdsBean.getLocationFromDigest(1234567L, a, key));

		try {
			IdsBean.getLocationFromDigest(1234568L, a, key);
			fail();
		} catch (InsufficientPrivilegesException e) {
			assertTrue(e.getMessage().contains("does not contain a valid hash"));
		}

		try {
			IdsBean.getLocationFromDigest(1234568L, location, key);
			fail();
		} catch (InsufficientPrivilegesException e) {
			assertTrue(e.getMessage().contains("does not contain hash"));
		}

	}

}