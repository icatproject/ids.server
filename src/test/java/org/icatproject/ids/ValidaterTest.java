package org.icatproject.ids;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.icatproject.ids.exceptions.BadRequestException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidaterTest {

    private static final Logger logger = LoggerFactory.getLogger(ValidaterTest.class);

	@Test
	public void testIsValidId() throws Exception {
		testValidUUID(false, null);
		testValidUUID(false, "");
		testValidUUID(false, "CBB082C8-1307-433C-9D7B-856B6C0878E9");
		testValidUUID(false, "xyz082c8-130743-3c-9d7b-856b6c0878e9");
		testValidUUID(false, "some random string");
		testValidUUID(false, "cbb082c8-130743-3c-9d7b-856b6c0878e9");
		testValidUUID(true, "cbb082c8-1307-433c-9d7b-856b6c0878e9");
	}

	private void testValidUUID(boolean b, String id) {
		try {
			IdsBean.validateUUID("testValidUUID", id);
			assertTrue(b);
		} catch (BadRequestException e) {
			logger.debug(e.getMessage());
			assertFalse(b);
		}

	}

}
