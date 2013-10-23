package org.icatproject.ids;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.icatproject.ids.util.ValidationHelper;
import org.icatproject.ids.webservice.exceptions.BadRequestException;
import org.junit.Test;

/*
 * Test each of the methods associated with validating the inputs to the 
 * IDS RESTful web interface.
 */
public class ValidationHelperTest {

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
			ValidationHelper.validateUUID("testValidUUID", id);
			assertTrue(b);
		} catch (BadRequestException e) {
			System.out.println(e.getMessage());
			assertFalse(b);
		}

	}

	private void testValidIdList(boolean b, String ids) {
		try {
			ValidationHelper.validateIdList("testValidIdList", ids);
			assertTrue(b);
		} catch (BadRequestException e) {
			System.out.println(e.getMessage());
			assertFalse(b);
		}

	}

	@Test
	public void testIsValidIdList() throws Exception {
		testValidIdList(false, "123456,123456,123456,123456,,123456");
		testValidIdList(false, "");
		testValidIdList(false, "abc");
		testValidIdList(false, "abc,def");
		testValidIdList(false, "123\u00ea");
		testValidIdList(false, "123,456\n");
		testValidIdList(false, "123.4,789");
		testValidIdList(false, "99999999999999999999");

		testValidIdList(true, null);
		testValidIdList(true, "123");
		testValidIdList(true, "123,456");
		testValidIdList(true, "123, 456");
		testValidIdList(true, "0,1,2,3,4,5,6,7,8,9");
		testValidIdList(true, "99999");

	}

}
