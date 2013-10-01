package org.icatproject.ids;

import static org.junit.Assert.assertEquals;

import org.icatproject.ids.util.ValidationHelper;
import org.junit.Test;

/*
 * Test each of the methods associated with validating the inputs to the 
 * IDS RESTful web interface.
 */
public class ValidationHelperTest {
	@Test
	public void aa() throws Exception {
		String idList = "123456,123456,123456,123456,,123456";
		assertEquals(false, ValidationHelper.isValidIdList(idList));
	}

	@Test
	public void testIsValidId() throws Exception {
		String msg = "test empty / null";
		assertEquals(msg, false, ValidationHelper.isValidId(null));
		assertEquals(msg, false, ValidationHelper.isValidId(""));

		msg = "test invalid characters";
		assertEquals(msg, false, ValidationHelper.isValidId("CBB082C8-1307-433C-9D7B-856B6C0878E9"));
		assertEquals(msg, false, ValidationHelper.isValidId("xyz082c8-130743-3c-9d7b-856b6c0878e9"));

		msg = "test incorrect UUID format";
		assertEquals(msg, false, ValidationHelper.isValidId("some random string"));
		assertEquals(msg, false, ValidationHelper.isValidId("cbb082c8-130743-3c-9d7b-856b6c0878e9"));

		msg = "valid";
		assertEquals(msg, true, ValidationHelper.isValidId("cbb082c8-1307-433c-9d7b-856b6c0878e9"));
	}

	@Test
	public void testIsValidIdList() throws Exception {
		String msg = "test empty";
		assertEquals(msg, false, ValidationHelper.isValidIdList(""));

		msg = "test invalid longs";
		assertEquals(msg, false, ValidationHelper.isValidIdList("abc"));
		assertEquals(msg, false, ValidationHelper.isValidIdList("abc,def"));
		assertEquals(msg, false, ValidationHelper.isValidIdList("123\u00ea"));
		assertEquals(msg, false, ValidationHelper.isValidIdList("123,456\n"));
		assertEquals(msg, false, ValidationHelper.isValidIdList("123.4,789"));
		assertEquals(msg, false, ValidationHelper.isValidIdList("99999999999999999999"));

		msg = "valid";
		assertEquals(msg, true, ValidationHelper.isValidIdList(null));
		assertEquals(msg, true, ValidationHelper.isValidIdList("123"));
		assertEquals(msg, true, ValidationHelper.isValidIdList("123,456"));
		assertEquals(msg, true, ValidationHelper.isValidIdList("123, 456"));
		assertEquals(msg, true, ValidationHelper.isValidIdList("0,1,2,3,4,5,6,7,8,9"));
		assertEquals(msg, true, ValidationHelper.isValidIdList("99999"));
	}

	@Test
	public void testIsValidBoolen() throws Exception {
		String msg = "test empty";
		assertEquals(msg, false, ValidationHelper.isValidBoolean(""));

		msg = "test invalid format";
		assertEquals(msg, false, ValidationHelper.isValidBoolean("True "));
		assertEquals(msg, false, ValidationHelper.isValidBoolean("fasle"));
		assertEquals(msg, false, ValidationHelper.isValidBoolean("true\n"));

		msg = "valid";
		assertEquals(msg, true, ValidationHelper.isValidBoolean(null));
		assertEquals(msg, true, ValidationHelper.isValidBoolean("true"));
		assertEquals(msg, true, ValidationHelper.isValidBoolean("false"));
		assertEquals(msg, true, ValidationHelper.isValidBoolean("TRUE"));
		assertEquals(msg, true, ValidationHelper.isValidBoolean("FALSE"));
		assertEquals(msg, true, ValidationHelper.isValidBoolean("True"));
		assertEquals(msg, true, ValidationHelper.isValidBoolean("False"));
		assertEquals(msg, true, ValidationHelper.isValidBoolean("\u0074rue"));
	}

	@Test
	public void testIsValidOffset() throws Exception {
		String msg = "test empty";
		assertEquals(msg, false, ValidationHelper.isValidOffset(""));

		msg = "test invalid format";
		assertEquals(msg, false, ValidationHelper.isValidOffset("-1234"));
		assertEquals(msg, false, ValidationHelper.isValidOffset("abc123"));
		assertEquals(msg, false, ValidationHelper.isValidOffset("123.456"));
		assertEquals(msg, false, ValidationHelper.isValidOffset("123\n"));

		msg = "valid";
		assertEquals(msg, true, ValidationHelper.isValidOffset(null));
		assertEquals(msg, true, ValidationHelper.isValidOffset("123"));
		assertEquals(msg, true, ValidationHelper.isValidOffset("0123"));
		assertEquals(msg, true, ValidationHelper.isValidOffset("0"));
		assertEquals(msg, true, ValidationHelper.isValidOffset("99999999999999999999999999999999"));
	}

	@Test
	public void testIsValidName() throws Exception {
		String msg = "test empty";
		assertEquals(msg, false, ValidationHelper.isValidName(""));

		msg = "test invalid format";
		assertEquals(msg, false, ValidationHelper.isValidName("name/cannot/have/slashes"));
		assertEquals(msg, false, ValidationHelper.isValidName("both\\types\\of\\slashes"));

		msg = "valid";
		assertEquals(msg, true, ValidationHelper.isValidName(null));
		assertEquals(msg, true, ValidationHelper.isValidName("aaaaa"));
		assertEquals(msg, true, ValidationHelper.isValidName("name-with-extension.zip"));
		assertEquals(msg, true, ValidationHelper.isValidName("name-without-extension"));
		assertEquals(msg, true, ValidationHelper.isValidName("some name with spaces.jpg"));
		assertEquals(msg, true, ValidationHelper.isValidName(".zip"));
		assertEquals(msg, true, ValidationHelper.isValidName("\u00ea_some_unicode_name"));
		assertEquals(msg, true, ValidationHelper.isValidName("\u0000_oh_no_a_null_byte!"));
		assertEquals(msg, true, ValidationHelper.isValidName(".."));
		assertEquals(msg, true, ValidationHelper.isValidName(" .."));
	}
}
