package org.icatproject.ids;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.icatproject.ids.webservice.DataSelection;
import org.icatproject.ids.webservice.IdsBean;
import org.icatproject.ids.webservice.exceptions.BadRequestException;
import org.junit.Test;

public class ValidaterTest {

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
			System.out.println(e.getMessage());
			assertFalse(b);
		}

	}

	private void testInValidIdList(String ids) {
		try {
			DataSelection.getValidIds("testValidIdList", ids);
			fail("Should have thrown exception");
		} catch (BadRequestException e) {
			System.out.println(e.getMessage());
		}

	}

	private void testValidIdList(String ids, List<Long> listIds) throws Exception {
		assertEquals(listIds, DataSelection.getValidIds("testValidIdList", ids));
	}

	@Test
	public void testIsValidIdList() throws Exception {
		testInValidIdList("123456,123456,123456,123456,,123456");
		testInValidIdList("");
		testInValidIdList("abc");
		testInValidIdList("abc,def");
		testInValidIdList("123\u00ea");
		testInValidIdList("123,456\n");
		testInValidIdList("123.4,789");
		testInValidIdList("99999999999999999999");

		testValidIdList(null, Arrays.asList(new Long[0]));
		testValidIdList("123", Arrays.asList(123L));
		testValidIdList("123,456", Arrays.asList(123L, 456L));
		testValidIdList("123, 456", Arrays.asList(123L, 456L));
		testValidIdList("0,1,2,3,4,5,6,7,8,9",
				Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L));
		testValidIdList("99999", Arrays.asList(99999L));

	}

}
