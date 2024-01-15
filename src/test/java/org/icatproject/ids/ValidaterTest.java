package org.icatproject.ids;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import org.icatproject.ids.exceptions.BadRequestException;

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

}
