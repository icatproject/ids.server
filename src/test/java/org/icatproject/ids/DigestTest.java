package org.icatproject.ids;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.v3.helper.LocationHelper;
import org.icatproject.utils.IcatSecurity;

public class DigestTest {

    String location = "/abc/def/ghi/jkl";
    String key = "abcdefghijklm";

    @Test
    public void testCheck() throws Exception {
        String a = location + " " + IcatSecurity.digest(1234567L, location, key);

        assertEquals(location, LocationHelper.getLocationFromDigest(1234567L, a, key));

        try {
            LocationHelper.getLocationFromDigest(1234568L, a, key);
            fail();
        } catch (InsufficientPrivilegesException e) {
            assertTrue(e.getMessage().contains("does not contain a valid hash"));
        }

        try {
            LocationHelper.getLocationFromDigest(1234568L, location, key);
            fail();
        } catch (InsufficientPrivilegesException e) {
            assertTrue(e.getMessage().contains("does not contain hash"));
        }

    }

}
