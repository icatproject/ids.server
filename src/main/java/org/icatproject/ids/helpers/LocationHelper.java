package org.icatproject.ids.helpers;

import java.security.NoSuchAlgorithmException;

import org.icatproject.utils.IcatSecurity;

import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.services.ServiceProvider;

public class LocationHelper {

    public static String getLocation(long dfid, String location)
            throws InsufficientPrivilegesException, InternalException {
        if (location == null) {
            throw new InternalException("location is null");
        }

        var key = ServiceProvider.getInstance().getPropertyHandler().getKey();
        if (key == null) {
            return location;
        } else {
            return getLocationFromDigest(dfid, location, key);
        }
    }

    public static String getLocationFromDigest(long id, String locationWithHash,
            String key)
            throws InternalException, InsufficientPrivilegesException {
        int i = locationWithHash.lastIndexOf(' ');
        try {
            String location = locationWithHash.substring(0, i);
            String hash = locationWithHash.substring(i + 1);
            if (!hash.equals(IcatSecurity.digest(id, location, key))) {
                throw new InsufficientPrivilegesException(
                        "Location \"" + locationWithHash
                                + "\" does not contain a valid hash.");
            }
            return location;
        } catch (IndexOutOfBoundsException e) {
            throw new InsufficientPrivilegesException("Location \""
                    + locationWithHash + "\" does not contain hash.");
        } catch (NoSuchAlgorithmException e) {
            throw new InternalException(e.getMessage());
        }
    }
}
