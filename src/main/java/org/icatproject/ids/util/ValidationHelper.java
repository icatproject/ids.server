package org.icatproject.ids.util;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This class provides validation function for the parameters of the RESTful methods.
 */
public class ValidationHelper {

    // matches standard UUID format of 8-4-4-4-12 hexadecimal digits
    private static final Pattern uuidRegExp = 
            Pattern.compile("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$");

    // matches a single positive integer
    private static final Pattern offsetRegExp = Pattern.compile("^[0-9]+$");

    // checks to see if filename contains any of the following invalid characters: / ? * : ; { } \
    // http://msdn.microsoft.com/en-us/library/aa365247%28v=vs.85%29.aspx#file_and_directory_names
    private static final Pattern filenameRegExp = Pattern.compile(".*[/\\\\:*?\"<>|]+.*");

    /**
     * Checks to see if the preparedId or sessionId conform to the correct UUID format. Does not
     * accept a null value.
     */
    public static boolean isValidId(String id) {
        return id != null && uuidRegExp.matcher(id).matches();
    }

    /**
     * Checks to see if the investigation, dataset or datafile id list is a valid comma separated
     * list of longs. No spaces or leading 0's. Also accepts null.
     */
    public static boolean isValidIdList(String idList) {
        boolean valid = true;
        if (idList != null && idList.isEmpty()) {
            valid = false;
        }
        
        // check that the longs are valid (ie. not too large)
        if (valid && idList != null) {
            List<String> ids = Arrays.asList(idList.split("\\s*,\\s*"));
            for (String id : ids) {
                try {
                    if (Long.parseLong(id) < 0) {
                        valid = false;
                    }                    
                } catch (NumberFormatException e) {
                    valid = false;
                }
            }
        }
            
        return valid;
    }

    /**
     * Check to see if string is either true or false or null
     */
    public static boolean isValidBoolean(String bool) {
        boolean valid = false;
        if (bool == null || "true".equalsIgnoreCase(bool) || "false".equalsIgnoreCase(bool)) {
            valid = true;
        }
        return valid;
    }

    /**
     * Check to see if file offset is valid ie. positive integer. Also accepts null.
     */
    public static boolean isValidOffset(String offset) {
        boolean valid = true;
        if (offset != null
                && (offset.isEmpty() == true || offsetRegExp.matcher(offset).matches() == false)) {
            valid = false;
        }
        return valid;
    }

    /**
     * Check to see if string is valid for a filename ie. dose not contain: / ? * : ; { } \ | Also
     * accepts null.
     */
    public static boolean isValidName(String name) {
        boolean valid = true;
        if (name != null && (name.isEmpty() || filenameRegExp.matcher(name).matches())) {
            valid = false;
        }
        return valid;
    }
}
