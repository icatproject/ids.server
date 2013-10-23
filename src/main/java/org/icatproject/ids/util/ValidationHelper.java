package org.icatproject.ids.util;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.icatproject.ids.webservice.exceptions.BadRequestException;

/*
 * This class provides validation function for the parameters of the RESTful methods.
 */
public class ValidationHelper {

	// matches standard UUID format of 8-4-4-4-12 hexadecimal digits
	private static final Pattern uuidRegExp = Pattern
			.compile("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$");

	/*
	 * Checks to see if the investigation, dataset or datafile id list is a valid comma separated
	 * list of longs. No spaces or leading 0's. Also accepts null.
	 */
	public static void validateIdList(String thing, String idList) throws BadRequestException {
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
		if (!valid) {
			throw new BadRequestException("The " + thing + " parameter '" + idList
					+ "' is not a valid "
					+ "string representation of a comma separated list of longs");
		}
	}

	public static void validateUUID(String thing, String id) throws BadRequestException {
		if (id == null || !uuidRegExp.matcher(id).matches())
			throw new BadRequestException("The " + thing + " parameter '" + id
					+ "' is not a valid UUID");
	}
}
