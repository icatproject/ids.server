package org.icatproject.ids;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Constants {
	
	public static String API_VERSION;

	public static final String PREPARED_DIR_NAME = "prepared";
	public static final String COMPLETED_DIR_NAME = "completed";
	public static final String FAILED_DIR_NAME = "failed";

	static {

		InputStream inputStream = Constants.class.getClassLoader().getResourceAsStream(
				"app.properties");
		Properties p = new Properties();

		try {
			p.load(inputStream);
			API_VERSION = p.getProperty("project.version");
		} catch (IOException e) {
			// API Version will be null
		}

	}
}
