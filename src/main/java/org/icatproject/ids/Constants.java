package org.icatproject.ids;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Constants {
	public static String API_VERSION;

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
