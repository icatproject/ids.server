package org.icatproject.ids.integration.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Small class to read a stream and put the result into a string. It extends Thread because it is
 * designed to be used asynchronously in conjunction with the ProcessBuilder.
 * 
 * @See ShellCommand
 */
public class StreamReader extends Thread {

	private String out;
	private InputStream inputStream;
	private IOException iOexception;

	public StreamReader(InputStream inputStream) {
		this.inputStream = inputStream;
	}

	@Override
	public void run() {
		try {
			byte[] buff = new byte[4096];
			int n;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			while ((n = inputStream.read(buff)) >= 0) {
				baos.write(buff, 0, n);
			}
			out = baos.toString();
		} catch (IOException e) {
			this.iOexception = e;
		}
	}

	public String getOut() throws IOException {
		if (iOexception != null) {
			throw iOexception;
		}
		return out;
	}

}