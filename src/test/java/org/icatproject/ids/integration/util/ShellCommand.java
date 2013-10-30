package org.icatproject.ids.integration.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A wrapper around ProcessBuilder to allow stdout and stderr to be kept separate without risk of
 * blocking. It throws no exceptions - it is the responsibility of the caller to look at the exit
 * code and at stderr and stdout to see if it worked as expected.
 */
public class ShellCommand {

	private int exitValue;
	private String stdout;
	private String stderr;

	public ShellCommand(String... args) {
		List<String> arglist = new ArrayList<String>(args.length);
		for (String arg : args) {
			arglist.add(arg);
		}
		init(arglist);
	}

	public ShellCommand(List<String> arglist) {
		init(arglist);
	}

	private void init(List<String> args) {
		Process p = null;
		StreamReader osr;
		StreamReader esr;
		InputStream posr = null;
		InputStream pesr = null;
		OutputStream pisr = null;
		try {
			p = new ProcessBuilder(args).start();

			posr = p.getInputStream();
			osr = new StreamReader(posr);
			osr.start();

			pesr = p.getErrorStream();
			esr = new StreamReader(pesr);
			esr.start();

			pisr = p.getOutputStream();
			pisr.close(); // Close the stream feeding the process

			osr.join();
			esr.join();
			p.waitFor();

			exitValue = p.exitValue();
			stdout = osr.getOut();
			stderr = esr.getOut();

		} catch (Exception e) {
			exitValue = 1; // Standard linux "catchall" value
			stdout = "";
			stderr = e.getMessage();

		} finally {
			/* Make sure everything is safely closed */
			close(pisr);
			close(posr);
			close(pesr);
		}
	}

	/** Close a stream ignoring any errors */
	private void close(Closeable stream) {
		if (stream != null) {
			try {
				stream.close();
			} catch (IOException e) {
				// Ignore
			}
		}
	}

	public String getStdout() {
		return stdout;
	}

	public String getStderr() {
		return stderr;
	}

	public int getExitValue() {
		return exitValue;
	}

	/**
	 * This considers an error to be a non-zero exit code or a non-empty stderr. This will not be
	 * valid for all commands.
	 */
	public boolean isError() {
		return exitValue != 0 || !stderr.isEmpty();
	}

	/** Returns an error message based on the exit code and what was returned in stderr */
	public String getMessage() {
		return "code " + exitValue + ": " + stderr;
	}

}