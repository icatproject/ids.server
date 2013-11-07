package org.icatproject.ids;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import org.icatproject.ids.RangeOutputStream;
import org.junit.Test;

public class RangeOutputStreamTest {

	byte[] bytes = "ABCDEFGHIJKLMONPQRSTUVWXYZ".getBytes();

	@Test
	public void t1() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputStream os = new RangeOutputStream(baos, 4L, 7L);
		for (byte b : bytes) {
			os.write(b);
		}
		os.close();
		assertEquals("EFGHIJK", baos.toString());
	}

	@Test
	public void t2() throws Exception {
		for (int m = 1 ; m < 26; m++) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputStream os = new RangeOutputStream(baos, 4L, 7L);
		for (int i = 0; i < bytes.length; i += 3) {
			int n = Math.min(3, bytes.length - i);
			os.write(bytes, i, n);
			os.flush();
		}
		os.close();
		assertEquals("m is " + m, "EFGHIJK", baos.toString());
		}
	}
	
	@Test
	public void t3() throws Exception {
		for (int m = 1 ; m < 26; m++) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputStream os = new RangeOutputStream(baos, 0L, null);
		for (int i = 0; i < bytes.length; i += 3) {
			int n = Math.min(3, bytes.length - i);
			os.write(bytes, i, n);
			os.flush();
		}
		os.close();
		assertEquals("m is " + m, "ABCDEFGHIJKLMONPQRSTUVWXYZ", baos.toString());
		}
	}
	
	@Test
	public void t4() throws Exception {
		for (int m = 1 ; m < 26; m++) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputStream os = new RangeOutputStream(baos, 0L, 13L);
		for (int i = 0; i < bytes.length; i += 3) {
			int n = Math.min(3, bytes.length - i);
			os.write(bytes, i, n);
			os.flush();
		}
		os.close();
		assertEquals("m is " + m, "ABCDEFGHIJKLM", baos.toString());
		}
	}


	@Test
	public void t5() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputStream os = new RangeOutputStream(baos, 4L, 7L);
		os.write(bytes);
		os.close();
		assertEquals("EFGHIJK", baos.toString());
	}
}
