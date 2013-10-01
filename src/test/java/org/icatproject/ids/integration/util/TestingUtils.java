package org.icatproject.ids.integration.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.Assert;

public class TestingUtils {

	/*
	 * Takes in a outputstream of a zip file. Creates a mapping between the filenames and their MD5
	 * sums.
	 */
	public static Map<String, String> filenameMD5Map(ByteArrayOutputStream file)
			throws IOException, NoSuchAlgorithmException {
		Map<String, String> filenameMD5map = new HashMap<String, String>();
		ZipInputStream zis = null;
		ByteArrayOutputStream os = null;
		ZipEntry entry = null;
		try {
			zis = new ZipInputStream(new ByteArrayInputStream(file.toByteArray()));
			while ((entry = zis.getNextEntry()) != null) {
				os = new ByteArrayOutputStream();
				int len;
				byte[] buffer = new byte[1024];
				while ((len = zis.read(buffer)) != -1) { // zis.read will read up to last byte of
															// the entry
					os.write(buffer, 0, len);
				}

				MessageDigest m = MessageDigest.getInstance("MD5");
				m.reset();
				m.update(os.toByteArray());
				String md5sum = new BigInteger(1, m.digest()).toString(16);
				while (md5sum.length() < 32) {
					md5sum = "0" + md5sum;
				}
				filenameMD5map.put(entry.getName(), md5sum);
			}
		} finally {
			if (zis != null) {
				zis.close();
			}
			if (os != null) {
				os.close();
			}
		}
		return filenameMD5map;
	}

	public static void checkMD5Values(Map<String, String> map, Setup setup) {
		Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>) it.next();
			String correctMD5 = setup.getFilenameMD5().get(pairs.getKey());
			if (correctMD5 == null) {
				Assert.fail("Cannot find MD5 sum for filename '" + pairs.getKey() + "'");
			}

			Assert.assertEquals("Stored MD5 sum for " + pairs.getKey()
					+ " does not match downloaded file", correctMD5, pairs.getValue());
			it.remove();
		}
	}

	public static int countZipEntries(File zip) throws IOException {
		int res = 0;
		ZipInputStream in = null;
		try {
			in = new ZipInputStream(new FileInputStream(zip));
			while (in.getNextEntry() != null) {
				res++;
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}
		return res;
	}

}
