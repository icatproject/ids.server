package org.icatproject.ids;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Checksum;

public class CheckedWithSizeInputStream extends FilterInputStream {
	private Checksum cksum;
	private long size;

	public CheckedWithSizeInputStream(InputStream in, Checksum cksum) {
		super(in);
		this.cksum = cksum;
	}

	public int read() throws IOException {
		int b = in.read();
		if (b != -1) {
			cksum.update(b);
			size++;
		}
		return b;
	}

	public int read(byte[] buf, int off, int len) throws IOException {
		len = in.read(buf, off, len);
		if (len != -1) {
			cksum.update(buf, off, len);
			size += len;
		}
		return len;
	}

	public long getSize() {
		return size;
	}
}
