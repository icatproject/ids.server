package org.icatproject.ids;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A FilterOutputStream than transmits a fixed number of bytes following some
 * offset.
 */
public class RangeOutputStream extends FilterOutputStream {

    private long min;

    private long pos;

    private long max;

    /**
     * Construct a RangeOutputStream that wraps an OutputStream and transmits a
     * fixed number of bytes following some offset.
     *
     * @param os    the stream to wrap
     * @param min   the number of bytes to skip. It may larger than the number of
     *              bytes in the stream.
     * @param count restrict to transmit only this number of bytes. The value null
     *              indicates that all bytes after the offset should be
     *              transmitted.
     */
    public RangeOutputStream(OutputStream os, long min, Long count) {
        super(os);
        this.min = min;
        this.max = count == null ? Long.MAX_VALUE : min + count;
        if (min < 0)
            throw new IllegalArgumentException(
                    "min must be a non-negative long");
        if (count != null && count < 0)
            throw new IllegalArgumentException(
                    "count must be a non-negative long");
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.FilterOutputStream#write(int)
     */
    @Override
    public void write(int b) throws IOException {
        if (pos >= min && pos < max) {
            out.write(b);
        }
        pos++;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.FilterOutputStream#write(byte[], int, int)
     */
    @Override
    public void write(byte[] b, int off, final int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0)
                || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        if (pos + len - 1 >= min && pos < max) {
            int newLen = len;
            if (newLen > max - pos) {
                newLen = (int) (max - pos);
            }
            if (pos < min) {
                off = (int) (off + min - pos);
                newLen = (int) (newLen + pos - min);
            }
            out.write(b, off, newLen);
        }
        pos += len;
    }

}
