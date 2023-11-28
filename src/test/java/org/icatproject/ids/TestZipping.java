package org.icatproject.ids;

import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertEquals;

public class TestZipping {

    @Test
    public void testDuplicates() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(baos));

        // zos.setLevel(0);
        List<String> in = new ArrayList<>();

        for (String entryName : Arrays.asList("abcd/qa", "abcd/qw", "abcd/qw", "abcd/qb", "abcd/qc", "abcd/qw")) {

            try {
                zos.putNextEntry(new ZipEntry(entryName));
                byte[] bytes = entryName.getBytes();

                zos.write(bytes, 0, bytes.length);
                zos.write(bytes, 0, bytes.length);
                in.add(entryName);
            } catch (ZipException e) {
                // OK
            }
            zos.closeEntry();
        }
        zos.close();

        assertEquals(4, in.size());

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ZipInputStream zis = new ZipInputStream(bais);

        int off = 0;
        ZipEntry ze = zis.getNextEntry();
        while (ze != null) {
            String entryName = ze.getName();

            assertEquals(in.get(off), entryName);
            byte[] bytes = new byte[40];
            int n;
            while ((n = zis.read(bytes, 0, 40)) >= 0) {
                String s = new String(bytes, 0, n);
                assertEquals(in.get(off) + in.get(off), s);
            }
            off++;
            ze = zis.getNextEntry();
        }
        zis.close();

    }

}