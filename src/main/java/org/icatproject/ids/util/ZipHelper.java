package org.icatproject.ids.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

public class ZipHelper {

    private final static Logger logger = Logger.getLogger(ZipHelper.class.getName());

    public static void compressFileList(File zipFile, Set<String> fileSet, String relativePath,
            boolean compress) {
        long startTime = System.currentTimeMillis();
        writeZipFileFromStringFileList(zipFile, fileSet, relativePath, compress);
        long endTime = System.currentTimeMillis();
        logger.log(Level.INFO, "Time took to zip the files: " + (endTime - startTime));
    }

    public static void getAllFiles(File dir, List<File> fileList) {
        try {
            File[] files = dir.listFiles();
            for (File file : files) {
                fileList.add(file);
                if (file.isDirectory()) {
                    logger.log(Level.INFO, "directory:" + file.getCanonicalPath());
                    getAllFiles(file, fileList);
                } else {
                    logger.log(Level.INFO, "     file:" + file.getCanonicalPath());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeZipFileFromStringFileList(File zipFile, Set<String> fileSet,
            String relativePath, boolean compress) {

        if (fileSet.isEmpty()) {
            // Create empty file
            try {
                zipFile.createNewFile();
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
            return;
        }

        try {
            FileOutputStream fos = new FileOutputStream(zipFile);
            ZipOutputStream zos = new ZipOutputStream(fos);

            // set whether to compress the zip file or not
            if (compress == true) {
                zos.setMethod(ZipOutputStream.DEFLATED);
            } else {
                // using compress with level 0 instead of archive (STORED) because
                // STORED requires you to set CRC, size and compressed size
                // TODO: find efficient way of calculating CRC
                zos.setMethod(ZipOutputStream.DEFLATED);
                zos.setLevel(0);
                //zos.setMethod(ZipOutputStream.STORED);
            }

            for (String file : fileSet) {
                addToZip(zipFile, file, zos, relativePath);
            }

            zos.close();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void addToZip(File directoryToZip, String fileStr, ZipOutputStream zos,
            String relativePath) {
        try {
            File file = new File(fileStr);
            FileInputStream fis = new FileInputStream(file);
            // to the directory being zipped, so chop off the rest of the path
            String zipFilePath = file.getCanonicalPath().substring(relativePath.length(),
                    file.getCanonicalPath().length());
            if (zipFilePath.startsWith(File.separator)) {
                zipFilePath = zipFilePath.substring(1);
            }
            
            logger.log(Level.INFO, "Writing '" + zipFilePath + "' to zip file");
            ZipEntry zipEntry = new ZipEntry(zipFilePath);
            try {
                zos.putNextEntry(zipEntry);
                byte[] bytes = new byte[1024];
                int length;
                while ((length = fis.read(bytes)) >= 0) {
                    zos.write(bytes, 0, length);
                }
                zos.closeEntry();
                fis.close();
            } catch (ZipException ex) {
                logger.log(Level.INFO, "Skipping the file" + ex);
                fis.close();
            }
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
}
