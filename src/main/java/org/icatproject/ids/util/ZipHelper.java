package org.icatproject.ids.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import org.icatproject.Datafile;
import org.icatproject.ids.entity.IdsRequestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipHelper {

    private final static Logger logger = LoggerFactory.getLogger(ZipHelper.class);

    public static void compressFileList(IdsRequestEntity request
    		/*, Set<String> fileSet, String relativePath, boolean compress*/) {
//    	logger.info(String.format("zipping %s datasets and %s datafiles, total %s icatDatafiles",
//    			request.getDatasets().size(), request.getDatafiles().size(),
//    			request.getIcatDatafiles().size()));
//        long startTime = System.currentTimeMillis();
//
//        writeZipFileFromStringFileList(zipFile, request.getIcatDatafiles(), 
//        		PropertyHandler.getInstance().getStorageDir(), request.isCompress());
//        long endTime = System.currentTimeMillis();
//        logger.info("Time took to zip the files: " + (endTime - startTime));
    }

//    public static void getAllFiles(File dir, List<File> fileList) {
//        try {
//            File[] files = dir.listFiles();
//            for (File file : files) {
//                fileList.add(file);
//                if (file.isDirectory()) {
//                    logger.log(Level.INFO, "directory:" + file.getCanonicalPath());
//                    getAllFiles(file, fileList);
//                } else {
//                    logger.log(Level.INFO, "     file:" + file.getCanonicalPath());
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public static void writeZipFileFromStringFileList(File zipFile, Collection<Datafile> fileSet,
            String relativePath, boolean compress) {
        logger.info("Will add " + fileSet.size() + " files to zip");
        if (fileSet.isEmpty()) {
            // Create empty file
            try {
                zipFile.createNewFile();
            } catch (IOException ex) {
                logger.error("writeZipFileFromStringFileList", ex);
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
            for (Datafile file : fileSet) {
            	logger.info("Adding file " + file.getLocation() + " to zip");
                addToZip(zipFile, file.getLocation(), zos, relativePath);
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
            File file = new File(relativePath, fileStr);
            FileInputStream fis = new FileInputStream(file);
            // to the directory being zipped, so chop off the rest of the path
            String zipFilePath = file.getCanonicalPath().substring(relativePath.length(),
                    file.getCanonicalPath().length());
            if (zipFilePath.startsWith(File.separator)) {
                zipFilePath = zipFilePath.substring(1);
            }
            
            logger.info("Writing '" + zipFilePath + "' to zip file");
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
                logger.info("Skipping the file" + ex);
                fis.close();
            }
        } catch (IOException ex) {
            logger.error("addToZip", ex);
        }
    }
}
