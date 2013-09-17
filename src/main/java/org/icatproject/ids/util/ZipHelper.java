package org.icatproject.ids.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.entity.IdsDatafileEntity;
import org.icatproject.ids.entity.IdsDatasetEntity;
import org.icatproject.ids.entity.IdsRequestEntity;
import org.icatproject.ids.storage.StorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipHelper {

	private final static Logger logger = LoggerFactory.getLogger(ZipHelper.class);

	public static InputStream zipDataset(Dataset dataset, boolean compress, StorageInterface storageInterface)
			throws IOException {
		File tmpZipFile = new File(PropertyHandler.getInstance().getTmpDir(),
				new Long(System.currentTimeMillis()).toString() + ".zip");
		if (dataset.getDatafiles().isEmpty()) {
			// Create empty file
			tmpZipFile.createNewFile();
			return new ZipInputStream(new FileInputStream(tmpZipFile));
		}
		ZipOutputStream zos = null;
		try {
			zos = new ZipOutputStream(new FileOutputStream(tmpZipFile));

			// set whether to compress the zip file or not
			if (compress == true) {
				zos.setMethod(ZipOutputStream.DEFLATED);
			} else {
				// using compress with level 0 instead of archive (STORED)
				// because
				// STORED requires you to set CRC, size and compressed size
				// TODO: find efficient way of calculating CRC
				zos.setMethod(ZipOutputStream.DEFLATED);
				zos.setLevel(0);
			}
			for (Datafile df : dataset.getDatafiles()) {
				logger.info("Adding file " + df.getName() + " to zip");
				addToZip(df.getName(), zos, storageInterface.getDatafile(df));
			}
		} finally {
			if (zos != null) {
				try {
					zos.close();
				} catch (Exception e) {
					logger.warn("Couldn't close the stream to " + tmpZipFile.getAbsolutePath());
				}
			}
		}
		return new FileInputStream(tmpZipFile);
	}

	public static InputStream prepareZipForUserRequest(IdsRequestEntity request, StorageInterface storageInterface)
			throws IOException {
		File tmpZipFile = new File(PropertyHandler.getInstance().getTmpDir(),
				request.getPreparedId() + ".zip");		
        ZipOutputStream zos = null;
        try {
	        zos = new ZipOutputStream(new FileOutputStream(tmpZipFile));
	
	        // set whether to compress the zip file or not
	        if (request.isCompress() == true) {
	            zos.setMethod(ZipOutputStream.DEFLATED);
	        } else {
	            // using compress with level 0 instead of archive (STORED) because
	            // STORED requires you to set CRC, size and compressed size
	            // TODO: find efficient way of calculating CRC
	            zos.setMethod(ZipOutputStream.DEFLATED);
	            zos.setLevel(0);
	        }
	        for (IdsDatafileEntity idsDf : request.getDatafiles()) {
	        	Datafile df = idsDf.getIcatDatafile();
	            addToZip("Datafile-" + df.getId(), zos, storageInterface.getDatafile(df));
	        }
	        for (IdsDatasetEntity idsDs : request.getDatasets()) {
	        	Dataset ds = idsDs.getIcatDataset();
	        	for (Datafile df : ds.getDatafiles()) {
	        		addToZip(String.format("Dataset-%s/Datafile-%s", ds.getId(), df.getId()), zos,
	        				storageInterface.getDatafile(df));
	        	}
	        }
        } finally {
			if (zos != null) {
				try {
					zos.close();
				} catch (Exception e) {
					logger.warn("Couldn't close the stream to " + tmpZipFile.getAbsolutePath());
				}
			}
		}
		return new FileInputStream(tmpZipFile);
	}

	private static void addToZip(String pathInsideOfZip, ZipOutputStream zos, InputStream datafileIn)
			throws IOException {
		logger.info("Writing '" + pathInsideOfZip + "' to zip file");
		ZipEntry zipEntry = new ZipEntry(pathInsideOfZip);
		try {
			zos.putNextEntry(zipEntry);
			byte[] bytes = new byte[1024];
			int length;
			while ((length = datafileIn.read(bytes)) >= 0) {
				zos.write(bytes, 0, length);
			}
			zos.closeEntry();
			datafileIn.close();
		} catch (ZipException ex) {
			logger.info("Skipping the file" + ex);
			datafileIn.close();
		}
	}

}
