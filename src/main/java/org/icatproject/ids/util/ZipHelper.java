package org.icatproject.ids.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipHelper {

	private final static Logger logger = LoggerFactory.getLogger(ZipHelper.class);

	// public static InputStream zipDataset(Dataset dataset, boolean compress,
	// StorageInterface storageInterface) throws IOException {
	// File tmpZipFile = new File(PropertyHandler.getInstance().getTmpDir(), new Long(
	// System.currentTimeMillis()).toString()
	// + ".zip");
	// if (dataset.getDatafiles().isEmpty()) {
	// // Create empty file
	// tmpZipFile.createNewFile();
	// return new ZipInputStream(new FileInputStream(tmpZipFile));
	// }
	// ZipOutputStream zos = null;
	// try {
	// zos = new ZipOutputStream(new FileOutputStream(tmpZipFile));
	// if (!compress) {
	// zos.setLevel(0); // Otherwise use default compression
	// }
	// for (Datafile df : dataset.getDatafiles()) {
	// logger.info("Adding file " + df.getName() + " to zip");
	// addToZip(df.getName(), zos, storageInterface.getDatafile(df.getLocation()));
	// }
	// } finally {
	// if (zos != null) {
	// try {
	// zos.close();
	// } catch (Exception e) {
	// logger.warn("Couldn't close the stream to " + tmpZipFile.getAbsolutePath());
	// }
	// }
	// }
	// return new FileInputStream(tmpZipFile);
	// }

	// public static InputStream prepareZipForUserRequest(IdsRequestEntity request,
	// StorageInterface storageInterface) throws IOException {
	// return prepareTemporaryZip(request.getPreparedId() + ".zip", request.getIcatDatasets(),
	// request.getIcatDatafiles(), request.isCompress(), storageInterface);
	// }

	// public static InputStream prepareTemporaryZip(String zipName, Collection<Dataset> datasets,
	// Collection<Datafile> datafiles, boolean compress, StorageInterface storageInterface)
	// throws IOException {
	// File tmpZipFile = new File(PropertyHandler.getInstance().getTmpDir(), zipName);
	// ZipOutputStream zos = null;
	// try {
	// zos = new ZipOutputStream(new FileOutputStream(tmpZipFile));
	//
	// if (!compress) {
	// zos.setLevel(0); // Otherwise use default compression
	// }
	// for (Datafile df : datafiles) {
	// addToZip("Datafile-" + df.getId(), zos,
	// storageInterface.getDatafile(df.getLocation()));
	// }
	// for (Dataset ds : datasets) {
	// for (Datafile df : ds.getDatafiles()) {
	// addToZip(String.format("Dataset-%s/Datafile-%s", ds.getId(), df.getId()), zos,
	// storageInterface.getDatafile(df.getLocation()));
	// }
	// }
	// } finally {
	// if (zos != null) {
	// try {
	// zos.close();
	// } catch (Exception e) {
	// logger.warn("Couldn't close the stream to " + tmpZipFile.getAbsolutePath());
	// }
	// }
	// }
	// return new FileInputStream(tmpZipFile);
	// }

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
