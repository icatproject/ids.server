package org.icatproject.ids.storage.local;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.icatproject.ids.storage.StorageInterface;
import org.icatproject.ids.storage.StoragePropertyHandler;
import org.icatproject.ids.storage.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileStorage implements StorageInterface {

	@SuppressWarnings("unused")
	private final static Logger logger = LoggerFactory.getLogger(LocalFileStorage.class);
	private final int BUFSIZ = 2048;

	final StorageType storageType;
	final StoragePropertyHandler storagePropertyHandler = StoragePropertyHandler.getInstance();
	final String STORAGE_ZIP_DIR;
	final String STORAGE_DIR;
	final String STORAGE_PREPARED_DIR;

	public LocalFileStorage(StorageType type) {
		storageType = type;
		if (type == StorageType.FAST) {
			STORAGE_ZIP_DIR = storagePropertyHandler.getFastStorageZipDir();
			STORAGE_DIR = storagePropertyHandler.getFastStorageDir();
			STORAGE_PREPARED_DIR = storagePropertyHandler.getFastStoragePreparedDir();
		} else {
			STORAGE_ZIP_DIR = storagePropertyHandler.getSlowStorageZipDir();
			STORAGE_DIR = storagePropertyHandler.getSlowStorageDir();
			STORAGE_PREPARED_DIR = storagePropertyHandler.getSlowStoragePreparedDir();
		}
	}

	@Override
	public InputStream getDataset(String location) throws IOException {
		if (STORAGE_ZIP_DIR == null) {
			throw new UnsupportedOperationException(String.format(
					"Storage %s doesn't support Datasets", storageType));
		}
		File zippedDs = new File(new File(STORAGE_ZIP_DIR, location), "files.zip");
		if (!zippedDs.exists()) {
			throw new FileNotFoundException(zippedDs.getAbsolutePath());
		}
		return new BufferedInputStream(new FileInputStream(zippedDs));
	}

	@Override
	public void putDataset(String location, InputStream is) throws IOException {
		if (STORAGE_ZIP_DIR == null) {
			throw new UnsupportedOperationException(String.format(
					"Storage %s doesn't support Datasets", storageType));
		}
		File zippedDs = new File(new File(STORAGE_ZIP_DIR, location), "files.zip");
		File zippedDsDir = zippedDs.getParentFile();
		zippedDsDir.mkdirs();
		zippedDs.createNewFile();
		writeInputStreamToFile(zippedDs, is);
		is.close();
	}

	@Override
	public void deleteDataset(String location) throws IOException {
		if (STORAGE_ZIP_DIR == null) {
			throw new UnsupportedOperationException(String.format(
					"Storage %s doesn't support Datasets", storageType));
		}
		File zippedDs = new File(new File(STORAGE_ZIP_DIR, location), "files.zip");
		zippedDs.delete();
	}

	@Override
	public boolean datasetExists(String location) throws IOException {
		if (STORAGE_ZIP_DIR == null) {
			throw new UnsupportedOperationException(String.format(
					"Storage %s doesn't support Datasets", storageType));
		}
		File zippedDs = new File(new File(STORAGE_ZIP_DIR, location), "files.zip");
		return zippedDs.exists();
	}

	@Override
	public InputStream getDatafile(String location) throws FileNotFoundException {
		if (STORAGE_DIR == null) {
			throw new UnsupportedOperationException(String.format(
					"Storage %s doesn't support single Datafiles", storageType));
		}
		File file = new File(STORAGE_DIR, location);
		if (!file.exists()) {
			throw new FileNotFoundException(file.getAbsolutePath());
		}
		return new BufferedInputStream(new FileInputStream(file));
	}

	@Override
	public long putDatafile(String location, InputStream is) throws IOException {
		if (STORAGE_DIR == null) {
			throw new UnsupportedOperationException(String.format(
					"Storage %s doesn't support single Datafiles", storageType));
		}
		File file = new File(STORAGE_DIR, location);
		writeInputStreamToFile(file, is);
		return file.length();
	}

	@Override
	public void deleteDatafile(String location) throws IOException {
		if (STORAGE_DIR == null) {
			throw new UnsupportedOperationException(String.format(
					"Storage %s doesn't support single Datafiles", storageType));
		}
		File file = new File(STORAGE_DIR, location);
		file.delete();
	}

	@Override
	public boolean datafileExists(String location) throws IOException {
		if (STORAGE_DIR == null) {
			throw new UnsupportedOperationException(String.format(
					"Storage %s doesn't support single Datafiles", storageType));
		}
		File file = new File(STORAGE_DIR, location);
		return file.exists();
	}

	@Override
	public InputStream getPreparedZip(String zipName, long offset) throws IOException {
		if (STORAGE_PREPARED_DIR == null) {
			throw new UnsupportedOperationException(String.format(
					"Storage %s doesn't support preparation of zip files for users", storageType));
		}
		File preparedZip = new File(STORAGE_PREPARED_DIR, zipName);
		if (!preparedZip.exists()) {
			throw new FileNotFoundException(preparedZip.getAbsolutePath());
		}
		if (offset >= preparedZip.length()) {
			throw new IllegalArgumentException("Offset (" + offset
					+ " bytes) is larger than file size (" + preparedZip.length() + " bytes)");
		}
		InputStream res = new BufferedInputStream(new FileInputStream(preparedZip));
		IOUtils.skip(res, offset);
		return res;
	}

	@Override
	public void putPreparedZip(String zipName, InputStream is) throws IOException {
		if (STORAGE_PREPARED_DIR == null) {
			throw new UnsupportedOperationException(String.format(
					"Storage %s doesn't support preparation of zip files for users", storageType));
		}
		File file = new File(STORAGE_PREPARED_DIR, zipName);
		writeInputStreamToFile(file, is);
		is.close();
	}

	private void writeInputStreamToFile(File file, InputStream is) throws IOException {
		File fileDir = file.getParentFile();
		fileDir.mkdirs();

		BufferedInputStream bis = null;
		BufferedOutputStream bos = null;
		try {
			int bytesRead = 0;
			byte[] buffer = new byte[BUFSIZ];
			bis = new BufferedInputStream(is);
			bos = new BufferedOutputStream(new FileOutputStream(file));

			// write bytes to output stream
			while ((bytesRead = bis.read(buffer)) > 0) {
				bos.write(buffer, 0, bytesRead);
			}
		} finally {
			if (bos != null) {
				bos.close();
			}
		}
	}

}
