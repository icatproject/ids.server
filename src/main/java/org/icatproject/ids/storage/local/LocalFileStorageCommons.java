package org.icatproject.ids.storage.local;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.icatproject.Dataset;
import org.icatproject.ids.webservice.exceptions.BadRequestException;

public class LocalFileStorageCommons {
	
	final int BUFSIZ = 2048;
	
	public boolean datasetExists(Dataset dataset, String storageDir) throws Exception {
		File zippedDs = new File(new File(storageDir, dataset.getLocation()), "files.zip");
		return zippedDs.exists();
	}
	
	public void getDataset(Dataset dataset, OutputStream os, String storageDir) throws Exception {
		File zippedDs = new File(new File(storageDir, dataset.getLocation()), "files.zip");
		writeFileToOutputStream(zippedDs, os, 0L);
	}
	
	public void getPreparedZip(String zipName, OutputStream os, long offset, String preparedDir) throws Exception {
		File preparedZip = new File(preparedDir, zipName);
		writeFileToOutputStream(preparedZip, os, offset);
	}
	
	private void writeFileToOutputStream(File file, OutputStream os, Long offset) throws Exception {
		if (!file.exists()) {
			throw new FileNotFoundException(file.getAbsolutePath());
		}
		if (offset >= file.length()) {
			throw new IllegalArgumentException("Offset (" + offset + " bytes) is larger than file size ("
					+ file.length() + " bytes)");
		}
		BufferedInputStream bis = null;
		BufferedOutputStream bos = null;
		try {
			int bytesRead = 0;
			byte[] buffer = new byte[BUFSIZ];
			bis = new BufferedInputStream(new FileInputStream(file));
			bos = new BufferedOutputStream(os);

			// apply offset to stream
			if (offset > 0) {
				bis.skip(offset);
			}

			// write bytes to output stream
			while ((bytesRead = bis.read(buffer)) > 0) {
				bos.write(buffer, 0, bytesRead);
			}
		} finally {
			if (bis != null) {
				bis.close();
			}
			if (bos != null) {
				bos.close();
			}
		}
	}
	
	public InputStream getDatasetInputStream(Dataset dataset, String storageDir) throws Exception {
		File zippedDs = new File(new File(storageDir, dataset.getLocation()), "files.zip");
		if (!zippedDs.exists()) {
			throw new FileNotFoundException(zippedDs.getAbsolutePath());
		}
		return new BufferedInputStream(new FileInputStream(zippedDs));
	}
	
	public void putDataset(Dataset dataset, InputStream is, String storageDir) throws Exception {
		File zippedDs = new File(new File(storageDir, dataset.getLocation()), "files.zip");
		File zippedDsDir = zippedDs.getParentFile();
		zippedDsDir.mkdirs();
		zippedDs.createNewFile();
		writeInputStreamToFile(zippedDs, is);
	}
	
	public void writeInputStreamToFile(File file, InputStream is) throws Exception {
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
			if (bis != null) {
				bis.close();
			}
			if (bos != null) {
				bos.close();
			}
		}
	}
	
	public void deleteDataset(Dataset dataset, String storageDir) throws Exception {
		File zippedDs = new File(new File(storageDir, dataset.getLocation()), "files.zip");
		zippedDs.delete();
	}

}
