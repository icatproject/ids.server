package org.icatproject.ids.storage.local;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.icatproject.Dataset;
import org.icatproject.ids.storage.StorageInterface;

public class FastLocalFileStorage implements StorageInterface {
	
	final int BUFSIZ = 2048;
	final String STORAGE_ZIP_DIR = "/home/wojtek/icat/icatzipdata/";
	final String STORAGE_DIR = "/home/wojtek/icat/icatdata/";
	final String STORAGE_PREPARED_DIR = "/home/wojtek/icat/icatprepareddata/";
	final LocalFileStorageCommons fsCommons = new LocalFileStorageCommons();
	
	@Override
	public boolean datasetExists(Dataset dataset) throws Exception {
		return fsCommons.datasetExists(dataset, STORAGE_ZIP_DIR);
	}
	
	@Override
	public void getDataset(Dataset dataset, OutputStream os) throws Exception {
		fsCommons.getDataset(dataset, os, STORAGE_ZIP_DIR);
	}
	
	@Override
	public InputStream getDatasetInputStream(Dataset dataset) throws Exception {
		return fsCommons.getDatasetInputStream(dataset, STORAGE_ZIP_DIR);
	}
	
	@Override
	public void putDataset(Dataset dataset, InputStream is) throws Exception {
		fsCommons.putDataset(dataset, is, STORAGE_ZIP_DIR);
		
		// unzip the dataset
		File tempdir = File.createTempFile("tmp", null, new File(STORAGE_DIR));
		File dir = new File(STORAGE_DIR, dataset.getLocation());
		File archdir = new File(STORAGE_ZIP_DIR, dataset.getLocation());
		tempdir.delete();
		tempdir.mkdir();
		dir.getParentFile().mkdirs();
		unzip(new File(archdir, "files.zip"), tempdir);
		tempdir.renameTo(dir);
		
	}
	
	private void unzip(File zip, File dir) throws IOException {
		final ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(zip)));
		ZipEntry entry;
		while ((entry = zis.getNextEntry()) != null) {
			final String name = entry.getName();
			final File file = new File(dir, name);
			System.out.println("Found " + name);
			if (entry.isDirectory()) {
				file.mkdir();
			} else {
				int count;
				final byte data[] = new byte[BUFSIZ];
				final BufferedOutputStream dest = new BufferedOutputStream(new FileOutputStream(file), BUFSIZ);
				while ((count = zis.read(data, 0, BUFSIZ)) != -1) {
					dest.write(data, 0, count);
				}
				dest.close();
			}
		}
		zis.close();
	}

}
