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
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.storage.StorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastLocalFileStorage implements StorageInterface {
	
	private final static Logger logger = LoggerFactory.getLogger(FastLocalFileStorage.class);
	
	final int BUFSIZ = 2048;
	final StoragePropertyHandler storagePropertyHandler = StoragePropertyHandler.getInstance();
	final String STORAGE_ZIP_DIR = storagePropertyHandler.getStorageZipDir();
	final String STORAGE_DIR = storagePropertyHandler.getStorageDir();
	final String STORAGE_PREPARED_DIR = storagePropertyHandler.getStoragePreparedDir();
	final LocalFileStorageCommons fsCommons = new LocalFileStorageCommons();
	
	@Override
	public InputStream getDataset(Dataset dataset) throws IOException {
		return fsCommons.getDatasetInputStream(dataset, STORAGE_ZIP_DIR);
	}
	
	@Override
	public void putDataset(Dataset dataset, InputStream is) throws IOException {
		fsCommons.putDataset(dataset, is, STORAGE_ZIP_DIR);
	}
	
	@Override
	public void deleteDataset(Dataset dataset) throws IOException {
		fsCommons.deleteDataset(dataset, STORAGE_ZIP_DIR);
		for (Datafile df : dataset.getDatafiles()) {
			File explodedFile = new File(STORAGE_DIR, df.getLocation());
			explodedFile.delete();
		}
	}
	
	@Override
	public boolean datasetExists(Dataset dataset) throws IOException {
		return fsCommons.datasetExists(dataset, STORAGE_ZIP_DIR);
	}
	
	@Override
	public InputStream getDatafile(Datafile datafile) throws FileNotFoundException {
		File file = new File(STORAGE_DIR, datafile.getLocation());
		if (!file.exists()) {
			throw new FileNotFoundException(file.getAbsolutePath());
		}
		return new BufferedInputStream(new FileInputStream(file));
	}
	
	@Override
	public long putDatafile(Datafile datafile, InputStream is) throws IOException {
		File file = new File(new File(STORAGE_DIR, datafile.getDataset().getLocation()), datafile.getName());
		fsCommons.writeInputStreamToFile(file, is);
		return file.length();
	};
	
	@Override
	public void prepareZipForRequest(Set<Dataset> datasets, Set<Datafile> datafiles, String zipName, boolean compress) throws IOException {
		logger.info(String.format("zipping %s datafiles", datafiles.size()));
        long startTime = System.currentTimeMillis();
        File zipFile = new File(STORAGE_PREPARED_DIR, zipName);
        prepareZipFileForUser(zipFile, datasets, datafiles, STORAGE_DIR, compress, null);
        long endTime = System.currentTimeMillis();
        logger.info("Time took to zip the files: " + (endTime - startTime));
	}
	
	@Override
	public void getPreparedZip(String zipName, OutputStream os, long offset) throws IOException {
		fsCommons.getPreparedZip(zipName, os, offset, STORAGE_PREPARED_DIR);
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
	
	private void prepareZipFileForUser(File zipFile, Set<Dataset> datasets, Set<Datafile> datafiles,
            String storageRootPath, boolean compress, Datafile newDatafile) {
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
            }
            for (Datafile df : datafiles) {
                addToZip("Datafile-" + df.getId(), zos, new File(storageRootPath, df.getLocation()).getAbsolutePath());
            }
            for (Dataset ds : datasets) {
            	for (Datafile df : ds.getDatafiles()) {
            		addToZip(String.format("Dataset-%s/Datafile-%s", ds.getId(), df.getId()), zos,
            				new File(storageRootPath, df.getLocation()).getAbsolutePath());
            	}
            }
            zos.close();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	/*
	 * newDatafile is a datafile that should be zipped with the rest of the dataset, but
	 * has not yet been added to it (because it hasn't been persisted to ICAT yet)
	 * Should be null if no such file is necessary.
	 */
	private void zipDataset(File zipFile, Dataset dataset,
            String storageRootPath, boolean compress, Datafile newDatafile) {
        if (dataset.getDatafiles().isEmpty() && newDatafile == null) {
            // Create empty file
            try {
                zipFile.createNewFile();
            } catch (IOException ex) {
                logger.error("writeZipFileFromDatafiles", ex);
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
            }
            for (Datafile file : dataset.getDatafiles()) {
            	logger.info("Adding file " + file.getName() + " to zip");
                addToZip(file.getName(), zos, new File(storageRootPath, 
                		file.getLocation()).getAbsolutePath());
            }
            if (newDatafile != null) {
            	logger.info("Adding file " + newDatafile.getName() + " to zip");
            	addToZip(newDatafile.getName(), zos, new File(storageRootPath, 
            			newDatafile.getLocation()).getAbsolutePath());
            }

            zos.close();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addToZip(String pathInsideOfZip, ZipOutputStream zos,
            String filePathOnDisk) {
        try {
            File fileOnDisk = new File(filePathOnDisk);
            FileInputStream fis = new FileInputStream(fileOnDisk);
            logger.info("Writing '" + pathInsideOfZip + "' to zip file");
            ZipEntry zipEntry = new ZipEntry(pathInsideOfZip);
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

	@Override
	public long putDatafile(String relativeLocation, InputStream is) throws IOException {
		File file = new File(STORAGE_DIR, relativeLocation);
		fsCommons.writeInputStreamToFile(file, is);
		return file.length();
	}

}
