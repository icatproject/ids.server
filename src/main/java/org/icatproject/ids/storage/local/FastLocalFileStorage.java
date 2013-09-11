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
import java.util.Collection;
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
	
	@Override
	public void deleteDataset(Dataset dataset) throws Exception {
		fsCommons.deleteDataset(dataset, STORAGE_ZIP_DIR);
		for (Datafile df : dataset.getDatafiles()) {
			File explodedFile = new File(STORAGE_DIR, df.getLocation());
			explodedFile.delete();
		}
	}
	
	public long putDatafile(String name, InputStream is, Dataset dataset) throws Exception {
		File file = new File(new File(STORAGE_DIR, dataset.getLocation()), name);
		File zipFile = new File(new File(STORAGE_ZIP_DIR, dataset.getLocation()), "files.zip");
		if (!zipFile.exists()) {
			logger.warn("Couldn't find zipped DS: " + zipFile.getAbsolutePath() + " in Fast.putDatafile");
			throw new FileNotFoundException(zipFile.getAbsolutePath());
		}
		fsCommons.writeInputStreamToFile(file, is);
		// TODO write new file to zip; what should be new file's location in zip?
		
		Datafile tmpDatafile = new Datafile();
		tmpDatafile.setName(name);
		tmpDatafile.setDataset(dataset);
		tmpDatafile.setLocation(new File(dataset.getLocation(), name).getPath());
		
		zipDataset(zipFile, dataset, STORAGE_DIR, false, tmpDatafile);
		return file.length();
	};
	
	@Override
	public void prepareZipForRequest(Set<Dataset> datasets, Set<Datafile> datafiles, String zipName, boolean compress) throws Exception {
		logger.info(String.format("zipping %s datafiles", datafiles.size()));
        long startTime = System.currentTimeMillis();
        File zipFile = new File(STORAGE_PREPARED_DIR, zipName);
        prepareZipFileForUser(zipFile, datasets, datafiles, STORAGE_DIR, compress, null);
        long endTime = System.currentTimeMillis();
        logger.info("Time took to zip the files: " + (endTime - startTime));
	}
	
	@Override
	public void getPreparedZip(String zipName, OutputStream os, long offset) throws Exception {
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
                //zos.setMethod(ZipOutputStream.STORED);
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
	
	/**
	 * @param newDatafile a datafile that should be zipped with the rest of the dataset, but
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
                //zos.setMethod(ZipOutputStream.STORED);
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

}
