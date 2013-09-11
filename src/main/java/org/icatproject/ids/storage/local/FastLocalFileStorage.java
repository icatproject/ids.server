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
		
		zipDataset(zipFile, dataset, STORAGE_DIR, false, tmpDatafile);
		return file.length();
	};
	
	@Override
	public void prepareZipForRequest(Set<Dataset> datasets, Set<Datafile> datafiles, String zipName, boolean compress) throws Exception {
		logger.info(String.format("zipping %s datafiles", datafiles.size()));
        long startTime = System.currentTimeMillis();
        File zipFile = new File(STORAGE_PREPARED_DIR, zipName);
        // TODO make it conform to rules specified in interface specification
        prepareZipFileForUser(zipFile, datasets, datafiles, 
        		STORAGE_DIR, compress, null);
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
            String relativePath, boolean compress, Datafile newDatafile) {
		throw new UnsupportedOperationException("not yet implemented");
	}
	
	/**
	 * @param newDatafile a datafile that should be zipped with the rest of the dataset, but
	 * has not yet been added to it (because it hasn't been persisted to ICAT yet)
	 * Should be null if no such file is necessary.
	 */
	public static void zipDataset(File zipFile, Dataset dataset,
            String relativePath, boolean compress, Datafile newDatafile) {
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
                addToZip(zipFile, file.getName(), zos, new File(relativePath, 
                		dataset.getLocation()).getAbsolutePath());
            }
            if (newDatafile != null) {
            	logger.info("Adding file " + newDatafile.getName() + " to zip");
            	addToZip(zipFile, newDatafile.getName(), zos, new File(relativePath, 
            			dataset.getLocation()).getAbsolutePath());
            }

            zos.close();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void addToZip(File directoryToZip, String fileName, ZipOutputStream zos,
            String relativePath) {
        try {
            File file = new File(relativePath, fileName);
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
