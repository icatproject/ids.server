package org.icatproject.ids.storage.local;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.icatproject.Dataset;
import org.icatproject.ids.storage.StorageInterface;
import org.icatproject.ids.util.StatusInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocalFileStorage implements StorageInterface {
	
	private final static Logger logger = LoggerFactory.getLogger(LocalFileStorage.class);
	
	private String storageDir;
	private String storageZipDir;
	private String storageArchiveDir;
    
    public LocalFileStorage(String storageDir, String storageZipDir, String storageArchiveDir) {
    	this.storageDir = storageDir;
    	this.storageZipDir = storageZipDir;
    	this.storageArchiveDir = storageArchiveDir;
    	logger.info("LocalFileStorage constructed");
    }
    
    @Override
    public StatusInfo restoreFromArchive(List<Dataset> datasets) {
    	try {
    		for (Dataset ds : datasets) {
    			logger.info("In restorer, processing dataset " + ds);
    	    	String location = ds.getLocation();
    	    	File basedir = new File(storageDir);
    	    	File dir = new File(storageDir, location);
    	    	File zipdir = new File(storageZipDir, location);
    	    	
	    		if (dir.exists()) {
	    			logger.info("Restorer omitted: files already present at " + location);
	    			return StatusInfo.COMPLETED;
	    		}
	    		final File archdir = new File(storageArchiveDir, location);
	    		//				logger.info("will restore from " + archdir.getAbsolutePath() + " that " + (archdir.exists() ? "exists" : "doesn't exist"));
	    		if (!archdir.exists()) {
	    			logger.error("No archive data to restore at " + location);
	    			return StatusInfo.NOT_FOUND;
	    		}
	    		File zipfiletmp = new File(zipdir, "files.zip.tmp");
	    		File zipfile = new File(zipdir, "files.zip");
	    		zipdir.mkdirs();
	    		FileUtils.copyFile(new File(archdir, "files.zip"), zipfiletmp);
	    		zipfiletmp.renameTo(zipfile);
	
	    		File tempdir = File.createTempFile("tmp", null, basedir);
	    		tempdir.delete();
	    		tempdir.mkdir();
	
	    		dir.getParentFile().mkdirs();
	    		unzip(new File(archdir, "files.zip"), tempdir);
	    		tempdir.renameTo(dir);
	    		logger.info("Restore of  " + location + " succesful");
    		}
    	} catch (final IOException e) {
    		logger.error("Restorer failed " + e.getMessage());
    		return StatusInfo.ERROR;
    	}
		return StatusInfo.COMPLETED;
    }
    
    private void unzip(File zip, File dir) throws IOException {
		final int BUFSIZ = 2048;
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

//    @Override
//    public HashSet<String> copyDatafiles(List<DatafileEntity> datafileList) {
//        HashSet<String> datafilePathSet = new HashSet<String>();
//        // Check if the file exists. if it does then it adds to the return array list
//        // and sets the status of datafile as found otherwise sets error in the status
//        // of datafile
//        for (DatafileEntity datafileEntity : datafileList) {
//            String filename = properties.getStorageDir() + File.separator
//                    + datafileEntity.getName();
//            logger.severe("filename: " + filename);
//            File df = new File(filename);
//            if (df.exists()) {
//                datafileEntity.setStatus(StatusInfo.COMPLETED.name());
//                datafilePathSet.add(filename);
//            } else {
//                datafileEntity.setStatus(StatusInfo.ERROR.name());
//            }
//        }
//        return datafilePathSet;
//    }

//    @Override
//    public String getStoragePath() {
//        return properties.getLocalStorageSystemPath();
//    }

//    @Override
//    public void clearUnusedFiles(int numberOfDays) {
//        // Doesn't delete the local cache files
//    }

}
