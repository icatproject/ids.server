package org.icatproject.ids.storage.local;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.icatproject.Dataset;
import org.icatproject.ids.storage.StorageInterface;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids2.ported.SimpleDirectoryWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileStorage /*implements StorageInterface*/ {

	private final static Logger logger = LoggerFactory.getLogger(LocalFileStorage.class);

	private String storageDir;
	private String storageZipDir;
	private String storageArchiveDir;
	final int BUFSIZ = 2048;

	public LocalFileStorage(String storageDir, String storageZipDir, String storageArchiveDir) {
		this.storageDir = storageDir;
		this.storageZipDir = storageZipDir;
		this.storageArchiveDir = storageArchiveDir;
		logger.info("LocalFileStorage constructed");
	}

	/*@Override*/
	public StatusInfo restoreFromArchive(Dataset ds) {
		try {
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
			// logger.info("will restore from " + archdir.getAbsolutePath() +
			// " that " + (archdir.exists() ? "exists" : "doesn't exist"));
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
		} catch (final IOException e) {
			logger.error("Restorer failed " + e.getMessage());
			return StatusInfo.ERROR;
		}
		return StatusInfo.COMPLETED;
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

	/*@Override*/
	public StatusInfo writeToArchive(Dataset ds) {
		try {
			String location = ds.getLocation();
			final File dir = new File(storageDir, location);
			final File archdir = new File(storageArchiveDir, location);
			final File zipdir = new File(storageZipDir, location);

			// if the file doesn't exist locally, we also delete it from the
			// archive
			if (!dir.exists() || dir.list().length == 0) {
				logger.info("No files present at " + location + " - archive deleted");
				FileUtils.deleteDirectory(archdir);
				FileUtils.deleteDirectory(zipdir);
			} else {
				archdir.mkdirs();
				File zipfiletmp = new File(zipdir, "files.zip.tmp");
				File zipfile = new File(zipdir, "files.zip");
				if (!zipfile.exists()) {
					zipdir.mkdirs();
					zip(new FileOutputStream(zipfiletmp), dir);
					zipfiletmp.renameTo(zipfile);
				}
				FileUtils.copyFileToDirectory(zipfile, archdir);
				logger.info("Write to archive of  " + location + " succesful");
			}
		} catch (final IOException e) {
			logger.error("Writer failed " + e.getMessage());
			return StatusInfo.ERROR;
		}
		return StatusInfo.COMPLETED;
	}
	
	// zips files from "dir" to the output "stream"
		private void zip(OutputStream stream, File dir) throws IOException {
			ZipOutputStream os = null;
			BufferedInputStream is = null;
			logger.info("Stream files from " + dir);
			try {
				os = new ZipOutputStream(new BufferedOutputStream(stream, BUFSIZ));
				final byte data[] = new byte[BUFSIZ];

				final SimpleDirectoryWalker sdw = new SimpleDirectoryWalker();
				final List<File> files = sdw.walk(dir);

				int startPos = 0;
				for (final File file : files) {
					final String name = file.getPath();
					if (startPos == 0) {
						startPos = name.length() + 1;
					} else {
						ZipEntry entry;
						if (file.isFile()) {
							entry = new ZipEntry(name.substring(startPos));
						} else {
							entry = new ZipEntry(name.substring(startPos) + "/");
						}
						os.putNextEntry(entry);
						if (file.isFile()) {
							int count;
							is = new BufferedInputStream(new FileInputStream(file), BUFSIZ);
							while ((count = is.read(data, 0, BUFSIZ)) != -1) {
								os.write(data, 0, count);
							}
							is.close();
						}
					}
				}
			} finally {
				if (os != null) {
					os.close();
				}
				if (is != null) {
					is.close();
				}
			}
		}

}
