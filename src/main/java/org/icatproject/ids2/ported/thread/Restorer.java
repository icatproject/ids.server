package org.icatproject.ids2.ported.thread;

import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.icatproject.ids.storage.StorageFactory;
import org.icatproject.ids.storage.StorageInterface;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids2.ported.Ids2DatasetEntity;
import org.icatproject.ids2.ported.RequestHelper;
import org.icatproject.ids2.ported.RequestQueues;
import org.icatproject.ids2.ported.RequestedState;

//copies files from the archive to the local storage (in zip), also creates an unzipped copy
public class Restorer implements Runnable {

	private final static Logger logger = Logger.getLogger(ProcessQueue.class.getName());

	private Ids2DatasetEntity ds;
	private RequestQueues requestQueues;
	private RequestHelper requestHelper;

	public Restorer(Ids2DatasetEntity ds, RequestHelper requestHelper) {
		this.ds = ds;
		this.requestQueues = RequestQueues.getInstance();
		this.requestHelper = requestHelper;
	}
	
	@Override
	public void run() {
		logger.info("starting restorer");
		StorageInterface storageInterface = StorageFactory.getInstance().createStorageInterface();
		StatusInfo resultingStatus = storageInterface.restoreFromArchive(ds);
		Map<Ids2DatasetEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
		Set<Ids2DatasetEntity> changing = requestQueues.getChanging();
		synchronized (deferredOpsQueue) {
			requestHelper.setDatasetStatus(ds, resultingStatus);
			changing.remove(ds);
		}
	}

//	@Override
//	public void run() {
//		final File basedir = new File(storageDir);
//		final File dir = new File(storageDir, ds.getLocation());
//		final File zipdir = new File(storageZipDir, ds.getLocation());
//		Map<Ids2DatasetEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
//		Set<Ids2DatasetEntity> changing = requestQueues.getChanging();
//
//		try {
//			if (dir.exists()) {
//				logger.info("Restorer omitted: files already present at " + ds.getLocation());
//			} else {
//				final File archdir = new File(storageArchiveDir, ds.getLocation());
//				logger.info("will restore from " + archdir.getAbsolutePath() + " that " + (archdir.exists() ? "exists" : "doesn't exist"));
//				if (archdir.exists()) {
//					File zipfiletmp = new File(zipdir, "files.zip.tmp");
//					File zipfile = new File(zipdir, "files.zip");
//					zipdir.mkdirs();
//					FileUtils.copyFile(new File(archdir, "files.zip"), zipfiletmp);
//					zipfiletmp.renameTo(zipfile);
//					
//					File tempdir = File.createTempFile("tmp", null, basedir);
//					tempdir.delete();
//					tempdir.mkdir();
//					
//					dir.getParentFile().mkdirs();
//					unzip(new File(archdir, "files.zip"), tempdir);
//					tempdir.renameTo(dir);
//					logger.info("Restore of  " + ds.getLocation() + " succesful");
//				} else {
//					logger.info("No archive data to restore at " + ds.getLocation());
//				}
//			}
//
//		} catch (final IOException e) {
//			logger.severe("Restorer failed " + e.getMessage());
//		} finally {
//			synchronized (deferredOpsQueue) {
//				requestHelper.setDatasetStatus(ds, StatusInfo.COMPLETED);
//				changing.remove(ds);
//			}
//		}
//	}
//
//	private void unzip(File zip, File dir) throws IOException {
//		final int BUFSIZ = 2048;
//		final ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(zip)));
//		ZipEntry entry;
//		while ((entry = zis.getNextEntry()) != null) {
//			final String name = entry.getName();
//			final File file = new File(dir, name);
//			System.out.println("Found " + name);
//			if (entry.isDirectory()) {
//				file.mkdir();
//			} else {
//				int count;
//				final byte data[] = new byte[BUFSIZ];
//				final BufferedOutputStream dest = new BufferedOutputStream(new FileOutputStream(file), BUFSIZ);
//				while ((count = zis.read(data, 0, BUFSIZ)) != -1) {
//					dest.write(data, 0, count);
//				}
//				dest.close();
//			}
//		}
//		zis.close();
//	}

}