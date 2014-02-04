package org.icatproject.ids.thread;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.icatproject.ids.DataSelection;
import org.icatproject.ids.DataSelection.DatafileInfo;
import org.icatproject.ids.DeferredOp;
import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prepare zips to download using /getData
 */
public class Preparer implements Runnable {

	public enum PreparerStatus {
		COMPLETE, INCOMPLETE, RESTORING, STARTED, WRITING
	}

	private static final int BUFSIZ = 1024;

	private final static Logger logger = LoggerFactory.getLogger(Preparer.class);
	private boolean compress;
	private DataSelection dataSelection;
	private FiniteStateMachine fsm;
	private MainStorageInterface mainStorage;
	private String message;
	private Path preparedDir;;
	private String preparedId;
	private PreparerStatus status = PreparerStatus.STARTED;
	private boolean twoLevel;
	private boolean zip;

	public Preparer(String preparedId, DataSelection dataSelection,
			PropertyHandler propertyHandler, FiniteStateMachine fsm, boolean compress, boolean zip) {
		this.preparedId = preparedId;
		this.dataSelection = dataSelection;
		mainStorage = propertyHandler.getMainStorage();
		twoLevel = propertyHandler.getArchiveStorage() != null;
		preparedDir = propertyHandler.getCacheDir().resolve("prepared");
		this.fsm = fsm;
		this.compress = compress;
		this.zip = zip;
	}

	public String getMessage() {
		return message;
	}

	public PreparerStatus getStatus() {
		return status;
	}

	@Override
	public void run() {
		logger.info("Starting preparer");

		if (twoLevel) {

			try {
				status = PreparerStatus.RESTORING;
				Collection<DsInfo> dsInfos = dataSelection.getDsInfo().values();
				Set<Long> emptyDs = dataSelection.getEmptyDatasets();
				boolean online = true;
				try {
					for (DsInfo dsInfo : dsInfos) {
						if (!emptyDs.contains(dsInfo.getDsId()) && !mainStorage.exists(dsInfo)) {
							online = false;
							fsm.queue(dsInfo, DeferredOp.RESTORE);
						}
					}
				} catch (IOException e) {
					message = e.getClass() + " " + e.getMessage();
					status = PreparerStatus.INCOMPLETE;
					return;
				}

				if (!online) {
					while (true) {
						online = true;
						Set<DsInfo> restoring = fsm.getRestoring();
						for (DsInfo dsInfo : dsInfos) {
							if (restoring.contains(dsInfo)) {
								logger.debug("Waiting for " + dsInfo + " to be restored");
								online = false;
								break;
							}
						}
						if (online) {
							break;
						}
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e) {
							// Ignore
						}
					}
				}
			} catch (Exception e) {
				message = e.getClass() + " " + e.getMessage();
				status = PreparerStatus.INCOMPLETE;
			}
		}
		status = PreparerStatus.WRITING;

		Path path = preparedDir.resolve(preparedId);
		try {
			byte[] bytes = new byte[BUFSIZ];
			Path tpath;
			if (zip) {
				tpath = Files.createTempFile(preparedDir, null, null);
				ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(
						Files.newOutputStream(tpath)));
				if (!compress) {
					zos.setLevel(0); // Otherwise use default compression
				}
				Map<Long, DsInfo> dsInfos = dataSelection.getDsInfo();
				for (DatafileInfo dfInfo : dataSelection.getDfInfo()) {
					DsInfo dsInfo = dsInfos.get(dfInfo.getDsId());
					zos.putNextEntry(new ZipEntry("ids/" + dsInfo.getFacilityName() + "/"
							+ dsInfo.getInvName() + "/" + dsInfo.getVisitId() + "/"
							+ dsInfo.getDsName() + "/" + dfInfo.getDfName()));
					InputStream stream = mainStorage.get(dfInfo.getDfLocation(),
							dfInfo.getCreator());
					int length;
					while ((length = stream.read(bytes)) >= 0) {
						zos.write(bytes, 0, length);
					}
					zos.closeEntry();
					stream.close();
				}
				zos.close();
			} else {
				tpath = Files.createTempDirectory(preparedDir, null);
				DatafileInfo dfInfo = dataSelection.getDfInfo().iterator().next();
				Path filePath = tpath.resolve(dfInfo.getDfName());
				Files.createDirectories(filePath.getParent());
				OutputStream output = new BufferedOutputStream(Files.newOutputStream(filePath));
				InputStream stream = mainStorage.get(dfInfo.getDfLocation(), dfInfo.getCreator());
				int length;
				while ((length = stream.read(bytes)) >= 0) {
					output.write(bytes, 0, length);
				}
				stream.close();
				output.close();
			}
			Files.move(tpath, path, StandardCopyOption.ATOMIC_MOVE);

			status = PreparerStatus.COMPLETE;
			logger.debug(path + " written");
		} catch (IOException e) {
			message = e.getClass() + " " + e.getMessage();
			status = PreparerStatus.INCOMPLETE;
		}

	}

	public boolean using(DsInfo dsInfo) {
		return dataSelection.getDsInfo().containsKey(dsInfo.getDsId());
	}

}
