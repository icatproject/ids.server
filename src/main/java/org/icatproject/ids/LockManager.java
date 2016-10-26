package org.icatproject.ids;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.ejb.DependsOn;
import javax.ejb.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.icatproject.ids.plugin.DsInfo;


@Singleton
@DependsOn("LoggingConfigurator")
public class LockManager {

	public enum LockType {
		SHARED, EXCLUSIVE
	}

	public class AlreadyLockedException extends IOException {
		public AlreadyLockedException() {
			super("Resource is already locked.");
		}
	}

	private class LockEntry {
		public final Long id;
		public final LockType type;
		private int count;
		public LockEntry(Long id, LockType type) {
			this.id = id;
			this.type = type;
			this.count = 0;
			locks.put(id, this);
		}
		public void inc() {
			count += 1;
		}
		public void dec() {
			assert count > 0;
			count -= 1;
			if (count == 0) {
				locks.remove(id);
			}
		}
	}

	public class Lock implements AutoCloseable {
		private final Long id;
		private boolean isValid;
		public Lock(Long id) {
			this.id = id;
			this.isValid = true;
		}
		public void release() throws IOException {
			synchronized (locks) {
				if (isValid) {
					locks.get(id).dec();
					isValid = false;
					logger.debug("Released a lock on {}.", 
						     id);
				}
			}
		}
		public void close() throws IOException {
			release();
		}
	}

	public class LockCollection implements AutoCloseable {
		private ArrayList<Lock> locklist;
		public LockCollection() {
			locklist = new ArrayList<>();
		}
		public void add(Lock l) {
			locklist.add(l);
		}
		public void release() throws IOException {
			IOException exc = null;
			for (Lock l: locklist) {
				try {
					l.release();
				} catch (IOException e) {
					if (exc == null) { exc = e; }
				}
			}
			if (exc != null) {
				throw exc;
			}
		}
		public void close() throws IOException {
			release();
		}
	}

	private static Logger logger 
		= LoggerFactory.getLogger(LockManager.class);
	private Map<Long, LockEntry> locks = new HashMap<>();

	@PostConstruct
	private void init() {
		logger.debug("LockManager initialized.");
	}

	public Lock lock(DsInfo ds, LockType type) throws IOException {
		Long id = ds.getDsId();
		assert id != null;
		synchronized (locks) {
			LockEntry le = locks.get(id);
			if (le == null) {
				le = new LockEntry(id, type);
			}
			else {
				if (type == LockType.EXCLUSIVE || 
				    le.type == LockType.EXCLUSIVE) {
					throw new AlreadyLockedException();
				}
			}
			le.inc();
			logger.debug("Acquired a {} lock on {}.", type, id);
			return new Lock(id);
		}
	}

	public LockCollection lock(Collection<DsInfo> datasets, 
				   LockType type) throws IOException {
		LockCollection lockCollection = new LockCollection();
		try {
			for (DsInfo ds: datasets) {
				lockCollection.add(lock(ds, type));
			}
		} catch (IOException e) {
			lockCollection.release();
			throw e;
		}
		return lockCollection;
	}

}

// Local Variables:
// c-basic-offset: 8
// End:
