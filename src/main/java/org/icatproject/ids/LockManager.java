package org.icatproject.ids;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.ejb.DependsOn;
import javax.ejb.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

	private static Logger logger 
		= LoggerFactory.getLogger(LockManager.class);
	private Map<Long, LockEntry> locks = new HashMap<>();

	@PostConstruct
	private void init() {
		logger.debug("LockManager initialized.");
	}

	public Lock lock(Long id, LockType type) throws IOException {
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

}

// Local Variables:
// c-basic-offset: 8
// End:
