package org.icatproject.ids.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.icatproject.Dataset;
import org.icatproject.ids.entity.IdsDataEntity;

public class RequestQueues {
	private final Map<IdsDataEntity, RequestedState> deferredOpsQueue;
	private final Set<Dataset> changing;
	private final Map<Dataset, Long> writeTimes;

	private static RequestQueues instance = null;

	private RequestQueues() {
		deferredOpsQueue = new HashMap<IdsDataEntity, RequestedState>();
		changing = new HashSet<Dataset>();
		writeTimes = new HashMap<Dataset, Long>();
	}

	public static RequestQueues getInstance() {
		if (instance == null)
			instance = new RequestQueues();
		return instance;
	}

	public Map<IdsDataEntity, RequestedState> getDeferredOpsQueue() {
		return deferredOpsQueue;
	}

	public Set<Dataset> getChanging() {
		return changing;
	}

	/*
	 * writeTimes map should be changed by RequestHelper only.
	 * This method should be invoked by other classes only as read-only.
	 * RequestHelper keeps the map synchronized with the IDS_WRITE_TIMES
	 * table in the IDS database.
	 * Clients wishing to add or update the write time for a particular
	 * dataset should invoke RequestHelper.setWriteTime
	 * and RequestHelper.removeWriteTime
	 */
	public Map<Dataset, Long> getWriteTimes() {
		return writeTimes;
	}
}
