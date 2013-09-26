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

	public Map<Dataset, Long> getWriteTimes() {
		return writeTimes;
	}
}
