package org.icatproject.ids2.ported;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.icatproject.Dataset;
import org.icatproject.ids2.ported.entity.Ids2DataEntity;

public class RequestQueues {
	private final Map<Ids2DataEntity, RequestedState> deferredOpsQueue;
	private final Set<Dataset> changing;
	private final Map<Dataset, Long> writeTimes;
	
	private static RequestQueues instance = null;
	
	private RequestQueues() {
		deferredOpsQueue = new HashMap<Ids2DataEntity, RequestedState>();
		changing = new HashSet<Dataset>();
		writeTimes = new HashMap<Dataset, Long>();
	}
	
	public static RequestQueues getInstance() {
		if (instance == null)
			instance = new RequestQueues();
		return instance;
	}
	
	public Map<Ids2DataEntity, RequestedState> getDeferredOpsQueue() {
		return deferredOpsQueue;
	}
	public Set<Dataset> getChanging() {
		return changing;
	}
	public Map<Dataset, Long> getWriteTimes() {
		return writeTimes;
	}
		
}
