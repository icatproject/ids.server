package org.icatproject.ids2.ported;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.ejb.Singleton;

public class RequestQueues {
	private final Map<Ids2DatasetEntity, RequestedState> deferredOpsQueue;
	private final Set<Ids2DatasetEntity> changing;
	private final Map<Ids2DatasetEntity, Long> writeTimes;
	
	private static RequestQueues instance = null;
	
	private RequestQueues() {
		deferredOpsQueue = new HashMap<Ids2DatasetEntity, RequestedState>();
		changing = new HashSet<Ids2DatasetEntity>();
		writeTimes = new HashMap<Ids2DatasetEntity, Long>();
	}
	
	public static RequestQueues getInstance() {
		if (instance == null)
			instance = new RequestQueues();
		return instance;
	}
	
	public Map<Ids2DatasetEntity, RequestedState> getDeferredOpsQueue() {
		return deferredOpsQueue;
	}
	public Set<Ids2DatasetEntity> getChanging() {
		return changing;
	}
	public Map<Ids2DatasetEntity, Long> getWriteTimes() {
		return writeTimes;
	}
		
}
