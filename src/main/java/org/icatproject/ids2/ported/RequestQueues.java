package org.icatproject.ids2.ported;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.icatproject.ids2.ported.entity.Ids2DataEntity;
import org.icatproject.ids2.ported.entity.Ids2DatasetEntity;

public class RequestQueues {
	private final Map<Ids2DataEntity, RequestedState> deferredOpsQueue;
	private final Set<Ids2DataEntity> changing;
	private final Map<Ids2DataEntity, Long> writeTimes;
	
	private static RequestQueues instance = null;
	
	private RequestQueues() {
		deferredOpsQueue = new HashMap<Ids2DataEntity, RequestedState>();
		changing = new HashSet<Ids2DataEntity>();
		writeTimes = new HashMap<Ids2DataEntity, Long>();
	}
	
	public static RequestQueues getInstance() {
		if (instance == null)
			instance = new RequestQueues();
		return instance;
	}
	
	public Map<Ids2DataEntity, RequestedState> getDeferredOpsQueue() {
		return deferredOpsQueue;
	}
	public Set<Ids2DataEntity> getChanging() {
		return changing;
	}
	public Map<Ids2DataEntity, Long> getWriteTimes() {
		return writeTimes;
	}
		
}
