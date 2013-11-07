package org.icatproject.ids.webservice;
//
//import javax.ejb.EJB;
//import javax.ejb.Schedule;
//import javax.ejb.Schedules;
//import javax.ejb.Singleton;
//
//import org.icatproject.ids.util.RequestHelper;
//
//@Singleton
//public class RequestClearanceService {
//
//	@EJB
//	RequestHelper requestHelper;
//
//	// TODO read the interval from ids.properties
//	@Schedules({ @Schedule(hour = "*/1") })
//	public void removeExpiredRequests() {
//		// TODO implement
//	}
//
//	// TODO add a property to ids.properties determining what space usage
//	// threshold should trigger invocation of this method. It should not be
//	// scheduled, but run automatically when we start to run out of disk space
//	@Schedules({ @Schedule(dayOfWeek = "Sun") })
//	public void removeOldestLocalCacheFiles() {
//		// TODO implement
//	}
//}
