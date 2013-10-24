package org.icatproject.ids.util;

import org.icatproject.ids.webservice.Status;

public enum StatusInfo {
	SUBMITTED(Status.RESTORING), RETRIEVING(Status.RESTORING), ERROR, COMPLETED(Status.ONLINE), DENIED, INFO_RETRIEVED(
			Status.RESTORING), INFO_RETRIEVING(Status.RESTORING), NOT_FOUND, INCOMPLETE(
			Status.INCOMPLETE);

	private Status userStatus;

	StatusInfo(Status userStatus) {
		this.userStatus = userStatus;
	}

	StatusInfo() {
	}

	public Status getUserStatus() {
		return userStatus;
	}
}