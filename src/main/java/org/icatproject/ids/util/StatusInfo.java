package org.icatproject.ids.util;

/*
 * Internal status (used within the server and IDS DB; not returned to the client). 
 * This status is later converted by getStatus to client-readable Status
 * (which contains statuses mentioned in the specification)
 */
public enum StatusInfo {
	SUBMITTED, RETRIEVING, ERROR, COMPLETED, DENIED, INFO_RETRIEVED, INFO_RETRIEVING, NOT_FOUND, INCOMPLETE
}