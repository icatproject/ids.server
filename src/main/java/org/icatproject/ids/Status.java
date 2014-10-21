package org.icatproject.ids;

/*
 *  These are the values that the getStatus methods can return. 
 *  They are only used when returning status to the client, nowhere else.
 */
public enum Status {
	ONLINE, RESTORING, ARCHIVED
}
