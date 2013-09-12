package org.icatproject.ids.webservice;

/*
 *  Theses are the values that the getStatus methods can return. 
 *  They are only used when returning status to the client, nowhere else.
 */
public enum Status {
        ONLINE, INCOMPLETE, RESTORING, ARCHIVED
}
