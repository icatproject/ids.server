package org.icatproject.ids.util;

/*
 * A state of a data entity as represented in the FSM
 */
public enum RequestedState {
	ARCHIVE_REQUESTED, RESTORE_REQUESTED, PREPARE_REQUESTED, WRITE_REQUESTED, WRITE_THEN_ARCHIVE_REQUESTED
}
