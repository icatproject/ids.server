package org.icatproject.ids.webservice;

/*
 * Represents the type of action that was requested by a user.
 * This action is only used at the initial stage, later on the FSM
 * decides what real action should be performed on data entities.
 */
public enum DeferredOp {
	ARCHIVE, RESTORE, PREPARE, WRITE
}
