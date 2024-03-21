package org.icatproject.ids.enums;

public enum OperationIdTypes {
    /**
     * The request doesn't processes prepared data. It works with a RequestIdNames.sessionId parameter
     */
    SESSIONID,

    /**
     * The request processes prepared data. It works with a RequestIdNames.preparedId parameter
     */
    PREPAREDID,

    /**
     * The request doesn't have a RequestIdNames.sessionId or a RequestIdNames.preparedId parameter
     */
    ANONYMOUS
}