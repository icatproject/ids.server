package org.icatproject.ids.enums;

public enum PreparedDataStatus {
    /**
     * The request doesn't processes prepared data. It works with a RequestIdNames.sessionId parameter
     */
    UNPREPARED,

    /**
     * The request processes prepared data. It works with a RequestIdNames.preparedId parameter
     */
    PREPARED,

    /**
     * The request doesn't have a RequestIdNames.sessionId or a RequestIdNames.preparedId parameter
     */
    NOMATTER
}