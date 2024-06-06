package org.icatproject.ids.models;

import org.icatproject.ids.plugin.DfInfo;

/**
 * Contains information about a Datafile. Replaces DsInfo in v3
 * May should implement DfInfo interface
 */
public class DatafileInfo
    extends DataInfoBase
    implements Comparable<DatafileInfo>, DfInfo {

    protected String createId;
    protected String modId;
    protected long datasId;

    public DatafileInfo(
        Long id,
        String name,
        String location,
        String createId,
        String modId,
        Long datasId
    ) {
        super(id, name, location);
        this.createId = createId;
        this.modId = modId;
        this.datasId = datasId;
    }

    public String getCreateId() {
        return this.createId;
    }

    public String getModId() {
        return this.modId;
    }

    public long getDsId() {
        return this.datasId;
    }

    @Override
    public String toString() {
        return this.location;
    }

    @Override
    public int compareTo(DatafileInfo o) {
        if (this.id > o.getId()) {
            return 1;
        }
        if (this.id < o.getId()) {
            return -1;
        }
        return 0;
    }

    // implementing DfInfo
    @Override
    public Long getDfId() {
        return this.getId();
    }

    @Override
    public String getDfLocation() {
        return this.getLocation();
    }

    @Override
    public String getDfName() {
        return this.getName();
    }
}
